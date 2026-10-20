import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field

import pandas as pd
import requests
import websockets
from flask import Flask
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("maquinadedinero")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0").strip() or "0")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089").strip()
DERIV_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
RENDER_URL = os.environ.get("RENDER_URL", "https://rich-machine-1.onrender.com").rstrip("/")
SYMBOL = os.environ.get("DERIV_SYMBOL", "1HZ75V").strip()
DECIMALS = int(os.environ.get("PRICE_DECIMALS", "3"))

STAKE = 0.50
MIN_PROFIT = 0.15
MAX_RISK = 0.05
RR = 3.0
Z_PERIOD = 20
ATR_PERIOD = 14
EMA_TREND = 200
VOL_MULT = 2.5
Z_EXTREME = 2.5
MULTIPLIER = 100


@dataclass
class State:
    mode: str = "manual"
    busy: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


STATE = State()


def pip_round(price):
    return round(float(price), DECIMALS)


def start_flask():
    app = Flask(__name__)

    @app.get("/")
    def root():
        return "OK", 200

    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


async def keepalive_ping():
    while True:
        await asyncio.sleep(600)
        try:
            response = await asyncio.to_thread(requests.get, RENDER_URL, timeout=20)
            log.info("keep-alive %s", response.status_code)
        except Exception as exc:
            log.warning("keep-alive %s", exc)


async def rpc(ws, payload, expect=None, timeout=20):
    await ws.send(json.dumps(payload))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, deadline - time.monotonic()))
        data = json.loads(raw)
        if data.get("error"):
            raise RuntimeError(data["error"].get("message", "deriv error"))
        if expect is None or data.get("msg_type") == expect:
            return data
    raise TimeoutError("deriv timeout")


async def connect():
    if not DERIV_TOKEN:
        raise RuntimeError("falta DERIV_TOKEN")
    ws = await websockets.connect(DERIV_URL, ping_interval=20, ping_timeout=20)
    await rpc(ws, {"authorize": DERIV_TOKEN}, expect="authorize")
    return ws


async def candles(ws, count=250):
    data = await rpc(
        ws,
        {
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": count,
            "end": "latest",
            "granularity": 60,
            "style": "candles",
        },
        expect="candles",
    )
    frame = pd.DataFrame(data["candles"])
    for col in ("open", "high", "low", "close"):
        frame[col] = frame[col].astype(float)
    frame["move"] = (frame["close"] - frame["open"]).abs()
    return frame


def metrics_of(frame):
    close = frame["close"]
    ema20 = close.ewm(span=Z_PERIOD, adjust=False).mean()
    ema200 = close.ewm(span=EMA_TREND, adjust=False).mean()
    stdev = close.rolling(Z_PERIOD).std(ddof=0)
    stdev_now = float(stdev.iloc[-1]) if pd.notna(stdev.iloc[-1]) else 0.0
    if stdev_now == 0.0:
        zscore = 0.0
    else:
        zscore = float((close.iloc[-1] - ema20.iloc[-1]) / stdev_now)
    prev = close.shift(1)
    tr = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev).abs(),
            (frame["low"] - prev).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = float(tr.rolling(ATR_PERIOD).mean().iloc[-1] or 0.0)
    window = frame["move"].tail(21)
    last_move = float(window.iloc[-1])
    base = float(window.iloc[:-1].mean() or 0.0)
    institucional = base > 0 and last_move > base * VOL_MULT
    price = float(close.iloc[-1])
    trend_up = price > float(ema200.iloc[-1])
    return {
        "price": price,
        "zscore": zscore,
        "atr": atr,
        "institucional": institucional,
        "trend_up": trend_up,
    }


def risk_plan(metrics):
    atr = metrics["atr"]
    price = metrics["price"]
    if atr <= 0 or price <= 0:
        return None, "volatilidad invalida"
    stop_loss = round(MAX_RISK, 2)
    take_profit = round(stop_loss * RR, 2)
    if take_profit < MIN_PROFIT or stop_loss > MAX_RISK:
        return None, "filtro monetario"
    if take_profit <= 0.03 or stop_loss >= 0.50:
        return None, "microganancia o perdida prohibida"
    sl_distance = atr * 1.5
    tp_distance = sl_distance * RR
    implied_risk = STAKE * (sl_distance / price)
    implied_profit = STAKE * (tp_distance / price)
    if implied_risk > MAX_RISK:
        return None, "riesgo excedido"
    if implied_profit < MIN_PROFIT:
        return None, "beneficio insuficiente"
    return {
        "stake": STAKE,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "sl_price": pip_round(sl_distance),
        "tp_price": pip_round(tp_distance),
    }, "ok"


def auto_side(metrics):
    if not metrics["institucional"]:
        return None
    if metrics["zscore"] >= Z_EXTREME:
        return "MULTUP"
    if metrics["zscore"] <= -Z_EXTREME:
        return "MULTDOWN"
    return None


def entry_text(trend_up, plan, side):
    trend = "alcista" if trend_up else "bajista"
    action = "compra" if side == "MULTUP" else "venta"
    return (
        f"la tendencia es '{trend}' cuanto quieres ganar {plan['take_profit']:.2f} "
        f"y cuanto es el stop lose {plan['stop_loss']:.2f}; entre en {action}"
    )


async def buy_contract(ws, side, plan):
    if plan["take_profit"] < MIN_PROFIT or plan["stop_loss"] > MAX_RISK:
        return None, "filtro monetario"
    proposal = await rpc(
        ws,
        {
            "proposal": 1,
            "amount": plan["stake"],
            "basis": "stake",
            "contract_type": side,
            "currency": "USD",
            "symbol": SYMBOL,
            "multiplier": MULTIPLIER,
            "limit_order": {
                "take_profit": plan["take_profit"],
                "stop_loss": plan["stop_loss"],
            },
        },
        expect="proposal",
    )
    bought = await rpc(ws, {"buy": proposal["proposal"]["id"], "price": plan["stake"]}, expect="buy")
    return bought["buy"], "ok"


async def session_profit():
    ws = await connect()
    try:
        data = await rpc(ws, {"profit_table": 1, "description": 1, "limit": 50, "sort": "DESC"}, expect="profit_table")
        rows = data.get("profit_table", {}).get("transactions", [])
        total = 0.0
        for row in rows:
            total += float(row.get("sell_price", 0)) - float(row.get("buy_price", 0))
        return round(total, 2), len(rows)
    finally:
        await ws.close()


async def read_metrics():
    ws = await connect()
    try:
        frame = await candles(ws)
        return metrics_of(frame)
    finally:
        await ws.close()


def menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Modo Manual", callback_data="mode_manual")],
            [InlineKeyboardButton("Modo Automático", callback_data="mode_auto")],
        ]
    )


def trade_buttons():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Comprar", callback_data="manual_buy"),
                InlineKeyboardButton("Vender", callback_data="manual_sell"),
            ]
        ]
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        context.application.bot_data.setdefault("chats", set()).add(update.effective_chat.id)
    await update.message.reply_text(
        f"Modo actual: {STATE.mode}",
        reply_markup=menu(),
    )


async def cmd_ganancias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        context.application.bot_data.setdefault("chats", set()).add(update.effective_chat.id)
    try:
        total, count = await session_profit()
        await update.message.reply_text(f"Neto profit table: {total:.2f} USD\nFilas: {count}")
    except Exception as exc:
        await update.message.reply_text(f"No pude leer ganancias: {exc}")


async def manual_card(context, chat_id):
    try:
        metrics = await read_metrics()
    except Exception as exc:
        await context.bot.send_message(chat_id, f"Mercado no disponible: {exc}")
        return
    plan, reason = risk_plan(metrics)
    trend = "alcista" if metrics["trend_up"] else "bajista"
    text = (
        f"Tendencia {trend}\n"
        f"Z-Score {metrics['zscore']:.2f}\n"
        f"ATR {metrics['atr']:.5f}\n"
        f"Volumen institucional: {'si' if metrics['institucional'] else 'no'}\n"
        f"Precio {pip_round(metrics['price'])}"
    )
    if plan is None:
        await context.bot.send_message(chat_id, text + f"\nDescartada: {reason}")
        return
    await context.bot.send_message(chat_id, text, reply_markup=trade_buttons())


async def place(context, chat_id, side):
    if STATE.mode != "manual":
        await context.bot.send_message(chat_id, "Activa Modo Manual.")
        return
    async with STATE.lock:
        if STATE.busy:
            await context.bot.send_message(chat_id, "Orden en curso.")
            return
        STATE.busy = True
        try:
            ws = await connect()
            try:
                metrics = metrics_of(await candles(ws))
                plan, reason = risk_plan(metrics)
                if plan is None:
                    await context.bot.send_message(chat_id, f"Descartada: {reason}")
                    return
                bought, status = await buy_contract(ws, side, plan)
                if bought is None:
                    await context.bot.send_message(chat_id, f"Descartada: {status}")
                    return
                await context.bot.send_message(
                    chat_id,
                    entry_text(metrics["trend_up"], plan, side) + f"\nContrato {bought.get('contract_id')}",
                )
            finally:
                await ws.close()
        except Exception as exc:
            await context.bot.send_message(chat_id, f"Fallo de orden: {exc}")
        finally:
            STATE.busy = False


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    chat_id = query.message.chat_id
    context.application.bot_data.setdefault("chats", set()).add(chat_id)
    if data == "mode_manual":
        STATE.mode = "manual"
        await query.edit_message_text("Modo Manual activo.", reply_markup=menu())
        await manual_card(context, chat_id)
        return
    if data == "mode_auto":
        STATE.mode = "auto"
        await query.edit_message_text("Modo Automático activo.", reply_markup=menu())
        return
    if data == "manual_buy":
        await place(context, chat_id, "MULTUP")
        return
    if data == "manual_sell":
        await place(context, chat_id, "MULTDOWN")


async def notify(app, text):
    targets = set(app.bot_data.get("chats", set()))
    if CHAT_ID:
        targets.add(CHAT_ID)
    for chat_id in targets:
        try:
            await app.bot.send_message(chat_id, text)
        except Exception as exc:
            log.warning("telegram %s", exc)


async def auto_loop(app: Application):
    await asyncio.sleep(5)
    while True:
        try:
            if STATE.mode == "auto" and not STATE.busy:
                async with STATE.lock:
                    STATE.busy = True
                    try:
                        ws = await connect()
                        try:
                            metrics = metrics_of(await candles(ws))
                            side = auto_side(metrics)
                            plan, reason = risk_plan(metrics)
                            if side is None or plan is None:
                                log.info("auto descartada %s", reason if side else "sin extremo")
                            else:
                                bought, status = await buy_contract(ws, side, plan)
                                if bought is None:
                                    log.info("auto descartada %s", status)
                                else:
                                    await notify(
                                        app,
                                        entry_text(metrics["trend_up"], plan, side)
                                        + f"\nContrato {bought.get('contract_id')}",
                                    )
                        finally:
                            await ws.close()
                    finally:
                        STATE.busy = False
        except Exception as exc:
            STATE.busy = False
            log.warning("auto %s", exc)
        await asyncio.sleep(20)


async def post_init(app: Application):
    app.bot_data["chats"] = set()
    if CHAT_ID:
        app.bot_data["chats"].add(CHAT_ID)
    asyncio.create_task(keepalive_ping())
    asyncio.create_task(auto_loop(app))


def main():
    if not TELEGRAM_TOKEN:
        raise SystemExit("falta TELEGRAM_TOKEN")
    threading.Thread(target=start_flask, name="flask", daemon=True).start()
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ganancias", cmd_ganancias))
    app.add_handler(CallbackQueryHandler(on_button))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
