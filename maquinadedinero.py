"maquinadedinero.py: bot híbrido (Modo Manual y Modo Automático) para Deriv, controlado desde Telegram y desplegado como Web Service gratuito en Render."
from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import websockets
from flask import Flask
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("maquinadedinero")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
CABECERAS_WS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Origin": "https://developers.deriv.com",
}

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0").strip() or "0")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089").strip()
DERIV_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

SYMBOL = os.environ.get("DERIV_SYMBOL", "R_100").strip()
MODO_REAL = os.environ.get("MODO_REAL", "false").strip().lower() == "true"
MULTIPLICADORES = sorted({int(x) for x in os.environ.get("MULTIPLICADORES", "10,20,30,40,50,100,200").split(",") if x.strip()})
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://rich-machine-1.onrender.com").strip()
PORT = int(os.environ.get("PORT", "5000"))

STAKE = 0.50
RIESGO_MAX_USD = 0.05
BENEFICIO_MIN_USD = 0.15
RATIO_RR = 3.0
SL_ATR_MULT = 1.5

ZLEN = 20
ATR_LEN = 14
EMA_MACRO = 200
VOL_LEN = 20
VOL_MULT = 2.5
Z_EXTREMO = 2.5

GRANULARIDAD = 60
VELAS = 300
TICKS = 5000
INTERVALO_KEEPALIVE = 600
INTERVALO_AUTO = 20


def validar_configuracion() -> None:
    faltantes = []
    if not TELEGRAM_TOKEN:
        faltantes.append("TELEGRAM_TOKEN")
    if not CHAT_ID:
        faltantes.append("TELEGRAM_CHAT_ID")
    if not DERIV_TOKEN:
        faltantes.append("DERIV_TOKEN")
    if faltantes:
        raise SystemExit("Faltan variables de entorno: " + ", ".join(faltantes))


flask_app = Flask(__name__)


@flask_app.route("/")
def inicio():
    return "Bot activo", 200


def _servir_web() -> None:
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


def iniciar_servidor_web() -> None:
    hilo = threading.Thread(target=_servir_web, name="flask-keepalive", daemon=True)
    hilo.start()


async def keep_alive(url: str) -> None:
    while True:
        await asyncio.sleep(INTERVALO_KEEPALIVE)
        try:
            resp = await asyncio.to_thread(requests.get, url, timeout=20)
            log.info("Keep-alive %s -> %s", url, resp.status_code)
        except Exception as e:
            log.warning("Keep-alive falló: %s", e)


class DerivError(Exception):
    pass


class DerivClient:
    def __init__(self, token: str, url: str) -> None:
        self.token = token
        self.url = url
        self.ws = None
        self.info: dict = {}
        self.listo = asyncio.Event()
        self._pendientes: dict[int, asyncio.Future] = {}
        self._req = 0
        self._tarea: asyncio.Task | None = None

    def iniciar(self) -> None:
        self._tarea = asyncio.create_task(self._mantener())

    async def detener(self) -> None:
        if self._tarea is not None:
            self._tarea.cancel()

    async def request(self, payload: dict, timeout: float = 25) -> dict:
        try:
            await asyncio.wait_for(self.listo.wait(), timeout=30)
        except asyncio.TimeoutError:
            raise ConnectionError("sin conexión con Deriv")
        ws = self.ws
        if ws is None:
            raise ConnectionError("sin conexión con Deriv")
        self._req += 1
        rid = self._req
        fut = asyncio.get_running_loop().create_future()
        self._pendientes[rid] = fut
        try:
            await ws.send(json.dumps({**payload, "req_id": rid}))
            msg = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            raise ConnectionError("Deriv no respondió a tiempo")
        finally:
            self._pendientes.pop(rid, None)
        if "error" in msg:
            raise DerivError(msg["error"].get("message", "error de Deriv"))
        return msg

    async def _mantener(self) -> None:
        espera = 2
        while True:
            try:
                async with websockets.connect(url, additional_headers=CABECERAS_WS, ping_interval=20, ping_timeout=20) as ws:
                    self.ws = ws
                    await ws.send(json.dumps({"authorize": self.token}))
                    resp = json.loads(await ws.recv())
                    if "error" in resp:
                        raise DerivError(resp["error"].get("message", "token inválido"))
                    self.info = resp["authorize"]
                    self.listo.set()
                    espera = 2
                    log.info("Deriv conectado: %s", self.info.get("loginid"))
                    async for raw in ws:
                        msg = json.loads(raw)
                        fut = self._pendientes.get(msg.get("req_id"))
                        if fut is not None and not fut.done():
                            fut.set_result(msg)
            except Exception as e:
                log.warning("Conexión con Deriv perdida: %s", e)
            finally:
                self.ws = None
                self.listo.clear()
                for fut in self._pendientes.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("conexión con Deriv perdida"))
                self._pendientes.clear()
            await asyncio.sleep(espera)
            espera = min(espera * 2, 60)


DERIV: DerivClient | None = None
PIP: float | None = None
DECIMALES = 5


@dataclass
class Lectura:
    epoch: int
    cierre: float
    precio_vivo: float
    z: float
    atr: float
    ema_macro: float
    vol_inst: bool


def proxy_volumen(precios: list, tiempos: list) -> pd.Series:
    p = pd.Series(precios, dtype="float64")
    t = pd.Series(tiempos, dtype="int64")
    movimiento = p.diff().abs().fillna(0.0)
    bucket = (t // GRANULARIDAD) * GRANULARIDAD
    return movimiento.groupby(bucket.to_numpy()).sum()


def calcular_indicadores(velas: pd.DataFrame) -> pd.DataFrame:
    df = velas.copy()
    cierre = df["close"]
    df["ema_z"] = cierre.ewm(span=ZLEN, adjust=False).mean()
    df["desv"] = cierre.rolling(ZLEN).std(ddof=0)
    df["z"] = (cierre - df["ema_z"]) / df["desv"].replace(0, np.nan)
    previo = cierre.shift(1)
    rango_verdadero = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previo).abs(),
        (df["low"] - previo).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = rango_verdadero.ewm(alpha=1 / ATR_LEN, adjust=False).mean()
    df["ema_macro"] = cierre.ewm(span=EMA_MACRO, adjust=False).mean()
    df["vol_media"] = df["vol_proxy"].rolling(VOL_LEN).mean()
    df["vol_inst"] = df["vol_proxy"] > df["vol_media"] * VOL_MULT
    return df


async def obtener_pip() -> tuple[float, int]:
    respuesta = await DERIV.request({"active_symbols": "brief"})
    for simbolo in respuesta.get("active_symbols", []):
        if simbolo.get("symbol") == SYMBOL:
            pip = float(simbolo.get("pip", 0.00001))
            return pip, max(0, int(round(-math.log10(pip))))
    return 0.00001, 5


def redondear(precio: float) -> float:
    return round(round(precio / PIP) * PIP, DECIMALES)


async def leer_mercado() -> Lectura | None:
    global PIP, DECIMALES
    if PIP is None:
        PIP, DECIMALES = await obtener_pip()

    velas_r, ticks_r = await asyncio.gather(
        DERIV.request({"ticks_history": SYMBOL, "style": "candles",
                       "granularity": GRANULARIDAD, "count": VELAS, "end": "latest"}),
        DERIV.request({"ticks_history": SYMBOL, "style": "ticks",
                       "count": TICKS, "end": "latest"}),
    )
    velas = pd.DataFrame(velas_r["candles"])
    if len(velas) < EMA_MACRO + 5:
        return None
    for columna in ("open", "high", "low", "close"):
        velas[columna] = velas[columna].astype(float)

    precios = [float(p) for p in ticks_r["history"]["prices"]]
    tiempos = [int(t) for t in ticks_r["history"]["times"]]
    proxy = proxy_volumen(precios, tiempos)
    velas["vol_proxy"] = velas["epoch"].map(proxy).fillna(0.0).astype(float)

    ventana = velas["epoch"].iloc[-(VOL_LEN + 1):-1]
    if not ventana.isin(proxy.index).all():
        return None

    indicadores = calcular_indicadores(velas)
    cerrada = indicadores.iloc[-2]
    if pd.isna(cerrada["z"]) or pd.isna(cerrada["atr"]):
        return None
    return Lectura(
        epoch=int(cerrada["epoch"]),
        cierre=float(cerrada["close"]),
        precio_vivo=precios[-1],
        z=float(cerrada["z"]),
        atr=float(cerrada["atr"]),
        ema_macro=float(cerrada["ema_macro"]),
        vol_inst=bool(cerrada["vol_inst"]),
    )


@dataclass
class Plan:
    direccion: int
    multiplicador: int
    entrada: float
    sl: float
    tp: float
    riesgo_usd: float
    beneficio_usd: float


def planificar(direccion: int, entrada: float, atr: float) -> tuple[Plan | None, str]:
    if not atr or atr <= 0:
        return None, "ATR no válido"
    dist_min = SL_ATR_MULT * atr
    cota = RIESGO_MAX_USD * entrada / (STAKE * dist_min)
    candidatos = [m for m in MULTIPLICADORES if m <= cota]
    if not candidatos:
        return None, (f"con ${RIESGO_MAX_USD:.2f} de riesgo el SL quedaría dentro "
                      f"del ruido (menos de {SL_ATR_MULT} ATR)")
    multiplicador = max(candidatos)
    dist_sl = RIESGO_MAX_USD * entrada / (STAKE * multiplicador)
    dist_tp = RATIO_RR * dist_sl
    riesgo = dist_sl * STAKE * multiplicador / entrada
    beneficio = dist_tp * STAKE * multiplicador / entrada
    if riesgo > RIESGO_MAX_USD + 1e-9 or beneficio < BENEFICIO_MIN_USD - 1e-9:
        return None, "no cumple el filtro de riesgo y beneficio"
    plan = Plan(
        direccion=direccion,
        multiplicador=multiplicador,
        entrada=entrada,
        sl=redondear(entrada - direccion * dist_sl),
        tp=redondear(entrada + direccion * dist_tp),
        riesgo_usd=riesgo,
        beneficio_usd=beneficio,
    )
    return plan, ""


def fmt(precio: float) -> str:
    return f"{precio:.7f}".rstrip("0").rstrip(".")


def texto_alerta(plan: Plan) -> str:
    tendencia = "alcista" if plan.direccion > 0 else "bajista"
    operacion = "compra" if plan.direccion > 0 else "venta"
    return (f"la tendencia es '{tendencia}' cuanto quieres ganar {fmt(plan.tp)} "
            f"y cuanto es el stop lose {fmt(plan.sl)}; entré en {operacion}")


class Estado:
    def __init__(self) -> None:
        self.auto_activo = False
        self.ultima_vela: int | None = None
        self.lock: asyncio.Lock | None = None
        self.tareas: list[asyncio.Task] = []
        self.vigilancias: set[asyncio.Task] = set()


ESTADO = Estado()


def verificar_modo() -> None:
    if DERIV is None or not DERIV.info:
        raise RuntimeError("sin conexión con Deriv")
    es_demo = bool(DERIV.info.get("is_virtual"))
    if (not es_demo) != MODO_REAL:
        raise RuntimeError("el token no coincide con MODO_REAL; con MODO_REAL=false solo se opera en demo")


async def abrir_operacion(plan: Plan, bot) -> int:
    async with ESTADO.lock:
        verificar_modo()
        abiertas = await DERIV.request({"portfolio": 1})
        if abiertas["portfolio"].get("contracts"):
            raise RuntimeError("ya hay una operación abierta; espero a que cierre")
        tipo = "MULTUP" if plan.direccion > 0 else "MULTDOWN"
        propuesta = (await DERIV.request({
            "proposal": 1,
            "amount": STAKE,
            "basis": "stake",
            "contract_type": tipo,
            "currency": "USD",
            "symbol": SYMBOL,
            "multiplier": plan.multiplicador,
            "limit_order": {
                "stop_loss": round(RIESGO_MAX_USD, 2),
                "take_profit": round(BENEFICIO_MIN_USD, 2),
            },
        }))["proposal"]
        compra = (await DERIV.request({"buy": propuesta["id"],
                                       "price": propuesta["ask_price"]}))["buy"]
        contrato = int(compra["contract_id"])
    await bot.send_message(chat_id=CHAT_ID, text=texto_alerta(plan))
    tarea = asyncio.create_task(vigilar_contrato(contrato, bot))
    ESTADO.vigilancias.add(tarea)
    tarea.add_done_callback(ESTADO.vigilancias.discard)
    return contrato


async def vigilar_contrato(contrato: int, bot) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            respuesta = await DERIV.request({"proposal_open_contract": 1,
                                             "contract_id": contrato})
        except Exception as e:
            log.warning("No pude consultar el contrato %s: %s", contrato, e)
            continue
        estado_contrato = respuesta["proposal_open_contract"]
        if estado_contrato.get("is_sold"):
            resultado = float(estado_contrato.get("profit", 0.0))
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"Contrato {contrato} cerrado. Resultado: ${resultado:+.2f}",
                )
            except Exception as e:
                log.warning("No pude notificar el cierre: %s", e)
            return


async def resumen_resultados(desde: int | None) -> tuple[int, float]:
    payload = {"profit_table": 1, "description": 1, "sort": "DESC", "limit": 100}
    if desde is not None:
        payload["date_from"] = desde
    respuesta = await DERIV.request(payload)
    transacciones = respuesta["profit_table"].get("transactions", [])
    ganancia = sum(float(t.get("sell_price", 0)) - float(t.get("buy_price", 0))
                   for t in transacciones)
    return len(transacciones), ganancia


async def texto_ganancias() -> str:
    inicio_dia = int(datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp())
    n_hoy, g_hoy = await resumen_resultados(inicio_dia)
    n_total, g_total = await resumen_resultados(None)
    saldo = (await DERIV.request({"balance": 1}))["balance"]
    return (
        "Ganancias (cuenta Deriv)\n"
        f"Hoy: ${g_hoy:+.2f} en {n_hoy} operaciones\n"
        f"Últimas {n_total} operaciones: ${g_total:+.2f}\n"
        f"Saldo actual: ${float(saldo['balance']):.2f} {saldo.get('currency', 'USD')}"
    )


async def ciclo_automatico(bot) -> None:
    lectura = await leer_mercado()
    if lectura is None or lectura.epoch == ESTADO.ultima_vela:
        return
    ESTADO.ultima_vela = lectura.epoch
    if not lectura.vol_inst:
        return
    if lectura.z >= Z_EXTREMO and lectura.cierre > lectura.ema_macro:
        direccion = 1
    elif lectura.z <= -Z_EXTREMO and lectura.cierre < lectura.ema_macro:
        direccion = -1
    else:
        return
    plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr)
    if plan is None:
        log.info("Señal automática descartada: %s", motivo)
        return
    try:
        await abrir_operacion(plan, bot)
    except Exception as e:
        log.warning("Orden automática no enviada: %s", e)


async def bucle_automatico(bot) -> None:
    while True:
        if ESTADO.auto_activo:
            try:
                await ciclo_automatico(bot)
            except Exception:
                log.exception("Error en el ciclo automático")
        await asyncio.sleep(INTERVALO_AUTO)


def solo_dueno(func):
    @functools.wraps(func)
    async def envoltorio(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        if chat is None or chat.id != CHAT_ID:
            return None
        return await func(update, context)
    return envoltorio


def texto_menu() -> str:
    cuenta = "sin conexión"
    if DERIV is not None and DERIV.info:
        tipo = "DEMO" if DERIV.info.get("is_virtual") else "REAL"
        cuenta = f"{DERIV.info.get('loginid')} ({tipo})"
    automatico = "ACTIVO" if ESTADO.auto_activo else "apagado"
    return (
        f"Bot {SYMBOL}\n"
        f"Cuenta: {cuenta}\n"
        f"Modo automático: {automatico}\n"
        f"Stake ${STAKE:.2f} · riesgo ${RIESGO_MAX_USD:.2f} · beneficio ${BENEFICIO_MIN_USD:.2f}"
    )


def teclado_menu() -> InlineKeyboardMarkup:
    if ESTADO.auto_activo:
        boton_auto = InlineKeyboardButton("Detener Modo Automático", callback_data="auto:off")
    else:
        boton_auto = InlineKeyboardButton("Modo Automático", callback_data="auto:on")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Modo Manual", callback_data="manual")],
        [boton_auto],
        [InlineKeyboardButton("Ganancias", callback_data="ganancias")],
    ])


def boton_volver() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Menú", callback_data="menu")]])


async def editar(q, texto: str, markup=None) -> None:
    try:
        await q.edit_message_text(texto, reply_markup=markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


def texto_lectura(lectura: Lectura, planes: dict[int, Plan], motivos: dict[int, str]) -> str:
    hora = datetime.fromtimestamp(lectura.epoch, timezone.utc).strftime("%H:%M UTC")
    tendencia = "alcista" if lectura.cierre > lectura.ema_macro else "bajista"
    lineas = [
        f"{SYMBOL} · vela cerrada {hora}",
        f"Tendencia (cierre vs EMA 200): {tendencia}",
        f"Z-Score (20): {lectura.z:+.2f}",
        f"ATR (14): {lectura.atr:.5f}",
        f"Volumen institucional (proxy): {'sí' if lectura.vol_inst else 'no'}",
        f"Precio actual: {fmt(redondear(lectura.precio_vivo))}",
    ]
    for direccion, nombre in ((1, "COMPRA"), (-1, "VENTA")):
        if direccion in planes:
            p = planes[direccion]
            lineas.append(
                f"{nombre}: SL {fmt(p.sl)} · TP {fmt(p.tp)} · multiplicador x{p.multiplicador} · "
                f"riesgo ${p.riesgo_usd:.2f} · beneficio ${p.beneficio_usd:.2f}"
            )
        else:
            lineas.append(f"{nombre}: descartada ({motivos[direccion]})")
    return "\n".join(lineas)


async def mostrar_menu_manual(q) -> None:
    await editar(q, "Analizando el mercado...")
    lectura = await leer_mercado()
    if lectura is None:
        await editar(q, "Datos insuficientes por ahora (faltan velas o ticks).", boton_volver())
        return
    planes: dict[int, Plan] = {}
    motivos: dict[int, str] = {}
    for direccion in (1, -1):
        plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr)
        if plan is None:
            motivos[direccion] = motivo
        else:
            planes[direccion] = plan
    botones = []
    if 1 in planes:
        botones.append(InlineKeyboardButton(f"Comprar ${STAKE:.2f}", callback_data="ejecutar:1"))
    if -1 in planes:
        botones.append(InlineKeyboardButton(f"Vender ${STAKE:.2f}", callback_data="ejecutar:-1"))
    filas = [botones] if botones else []
    filas.append([InlineKeyboardButton("Menú", callback_data="menu")])
    await editar(q, texto_lectura(lectura, planes, motivos), InlineKeyboardMarkup(filas))


async def ejecutar_manual(q, direccion: int, bot) -> None:
    await editar(q, "Recalculando la señal y enviando la orden...")
    lectura = await leer_mercado()
    if lectura is None:
        raise RuntimeError("datos insuficientes para recalcular")
    plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr)
    if plan is None:
        await editar(q, f"Señal descartada al confirmar: {motivo}", boton_volver())
        return
    contrato = await abrir_operacion(plan, bot)
    await editar(q, f"Orden enviada (contrato {contrato}).", boton_volver())


@solo_dueno
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(texto_menu(), reply_markup=teclado_menu())


@solo_dueno
async def cmd_ganancias(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        texto = await texto_ganancias()
    except Exception as e:
        log.exception("Error en /ganancias")
        texto = f"No pude consultar las ganancias: {e}"
    await update.message.reply_text(texto)


@solo_dueno
async def on_boton(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    consulta = update.callback_query
    await consulta.answer()
    dato = consulta.data
    try:
        if dato == "menu":
            await editar(consulta, texto_menu(), teclado_menu())
        elif dato == "manual":
            await mostrar_menu_manual(consulta)
        elif dato.startswith("ejecutar:"):
            await ejecutar_manual(consulta, int(dato.split(":", 1)[1]), context.bot)
        elif dato == "auto:on":
            ESTADO.auto_activo = True
            await editar(consulta, texto_menu(), teclado_menu())
        elif dato == "auto:off":
            ESTADO.auto_activo = False
            await editar(consulta, texto_menu(), teclado_menu())
        elif dato == "ganancias":
            await editar(consulta, await texto_ganancias(), boton_volver())
    except Exception as e:
        log.exception("Error en el botón %s", dato)
        await editar(consulta, f"Error: {e}", boton_volver())


async def on_init(app: Application) -> None:
    global DERIV
    ESTADO.lock = asyncio.Lock()
    DERIV = DerivClient(DERIV_TOKEN, DERIV_URL)
    DERIV.iniciar()
    ESTADO.tareas.append(asyncio.create_task(keep_alive(RENDER_URL)))
    ESTADO.tareas.append(asyncio.create_task(bucle_automatico(app.bot)))
    log.info("Servicio iniciado. Keep-alive cada %d s hacia %s", INTERVALO_KEEPALIVE, RENDER_URL)


async def on_shutdown(app: Application) -> None:
    for tarea in ESTADO.tareas:
        tarea.cancel()
    if DERIV is not None:
        await DERIV.detener()


def main() -> None:
    validar_configuracion()
    iniciar_servidor_web()
    aplicacion = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(on_init)
        .post_shutdown(on_shutdown)
        .build()
    )
    aplicacion.add_handler(CommandHandler("start", cmd_start))
    aplicacion.add_handler(CommandHandler("ganancias", cmd_ganancias))
    aplicacion.add_handler(CallbackQueryHandler(on_boton))
    aplicacion.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
