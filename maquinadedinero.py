"""maquinadedinero.py: bot híbrido para Deriv (API nueva, con OTP) con Modo Manual, Modo Automático, backtest estadístico y control de riesgo, controlado desde Telegram y desplegado como Web Service gratuito en Render."""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import threading
import time
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

import fundamentos

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("maquinadedinero")

API_BASE = "https://api.derivws.com"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0").strip() or "0")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "").strip()
MODO_REAL = os.environ.get("MODO_REAL", "false").strip().lower() == "true"

SYMBOL = os.environ.get("DERIV_SYMBOL", "frxXAUUSD").strip()
MULTIPLICADORES = sorted({int(x) for x in os.environ.get("MULTIPLICADORES", "100,200,300,500,800").split(",") if x.strip()})
STAKE = float(os.environ.get("STAKE", "1.00"))
RIESGO_USD = float(os.environ.get("RIESGO_USD", "0.10"))
BENEFICIO_USD = float(os.environ.get("BENEFICIO_USD", "0.40"))
COMISION_USD = float(os.environ.get("COMISION_USD", "0.02"))
CAPITAL_CUENTA = float(os.environ.get("CAPITAL_CUENTA", "5.00"))
MODO_SENAL = os.environ.get("MODO_SENAL", "momentum").strip().lower()
MIN_SENALES = int(os.environ.get("MIN_SENALES", "100"))
GATE_HORAS = float(os.environ.get("GATE_HORAS", "1"))
MAX_PERDIDAS_SEGUIDAS = int(os.environ.get("MAX_PERDIDAS_SEGUIDAS", "3"))
LIMITE_PERDIDA_DIA = float(os.environ.get("LIMITE_PERDIDA_DIA", "0.30"))
COOLDOWN_AUTO_MIN = float(os.environ.get("COOLDOWN_AUTO_MIN", "15"))
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://rich-machine-1.onrender.com").strip()
PORT = int(os.environ.get("PORT", "5000"))

SL_ATR_MULT = 1.5
ZLEN = 20
ATR_LEN = 14
EMA_MACRO = 200
VOL_LEN = 20
VOL_MULT = 2.5
Z_EXTREMO = 2.5
GRANULARIDAD = 60
VELAS = 300
VELAS_BACKTEST = 5000
HORIZONTE_BACKTEST = 60
INTERVALO_KEEPALIVE = 600
INTERVALO_AUTO = 20
CABECERAS_WS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Origin": "https://developers.deriv.com",
}


class DerivError(Exception):
    pass


def validar_configuracion() -> None:
    requeridas = {
        "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID": CHAT_ID,
        "DERIV_TOKEN": DERIV_TOKEN,
        "DERIV_APP_ID": DERIV_APP_ID,
    }
    faltantes = [nombre for nombre, valor in requeridas.items() if not valor]
    if faltantes:
        raise SystemExit("Faltan variables de entorno: " + ", ".join(faltantes))
    if RIESGO_USD <= COMISION_USD:
        raise SystemExit("RIESGO_USD debe ser mayor que COMISION_USD")
    if MODO_SENAL not in ("momentum", "reversion"):
        raise SystemExit("MODO_SENAL debe ser 'momentum' o 'reversion'")


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


def _rest(metodo: str, ruta: str, cuerpo: dict | None = None) -> dict:
    cabeceras = {"Authorization": f"Bearer {DERIV_TOKEN}", "Deriv-App-ID": DERIV_APP_ID}
    resp = requests.request(metodo, f"{API_BASE}{ruta}", headers=cabeceras, json=cuerpo, timeout=20)
    try:
        datos = resp.json()
    except ValueError:
        datos = {}
    if resp.status_code >= 400:
        errores = datos.get("errors") or [{}]
        mensaje = errores[0].get("message", f"HTTP {resp.status_code}")
        raise DerivError(f"{metodo} {ruta}: {mensaje}")
    return datos


def elegir_cuenta(cuentas: list) -> dict | None:
    tipo = "real" if MODO_REAL else "demo"
    for cuenta in cuentas:
        if cuenta.get("account_type") == tipo and cuenta.get("status", "active") == "active":
            return cuenta
    return None


async def obtener_url_ws() -> tuple[str, dict]:
    datos = await asyncio.to_thread(_rest, "GET", "/trading/v1/options/accounts")
    cuenta = elegir_cuenta(datos.get("data", []))
    if cuenta is None:
        if MODO_REAL:
            raise DerivError("no hay cuenta real de Options; créala en Deriv antes de activar MODO_REAL")
        creada = await asyncio.to_thread(
            _rest, "POST", "/trading/v1/options/accounts",
            {"currency": "USD", "group": "row", "account_type": "demo"},
        )
        datos_cuenta = creada["data"]
        cuenta = datos_cuenta[0] if isinstance(datos_cuenta, list) else datos_cuenta
    otp = await asyncio.to_thread(
        _rest, "POST", f"/trading/v1/options/accounts/{cuenta['account_id']}/otp"
    )
    return otp["data"]["url"], cuenta


class DerivClient:
    def __init__(self) -> None:
        self.ws = None
        self.cuenta: dict = {}
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
                url, cuenta = await obtener_url_ws()
                self.cuenta = cuenta
                async with websockets.connect(
                    url,
                    additional_headers=CABECERAS_WS,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    self.ws = ws
                    self.listo.set()
                    espera = 2
                    log.info("Deriv conectado: cuenta %s (%s)", cuenta.get("account_id"), cuenta.get("account_type"))
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
PIP: float = 0.00001
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
    abierto: bool


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


async def info_simbolo() -> dict:
    respuesta = await DERIV.request({"active_symbols": "brief"})
    for simbolo in respuesta.get("active_symbols", []):
        if simbolo.get("underlying_symbol") == SYMBOL:
            return simbolo
    raise DerivError(f"el símbolo {SYMBOL} no está disponible en esta cuenta")


def fijar_pip(info: dict) -> None:
    global PIP, DECIMALES
    PIP = float(info.get("pip_size", 0.00001))
    DECIMALES = max(0, int(round(-math.log10(PIP))))


def redondear(precio: float) -> float:
    return round(round(precio / PIP) * PIP, DECIMALES)


async def leer_mercado() -> Lectura | None:
    info = await info_simbolo()
    fijar_pip(info)
    abierto = int(info.get("exchange_is_open", 0)) == 1 and int(info.get("is_trading_suspended", 0)) == 0

    respuesta = await DERIV.request({"ticks_history": SYMBOL, "style": "candles",
                                     "granularity": GRANULARIDAD, "count": VELAS, "end": "latest"})
    velas = pd.DataFrame(respuesta.get("candles", []))
    if len(velas) < EMA_MACRO + 5:
        raise DerivError(f"Deriv devolvió {len(velas)} velas y necesito al menos {EMA_MACRO + 5}")
    for columna in ("open", "high", "low", "close"):
        velas[columna] = velas[columna].astype(float)
    velas["vol_proxy"] = (velas["high"] - velas["low"]).astype(float)

    indicadores = calcular_indicadores(velas)
    cerrada = indicadores.iloc[-2]
    if pd.isna(cerrada["z"]) or pd.isna(cerrada["atr"]):
        raise DerivError("los indicadores aún no tienen suficientes datos")
    return Lectura(
        epoch=int(cerrada["epoch"]),
        cierre=float(cerrada["close"]),
        precio_vivo=float(velas["close"].iloc[-1]),
        z=float(cerrada["z"]),
        atr=float(cerrada["atr"]),
        ema_macro=float(cerrada["ema_macro"]),
        vol_inst=bool(cerrada["vol_inst"]),
        abierto=abierto,
    )


def senal_de(z: float, cierre: float, ema: float, vol_inst: bool, modo: str) -> int | None:
    if not vol_inst:
        return None
    if z >= Z_EXTREMO:
        extremo = 1
    elif z <= -Z_EXTREMO:
        extremo = -1
    else:
        return None
    if modo == "reversion":
        return -extremo
    if extremo == 1 and cierre > ema:
        return 1
    if extremo == -1 and cierre < ema:
        return -1
    return None


@dataclass
class Plan:
    direccion: int
    multiplicador: int
    entrada: float
    sl: float
    tp: float
    riesgo_usd: float
    beneficio_usd: float
    stake: float


def planificar(direccion: int, entrada: float, atr: float, stake: float = STAKE) -> tuple[Plan | None, str]:
    if not atr or atr <= 0:
        return None, "ATR no válido"
    perdida_bruta = RIESGO_USD - COMISION_USD
    ganancia_bruta = BENEFICIO_USD + COMISION_USD
    cota = perdida_bruta * entrada / (stake * SL_ATR_MULT * atr)
    candidatos = [m for m in MULTIPLICADORES if m <= cota]
    if not candidatos:
        return None, f"el stop de ${RIESGO_USD:.2f} quedaría dentro del ruido (menos de {SL_ATR_MULT} ATR)"
    multiplicador = max(candidatos)
    dist_sl = perdida_bruta * entrada / (stake * multiplicador)
    dist_tp = ganancia_bruta * entrada / (stake * multiplicador)
    riesgo = dist_sl * stake * multiplicador / entrada + COMISION_USD
    beneficio = dist_tp * stake * multiplicador / entrada - COMISION_USD
    if riesgo > RIESGO_USD + 1e-9 or beneficio < BENEFICIO_USD - 1e-9:
        return None, "no cumple el filtro de riesgo y beneficio"
    plan = Plan(
        direccion=direccion,
        multiplicador=multiplicador,
        entrada=entrada,
        sl=redondear(entrada - direccion * dist_sl),
        tp=redondear(entrada + direccion * dist_tp),
        riesgo_usd=riesgo,
        beneficio_usd=beneficio,
        stake=stake,
    )
    return plan, ""


def _resolver(df: pd.DataFrame, desde: int, plan: Plan) -> tuple[str, int]:
    hasta = min(desde + HORIZONTE_BACKTEST, len(df))
    for j in range(desde, hasta):
        alto = float(df["high"].iloc[j])
        bajo = float(df["low"].iloc[j])
        if plan.direccion > 0:
            if bajo <= plan.sl:
                return "perdida", j
            if alto >= plan.tp:
                return "ganada", j
        else:
            if alto >= plan.sl:
                return "perdida", j
            if bajo <= plan.tp:
                return "ganada", j
    return "expirada", hasta - 1


def simular_variante(df: pd.DataFrame, modo: str) -> list[str]:
    resultados: list[str] = []
    i = EMA_MACRO + ZLEN
    fin = len(df) - HORIZONTE_BACKTEST - 1
    while i < fin:
        fila = df.iloc[i]
        if pd.isna(fila["z"]) or pd.isna(fila["atr"]):
            i += 1
            continue
        direccion = senal_de(float(fila["z"]), float(fila["close"]),
                             float(fila["ema_macro"]), bool(fila["vol_inst"]), modo)
        if direccion is None:
            i += 1
            continue
        plan, _ = planificar(direccion, float(fila["close"]), float(fila["atr"]))
        if plan is None:
            i += 1
            continue
        resultado, salida = _resolver(df, i + 1, plan)
        resultados.append(resultado)
        i = salida + 1
    return resultados


class Estado:
    def __init__(self) -> None:
        self.auto_activo = False
        self.ultima_vela: int | None = None
        self.ultima_auto: float = 0.0
        self.dia: str | None = None
        self.pnl_dia: float = 0.0
        self.perdidas_seguidas: int = 0
        self.gate: dict | None = None
        self.gate_hora: float = 0.0
        self.lock: asyncio.Lock | None = None
        self.tareas: list[asyncio.Task] = []
        self.vigilancias: set[asyncio.Task] = set()


ESTADO = Estado()


def fmt(precio: float) -> str:
    return f"{precio:.7f}".rstrip("0").rstrip(".")


def texto_alerta(direccion: int, tp: float, sl: float) -> str:
    tendencia = "alcista" if direccion > 0 else "bajista"
    operacion = "compra" if direccion > 0 else "venta"
    return (f"la tendencia es '{tendencia}' cuanto quieres ganar {fmt(tp)} "
            f"y cuanto es el stop lose {fmt(sl)}; entré en {operacion}")


def verificar_modo() -> None:
    if DERIV is None or not DERIV.cuenta:
        raise RuntimeError("sin conexión con Deriv")
    es_real = DERIV.cuenta.get("account_type") == "real"
    if es_real != MODO_REAL:
        raise RuntimeError("la cuenta conectada no coincide con MODO_REAL")


def registrar_resultado(resultado: float, automatica: bool) -> str | None:
    hoy = datetime.now(timezone.utc).date().isoformat()
    if ESTADO.dia != hoy:
        ESTADO.dia = hoy
        ESTADO.pnl_dia = 0.0
    ESTADO.pnl_dia += resultado
    ESTADO.perdidas_seguidas = ESTADO.perdidas_seguidas + 1 if resultado < 0 else 0
    if automatica and ESTADO.auto_activo:
        if ESTADO.perdidas_seguidas >= MAX_PERDIDAS_SEGUIDAS:
            ESTADO.auto_activo = False
            return f"Modo automático DETENIDO: {ESTADO.perdidas_seguidas} pérdidas seguidas."
        if ESTADO.pnl_dia <= -LIMITE_PERDIDA_DIA:
            ESTADO.auto_activo = False
            return f"Modo automático DETENIDO: pérdida del día ${ESTADO.pnl_dia:.2f}."
    return None


async def abrir_operacion(plan: Plan, bot, automatica: bool = False) -> int:
    async with ESTADO.lock:
        verificar_modo()
        abiertas = await DERIV.request({"portfolio": 1})
        if abiertas["portfolio"].get("contracts"):
            raise RuntimeError("ya hay una operación abierta; espero a que cierre")
        tipo = "MULTUP" if plan.direccion > 0 else "MULTDOWN"
        propuesta = (await DERIV.request({
            "proposal": 1,
            "amount": plan.stake,
            "basis": "stake",
            "contract_type": tipo,
            "currency": "USD",
            "underlying_symbol": SYMBOL,
            "multiplier": plan.multiplicador,
            "limit_order": {
                "stop_loss": RIESGO_USD,
                "take_profit": BENEFICIO_USD,
            },
        }))["proposal"]
        niveles = propuesta.get("limit_order", {})
        tp = float(niveles["take_profit"]["value"]) if "take_profit" in niveles else plan.tp
        sl = float(niveles["stop_loss"]["value"]) if "stop_loss" in niveles else plan.sl
        compra = (await DERIV.request({
            "buy": propuesta["id"],
            "price": float(propuesta["ask_price"]),
        }))["buy"]
        contrato = int(compra["contract_id"])
    await bot.send_message(chat_id=CHAT_ID, text=texto_alerta(plan.direccion, tp, sl))
    tarea = asyncio.create_task(vigilar_contrato(contrato, bot, automatica))
    ESTADO.vigilancias.add(tarea)
    tarea.add_done_callback(ESTADO.vigilancias.discard)
    return contrato


async def vigilar_contrato(contrato: int, bot, automatica: bool) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            respuesta = await DERIV.request({"proposal_open_contract": 1, "contract_id": contrato})
        except Exception as e:
            log.warning("No pude consultar el contrato %s: %s", contrato, e)
            continue
        estado_contrato = respuesta["proposal_open_contract"]
        if estado_contrato.get("is_sold"):
            resultado = float(estado_contrato.get("profit", 0.0))
            avisos = [f"Contrato {contrato} cerrado. Resultado: ${resultado:+.2f}"]
            aviso_riesgo = registrar_resultado(resultado, automatica)
            if aviso_riesgo:
                avisos.append(aviso_riesgo)
            try:
                await bot.send_message(chat_id=CHAT_ID, text="\n".join(avisos))
            except Exception as e:
                log.warning("No pude notificar el cierre: %s", e)
            return


async def resumen_resultados(desde: int | None) -> tuple[int, float, int, int]:
    payload = {"profit_table": 1, "description": 1, "sort": "DESC", "limit": 100}
    if desde is not None:
        payload["date_from"] = desde
    respuesta = await DERIV.request(payload)
    transacciones = respuesta["profit_table"].get("transactions", [])
    ganancia = 0.0
    ganadas = 0
    perdidas = 0
    for t in transacciones:
        neto = float(t.get("sell_price", 0)) - float(t.get("buy_price", 0))
        ganancia += neto
        if neto > 0:
            ganadas += 1
        elif neto < 0:
            perdidas += 1
    return len(transacciones), ganancia, ganadas, perdidas


async def texto_ganancias() -> str:
    inicio_dia = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    n_hoy, g_hoy, w_hoy, l_hoy = await resumen_resultados(inicio_dia)
    n_tot, g_tot, w_tot, l_tot = await resumen_resultados(None)
    tasa = w_tot / (w_tot + l_tot) if (w_tot + l_tot) else 0.0
    saldo = (await DERIV.request({"balance": 1}))["balance"]
    return (
        "Ganancias (cuenta Deriv)\n"
        f"Hoy: ${g_hoy:+.2f} · {w_hoy} ganadas, {l_hoy} perdidas\n"
        f"Últimas {n_tot} operaciones: ${g_tot:+.2f} · acierto {tasa:.0%}\n"
        f"Saldo actual: ${float(saldo['balance']):.2f} {saldo.get('currency', 'USD')}"
    )


async def preparar_df_backtest() -> pd.DataFrame:
    info = await info_simbolo()
    fijar_pip(info)
    respuesta = await DERIV.request({"ticks_history": SYMBOL, "style": "candles",
                                     "granularity": GRANULARIDAD, "count": VELAS_BACKTEST, "end": "latest"})
    velas = pd.DataFrame(respuesta.get("candles", []))
    if len(velas) < EMA_MACRO + ZLEN + HORIZONTE_BACKTEST + 10:
        raise DerivError(f"solo llegaron {len(velas)} velas")
    for columna in ("open", "high", "low", "close"):
        velas[columna] = velas[columna].astype(float)
    velas["vol_proxy"] = (velas["high"] - velas["low"]).astype(float)
    return calcular_indicadores(velas)


async def actualizar_gate() -> dict:
    df = await preparar_df_backtest()
    resultados = await asyncio.to_thread(simular_variante, df, MODO_SENAL)
    gate = fundamentos.evaluar(resultados, RIESGO_USD, BENEFICIO_USD, MIN_SENALES)
    ESTADO.gate = gate
    ESTADO.gate_hora = time.time()
    log.info("Gate de ventaja (%s): %s", MODO_SENAL, gate["motivo"])
    return gate


def bloque_variante(nombre: str, ev: dict) -> str:
    if ev["decididas"] == 0:
        return f"{nombre}: sin operaciones resueltas"
    return (
        f"{nombre}: {ev['n']} señales (ganadas {ev['ganadas']}, perdidas {ev['perdidas']}, expiradas {ev['expiradas']})\n"
        f"   acierto {ev['p']:.1%} · IC 95% [{ev['p_bajo']:.1%}, {ev['p_alto']:.1%}]\n"
        f"   valor esperado ${ev['ev']:+.3f} · cota inferior ${ev['ev_bajo']:+.3f}\n"
        f"   Kelly con cota inferior: {ev['kelly']:.1%} del capital"
    )


async def texto_backtest() -> str:
    df = await preparar_df_backtest()
    riesgo_bruto = RIESGO_USD - COMISION_USD
    beneficio_bruto = BENEFICIO_USD + COMISION_USD
    p_teorico = fundamentos.paseo_aleatorio_teorico(riesgo_bruto, beneficio_bruto)
    multiplicador = MULTIPLICADORES[0]
    cierres = df["close"].to_numpy(dtype=float)
    sigma_min = float(np.std(np.diff(np.log(cierres)), ddof=1))
    p_simulado = await asyncio.to_thread(
        fundamentos.paseo_aleatorio_gbm, sigma_min,
        riesgo_bruto / (STAKE * multiplicador), beneficio_bruto / (STAKE * multiplicador),
    )
    res_momentum = await asyncio.to_thread(simular_variante, df, "momentum")
    res_reversion = await asyncio.to_thread(simular_variante, df, "reversion")
    ev_momentum = fundamentos.evaluar(res_momentum, RIESGO_USD, BENEFICIO_USD, MIN_SENALES)
    ev_reversion = fundamentos.evaluar(res_reversion, RIESGO_USD, BENEFICIO_USD, MIN_SENALES)
    ev_activo = ev_momentum if MODO_SENAL == "momentum" else ev_reversion
    ESTADO.gate = ev_activo
    ESTADO.gate_hora = time.time()

    ruina_sin_ventaja = fundamentos.riesgo_de_ruina(p_teorico, BENEFICIO_USD, RIESGO_USD, CAPITAL_CUENTA)
    lineas_ruina = [
        f"4) Ruina en 100 operaciones con ${CAPITAL_CUENTA:.2f}:",
        f"   sin ventaja (acierto {p_teorico:.1%}): quiebra {ruina_sin_ventaja['quiebra']:.0%}, "
        f"pierde la mitad {ruina_sin_ventaja['pierde_mitad']:.0%}",
    ]
    if ev_activo["decididas"] > 0:
        ruina_observada = fundamentos.riesgo_de_ruina(ev_activo["p"], BENEFICIO_USD, RIESGO_USD, CAPITAL_CUENTA)
        lineas_ruina.append(
            f"   con el acierto observado ({ev_activo['p']:.1%}): quiebra {ruina_observada['quiebra']:.0%}, "
            f"pierde la mitad {ruina_observada['pierde_mitad']:.0%}"
        )
    ind = fundamentos.independencia(cierres)
    primera = datetime.fromtimestamp(int(df["epoch"].iloc[0]), timezone.utc).strftime("%d/%m %H:%M")
    ultima = datetime.fromtimestamp(int(df["epoch"].iloc[-1]), timezone.utc).strftime("%d/%m %H:%M")
    equilibrio = RIESGO_USD / (RIESGO_USD + BENEFICIO_USD)
    veredicto = "APTO para automático" if ev_activo["apto"] else f"NO APTO para automático: {ev_activo['motivo']}"
    return "\n".join([
        f"Backtest · {len(df)} velas de 1 min · {primera} a {ultima} UTC",
        f"Equilibrio: el acierto debe superar {equilibrio:.0%}",
        "",
        f"1) Paseo aleatorio (sin ventaja): acierto teórico {p_teorico:.1%}, simulado {p_simulado:.1%}",
        f"   valor esperado ${-COMISION_USD:+.3f} por operación, igual a la comisión",
        "",
        "2) " + bloque_variante("Momentum (actual)", ev_momentum),
        "",
        "3) " + bloque_variante("Reversión", ev_reversion),
        "",
        *lineas_ruina,
        "",
        f"5) Independencia: P(sube | subió antes) {ind['p_sube_tras_sube']:.1%} · "
        f"P(sube | bajó antes) {ind['p_sube_tras_baja']:.1%} · z {ind['z']:+.2f}",
        "",
        f"Veredicto ({MODO_SENAL}): {veredicto}",
    ])


async def ciclo_automatico(bot) -> None:
    if ESTADO.gate is None or not ESTADO.gate["apto"]:
        return
    if time.time() - ESTADO.ultima_auto < COOLDOWN_AUTO_MIN * 60:
        return
    lectura = await leer_mercado()
    if lectura is None or not lectura.abierto or lectura.epoch == ESTADO.ultima_vela:
        return
    ESTADO.ultima_vela = lectura.epoch
    direccion = senal_de(lectura.z, lectura.cierre, lectura.ema_macro, lectura.vol_inst, MODO_SENAL)
    if direccion is None:
        return
    plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr)
    if plan is None:
        log.info("Señal automática descartada: %s", motivo)
        return
    try:
        await abrir_operacion(plan, bot, automatica=True)
        ESTADO.ultima_auto = time.time()
    except Exception as e:
        log.warning("Orden automática no enviada: %s", e)


async def bucle_automatico(bot) -> None:
    while True:
        if ESTADO.auto_activo:
            try:
                if ESTADO.gate is None or time.time() - ESTADO.gate_hora > GATE_HORAS * 3600:
                    await actualizar_gate()
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
    if DERIV is not None and DERIV.cuenta:
        cuenta = f"{DERIV.cuenta.get('account_id')} ({DERIV.cuenta.get('account_type')})"
    automatico = "ACTIVO" if ESTADO.auto_activo else "apagado"
    if ESTADO.gate is None:
        ventaja = "sin medir (usa Backtest)"
    elif ESTADO.gate["apto"]:
        ventaja = "demostrada"
    else:
        ventaja = "no demostrada (automático bloqueado)"
    return (
        f"Bot {SYMBOL}\n"
        f"Cuenta: {cuenta}\n"
        f"Modo automático: {automatico} (señal: {MODO_SENAL})\n"
        f"Ventaja de la señal: {ventaja}\n"
        f"Stake ${STAKE:.2f} · riesgo ${RIESGO_USD:.2f} · beneficio ${BENEFICIO_USD:.2f}"
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
        [InlineKeyboardButton("Backtest", callback_data="backtest")],
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
    estado_mercado = "abierto" if lectura.abierto else "cerrado"
    lineas = [
        f"{SYMBOL} · vela cerrada {hora} · mercado {estado_mercado}",
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
                f"{nombre}: SL {fmt(p.sl)} · TP {fmt(p.tp)} · x{p.multiplicador} · "
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
    if not lectura.abierto:
        motivos = {1: "mercado cerrado", -1: "mercado cerrado"}
    else:
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
    if not lectura.abierto:
        await editar(q, "El mercado está cerrado; no se envió la orden.", boton_volver())
        return
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
async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    mensaje = await update.message.reply_text("Calculando backtest... puede tardar unos segundos.")
    try:
        texto = await texto_backtest()
    except Exception as e:
        log.exception("Error en /backtest")
        texto = f"No pude calcular el backtest: {e}"
    await mensaje.edit_text(texto)


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
        elif dato == "backtest":
            await editar(consulta, "Calculando backtest... puede tardar unos segundos.")
            await editar(consulta, await texto_backtest(), boton_volver())
    except Exception as e:
        log.exception("Error en el botón %s", dato)
        await editar(consulta, f"Error: {e}", boton_volver())


async def on_init(app: Application) -> None:
    global DERIV
    ESTADO.lock = asyncio.Lock()
    DERIV = DerivClient()
    DERIV.iniciar()
    ESTADO.tareas.append(asyncio.create_task(keep_alive(RENDER_URL)))
    ESTADO.tareas.append(asyncio.create_task(bucle_automatico(app.bot)))
    log.info("Servicio iniciado. Símbolo %s, señal %s, keep-alive hacia %s", SYMBOL, MODO_SENAL, RENDER_URL)


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
    aplicacion.add_handler(CommandHandler("backtest", cmd_backtest))
    aplicacion.add_handler(CallbackQueryHandler(on_boton))
    aplicacion.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
