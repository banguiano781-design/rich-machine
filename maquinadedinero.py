"""Bot híbrido Deriv + Telegram para microcuenta en Render.

Conexión a Deriv:
- Si defines DERIV_PAT y DERIV_ACCOUNT_ID usa la API NUEVA (token personal + OTP).
- Si no, sigue usando la API antigua con DERIV_TOKEN.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import math
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
import websockets
from flask import Flask
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Conflict, NetworkError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot_hibrido")
# Evita que el token de Telegram aparezca en los logs (httpx imprime la URL completa).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

def _leer_env(nombre: str, defecto: str = "") -> str:
    """Lee una variable de entorno quitando espacios, comillas y un posible 'Bearer ' de más."""
    valor = os.environ.get(nombre, defecto).strip().strip('"').strip("'").strip()
    if valor.lower().startswith("bearer "):
        valor = valor[7:].strip()
    return valor


TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0").strip() or "0")

# API antigua
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()
# API nueva
DERIV_PAT = _leer_env("DERIV_PAT")
DERIV_ACCOUNT_ID = _leer_env("DERIV_ACCOUNT_ID")
USAR_NUEVA_API = bool(DERIV_PAT and DERIV_ACCOUNT_ID) and _leer_env("DERIV_API", "nueva").lower() != "antigua"
MODO_DESCUBRIR = bool(DERIV_PAT) and not DERIV_ACCOUNT_ID

if not TELEGRAM_TOKEN or not CHAT_ID or not (DERIV_TOKEN or USAR_NUEVA_API or MODO_DESCUBRIR):
    raise SystemExit(
        "Faltan TELEGRAM_TOKEN, TELEGRAM_CHAT_ID o las credenciales de Deriv "
        "(DERIV_TOKEN, o bien DERIV_PAT + DERIV_ACCOUNT_ID)."
    )

SYMBOL = os.environ.get("DERIV_SYMBOL", "frxXAUUSD").strip()
MODO_REAL = os.environ.get("MODO_REAL", "false").strip().lower() == "true"
MULTIPLICADORES = sorted(
    {int(x) for x in os.environ.get("MULTIPLICADORES", "10,20,30,40,50,100,200").split(",") if x.strip()}
)
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", os.environ.get("RENDER_URL", "https://rich-machine.onrender.com")).rstrip("/")
PORT = int(os.environ.get("PORT", "5000"))

STAKE = float(os.environ.get("STAKE", "1"))
RIESGO_MAX_USD = float(os.environ.get("MAX_RISK", "0.40"))
RATIO_RR = float(os.environ.get("RR", "3"))
BENEFICIO_OBJETIVO = round(RIESGO_MAX_USD * RATIO_RR, 2)
SL_ATR_1M = 4.0
SL_ATR_15M = 2.0
PAUSA_TRAS_STOP = 15 * 60

ZLEN = 20
ATR_LEN = 14
EMA_MACRO = 200
VOL_LEN = 20
VOL_MULT = 2.5
Z_UMBRAL = 2.5

GRANULARIDAD = 60
VELAS = 300
TICKS = 5000
INTERVALO_KEEPALIVE = 600
INTERVALO_AUTO = 20
MIN_VELAS_MANUAL = 60      # en modo manual basta con menos velas que en automático
MAX_PAGINAS_TICKS = 6      # veces que se pide más historial de ticks si la API limita la cantidad
MAX_OPERACIONES = max(1, min(10, int(os.environ.get("MAX_OPERACIONES", "5"))))

# Hora local para decidir si es fin de semana (México = UTC-6; cámbialo con UTC_OFFSET_HORAS).
ZONA_LOCAL = timezone(timedelta(hours=float(os.environ.get("UTC_OFFSET_HORAS", "-6"))))
# El mercado de pares (forex y oro) cierra el viernes por la tarde y reabre el domingo por la tarde.
# Horas locales aproximadas (16:00 hora de México ~ 22:00 UTC). Ajústalas en Render si hace falta.
VIERNES_CIERRE_HORA = float(os.environ.get("VIERNES_CIERRE_HORA", "16"))
DOMINGO_APERTURA_HORA = float(os.environ.get("DOMINGO_APERTURA_HORA", "16"))

# Entre semana (lunes a viernes) solo estos pares. Código de Deriv -> nombre para mostrar.
PARES_SEMANA = {
    "frxXAUUSD": "Oro XAU/USD",
    "frxEURUSD": "EUR/USD",
    "frxGBPUSD": "GBP/USD",
    "frxUSDJPY": "USD/JPY",
    "frxAUDUSD": "AUD/USD",
    "frxUSDCAD": "USD/CAD",
}
# Sábado y domingo solo estos índices sintéticos (abiertos 24/7), solo para probar el bot.
INDICES_FINDE = {
    "R_10": "Volatility 10",
    "R_25": "Volatility 25",
    "R_50": "Volatility 50",
    "R_75": "Volatility 75",
    "R_100": "Volatility 100",
}

DERIV_APP_ID = _leer_env("DERIV_APP_ID", "1089") or "1089"
if DERIV_APP_ID.startswith("pat_") and DERIV_PAT and not DERIV_PAT.startswith("pat_"):
    DERIV_PAT, DERIV_APP_ID = DERIV_APP_ID, DERIV_PAT
    log.warning(
        "DERIV_PAT y DERIV_APP_ID estaban al revés en Render; los intercambié en memoria. "
        "Corrígelos en Render para que queden en su variable correcta."
    )
DERIV_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
DERIV_REST = "https://api.derivws.com"

# Cabeceras de navegador: Cloudflare puede rechazar (520) clientes que no parecen un navegador.
CABECERAS_NAVEGADOR = {
    "Origin": "https://deriv.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}
try:
    _params_ws = inspect.signature(websockets.connect).parameters
except (TypeError, ValueError):
    _params_ws = {}
# websockets >= 14 usa "additional_headers"; las versiones anteriores usan "extra_headers".
CLAVE_CABECERAS = "additional_headers" if "additional_headers" in _params_ws else "extra_headers"

if USAR_NUEVA_API and DERIV_APP_ID == "1089":
    _definida = "DERIV_APP_ID" in os.environ
    _parecidas = sorted(k for k in os.environ if "DERIV" in k.upper() or "APP_ID" in k.upper())
    raise SystemExit(
        "DERIV_APP_ID no sirve para la API nueva: "
        + ("la variable existe pero vale 1089 (o está vacía)." if _definida else "la variable NO está definida con ese nombre exacto.")
        + " Pon el App ID propio (el de 21 caracteres de developers.deriv.com), con el nombre exacto DERIV_APP_ID, "
        "sin espacios ni comillas. "
        f"Variables con nombre parecido que veo en Render: {_parecidas}"
    )

# En la API nueva el campo "symbol" se llama "underlying_symbol".
CLAVE_SIMBOLO = "underlying_symbol" if USAR_NUEVA_API else "symbol"

flask_app = Flask(__name__)


@flask_app.route("/")
def inicio():
    return "Bot híbrido activo", 200


def _servir_web() -> None:
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


def iniciar_servidor_web() -> None:
    threading.Thread(target=_servir_web, name="flask-keepalive", daemon=True).start()


async def keep_alive(url: str) -> None:
    while True:
        await asyncio.sleep(INTERVALO_KEEPALIVE)
        try:
            resp = await asyncio.to_thread(requests.get, url, timeout=20)
            log.info("Keep-alive %s -> %s", url, resp.status_code)
        except Exception as exc:
            log.warning("Keep-alive falló: %s", exc)


class DerivError(Exception):
    pass


def detalle_rechazo(exc: Exception) -> str:
    """Si el servidor rechazó la conexión, devuelve datos útiles de su respuesta (sin secretos)."""
    resp = getattr(exc, "response", None)
    cabeceras = getattr(resp, "headers", None) or getattr(exc, "headers", None)
    partes = []
    if cabeceras is not None:
        for clave in ("server", "cf-ray"):
            try:
                valor = cabeceras.get(clave)
            except Exception:
                valor = None
            if valor:
                partes.append(f"{clave}={valor}")
    cuerpo = getattr(resp, "body", None)
    if cuerpo:
        crudo = cuerpo[:3000].decode("utf-8", "replace") if isinstance(cuerpo, (bytes, bytearray)) else str(cuerpo)[:3000]
        texto = " ".join(re.sub(r"<[^>]+>", " ", crudo).split())
        if texto:
            partes.append("cuerpo=" + texto[:160])
    return (" | " + "; ".join(partes)) if partes else ""


def listar_cuentas() -> str:
    """Pide por REST tus cuentas de la API nueva (para copiar el ID de la demo)."""
    resp = requests.get(
        f"{DERIV_REST}/trading/v1/options/accounts",
        headers={"Authorization": f"Bearer {DERIV_PAT}", "Deriv-App-ID": DERIV_APP_ID},
        timeout=20,
    )
    cuerpo = " ".join(resp.text.split())[:1500]
    return f"HTTP {resp.status_code}: {cuerpo}"


async def descubrir_cuentas() -> None:
    """Modo ayuda: falta DERIV_ACCOUNT_ID. Escribe en el log tus cuentas y la respuesta de Deriv."""
    while True:
        try:
            if DERIV_APP_ID == "1089":
                log.error("[DESCUBRIR] Falta DERIV_APP_ID: el 1089 es de la API antigua. Pon el App ID nuevo.")
            else:
                log.info(
                    "[DESCUBRIR] Token: %s car. (empieza con pat_: %s) | App ID: %s car.",
                    len(DERIV_PAT),
                    "si" if DERIV_PAT.startswith("pat_") else "NO",
                    len(DERIV_APP_ID),
                )
                texto = await asyncio.to_thread(listar_cuentas)
                log.info("[DESCUBRIR] Respuesta de Deriv -> %s", texto)
                log.info("[DESCUBRIR] Si salió HTTP 200, copia el ID de la cuenta DEMO a DERIV_ACCOUNT_ID en Render.")
        except Exception as exc:
            log.warning("[DESCUBRIR] Error: %s", exc)
        await asyncio.sleep(120)


def _buscar_wss(obj):
    """Busca la primera cadena que empiece con wss:// dentro de una respuesta JSON."""
    if isinstance(obj, str):
        return obj if obj.startswith("wss://") else None
    if isinstance(obj, dict):
        for valor in obj.values():
            hallado = _buscar_wss(valor)
            if hallado:
                return hallado
    if isinstance(obj, list):
        for valor in obj:
            hallado = _buscar_wss(valor)
            if hallado:
                return hallado
    return None


def pedir_url_otp() -> str:
    """API nueva: pide por REST una URL de WebSocket ya autenticada (OTP)."""
    resp = requests.post(
        f"{DERIV_REST}/trading/v1/options/accounts/{DERIV_ACCOUNT_ID}/otp",
        headers={"Authorization": f"Bearer {DERIV_PAT}", "Deriv-App-ID": DERIV_APP_ID},
        timeout=20,
    )
    if resp.status_code != 200:
        pistas = {
            401: "token inválido, vencido o mal copiado, o App ID vacío/incorrecto",
            403: "el token no tiene el permiso 'trade'",
            404: "DERIV_ACCOUNT_ID no existe para ese token",
            429: "demasiadas peticiones, espera un momento",
        }
        pista = pistas.get(resp.status_code)
        pista = f" -> {pista}" if pista else ""
        raise DerivError(f"OTP rechazado (HTTP {resp.status_code}){pista}: {resp.text[:300]}")
    try:
        datos = resp.json()
    except ValueError as exc:
        raise DerivError("La respuesta del OTP no es JSON") from exc
    url = _buscar_wss(datos)
    if not url:
        campos = list(datos)[:10] if isinstance(datos, dict) else type(datos).__name__
        raise DerivError(f"Respuesta de OTP sin URL de WebSocket (campos: {campos})")
    return url


def es_cuenta_demo(url_ws: str) -> bool:
    """API nueva: consulta si DERIV_ACCOUNT_ID es demo o real; si falla, lo deduce de la URL."""
    try:
        resp = requests.get(
            f"{DERIV_REST}/trading/v1/options/accounts",
            headers={"Authorization": f"Bearer {DERIV_PAT}", "Deriv-App-ID": DERIV_APP_ID},
            timeout=20,
        )
        if resp.status_code == 200:
            for cuenta in resp.json().get("data", []):
                if cuenta.get("account_id") == DERIV_ACCOUNT_ID:
                    return cuenta.get("account_type") == "demo"
    except Exception as exc:
        log.warning("No pude consultar el tipo de cuenta: %s", exc)
    return "/ws/demo" in url_ws


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
        if self._tarea:
            self._tarea.cancel()

    async def request(self, payload: dict, timeout: float = 25) -> dict:
        await asyncio.wait_for(self.listo.wait(), timeout=30)
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
        finally:
            self._pendientes.pop(rid, None)
        if "error" in msg:
            raise DerivError(msg["error"].get("message", "error de Deriv"))
        return msg

    async def _ping_aplicacion(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                await ws.send(json.dumps({"ping": 1}))
        except Exception:
            return

    async def _mantener(self) -> None:
        espera = 2
        while True:
            ping_tarea = None
            url = None
            try:
                if USAR_NUEVA_API:
                    # Un OTP nuevo en cada intento de conexión.
                    url = await asyncio.to_thread(pedir_url_otp)
                else:
                    url = self.url
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=20,
                    **{CLAVE_CABECERAS: CABECERAS_NAVEGADOR},
                ) as ws:
                    self.ws = ws
                    if USAR_NUEVA_API:
                        # La URL del OTP ya viene autenticada: no hay mensaje "authorize".
                        es_demo = await asyncio.to_thread(es_cuenta_demo, url)
                        self.info = {
                            "loginid": DERIV_ACCOUNT_ID,
                            "is_virtual": 1 if es_demo else 0,
                        }
                    else:
                        await ws.send(json.dumps({"authorize": self.token}))
                        resp = json.loads(await ws.recv())
                        if "error" in resp:
                            raise DerivError(resp["error"].get("message", "token inválido"))
                        self.info = resp["authorize"]
                    self.listo.set()
                    espera = 2
                    ping_tarea = asyncio.create_task(self._ping_aplicacion(ws))
                    log.info("Deriv conectado: %s", self.info.get("loginid"))
                    async for raw in ws:
                        msg = json.loads(raw)
                        fut = self._pendientes.get(msg.get("req_id"))
                        if fut and not fut.done():
                            fut.set_result(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                destino = urlparse(url).netloc if url else "paso OTP (REST)"
                log.warning(
                    "Conexión con Deriv perdida [API %s -> %s]: %s%s",
                    "NUEVA" if USAR_NUEVA_API else "antigua",
                    destino,
                    exc,
                    detalle_rechazo(exc),
                )
            finally:
                if ping_tarea:
                    ping_tarea.cancel()
                self.ws = None
                self.listo.clear()
                for fut in self._pendientes.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("conexión con Deriv perdida"))
                self._pendientes.clear()
            await asyncio.sleep(espera + random.random())
            espera = min(espera * 2, 60)


DERIV: DerivClient | None = None


@dataclass
class Lectura:
    epoch: int
    cierre: float
    precio_vivo: float
    z: float
    atr: float
    atr_stop: float
    ema_macro: float
    vol_inst: bool
    nota: str = ""


def proxy_volumen(precios: list, tiempos: list) -> pd.Series:
    precios_s = pd.Series(precios, dtype="float64")
    tiempos_s = pd.Series(tiempos, dtype="int64")
    movimiento = precios_s.diff().abs().fillna(0.0)
    bucket = (tiempos_s // GRANULARIDAD) * GRANULARIDAD
    return movimiento.groupby(bucket.to_numpy()).sum()


def calcular_indicadores(velas: pd.DataFrame) -> pd.DataFrame:
    df = velas.copy()
    cierre = df["close"]
    df["ema_z"] = cierre.ewm(span=ZLEN, adjust=False).mean()
    df["desv"] = cierre.rolling(ZLEN).std(ddof=0)
    df["z"] = (cierre - df["ema_z"]) / df["desv"].replace(0, np.nan)
    prev = cierre.shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev).abs(),
            (df["low"] - prev).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / ATR_LEN, adjust=False).mean()
    df["ema_macro"] = cierre.ewm(span=EMA_MACRO, adjust=False).mean()
    df["vol_media"] = df["vol_proxy"].rolling(VOL_LEN).mean()
    df["vol_inst"] = df["vol_proxy"] > df["vol_media"] * VOL_MULT
    return df


async def obtener_pip(simbolo: str) -> tuple[float, int]:
    respuesta = await DERIV.request({"active_symbols": "brief"})
    for item in respuesta.get("active_symbols", []):
        # API nueva: underlying_symbol / pip_size. API antigua: symbol / pip.
        nombre = item.get("underlying_symbol") or item.get("symbol")
        if nombre == simbolo:
            pip = float(item.get("pip_size") or item.get("pip") or 0.00001)
            return pip, max(0, int(round(-math.log10(pip))))
    return 0.00001, 5


def redondear(precio: float) -> float:
    pip = ESTADO.pip or 0.00001
    return round(round(precio / pip) * pip, ESTADO.decimales)


async def obtener_ticks(simbolo: str, desde: int) -> tuple[list[float], list[int]]:
    """Trae ticks hacia atrás hasta cubrir 'desde' (epoch).

    Si la API limita cuántos ticks entrega por llamada, pide más páginas hacia atrás.
    """
    precios: list[float] = []
    tiempos: list[int] = []
    fin: int | str = "latest"
    for _ in range(MAX_PAGINAS_TICKS):
        try:
            r = await DERIV.request({"ticks_history": simbolo, "style": "ticks", "count": TICKS, "end": fin})
        except DerivError:
            if not tiempos:
                raise
            break
        hist = r.get("history") or {}
        p = [float(x) for x in hist.get("prices", [])]
        t = [int(x) for x in hist.get("times", [])]
        if not t:
            break
        precios = p + precios
        tiempos = t + tiempos
        if tiempos[0] <= desde:
            break
        nuevo_fin = t[0] - 1
        if fin != "latest" and nuevo_fin >= fin:
            break
        fin = nuevo_fin
    return precios, tiempos


async def leer_mercado(estricto: bool = True) -> Lectura | None:
    """Lee velas y ticks del par actual.

    estricto=True (automático): exige historial completo y filtro de volumen.
    estricto=False (manual): tolera pocas velas o ticks incompletos y lo avisa en la nota.
    Si devuelve None, el motivo queda en ESTADO.motivo_datos.
    """
    simbolo = ESTADO.simbolo
    ESTADO.motivo_datos = ""
    if ESTADO.pip is None:
        ESTADO.pip, ESTADO.decimales = await obtener_pip(simbolo)

    velas_r, velas15_r = await asyncio.gather(
        DERIV.request(
            {"ticks_history": simbolo, "style": "candles", "granularity": GRANULARIDAD, "count": VELAS, "end": "latest"}
        ),
        DERIV.request(
            {"ticks_history": simbolo, "style": "candles", "granularity": 900, "count": 80, "end": "latest"}
        ),
    )
    velas = pd.DataFrame(velas_r.get("candles") or [])
    minimo = (EMA_MACRO + 5) if estricto else MIN_VELAS_MANUAL
    if len(velas) < minimo:
        ESTADO.motivo_datos = (
            f"llegaron {len(velas)} velas de 1 minuto y necesito al menos {minimo} "
            "(¿mercado cerrado o par sin datos?)"
        )
        return None
    for col in ("open", "high", "low", "close"):
        velas[col] = pd.to_numeric(velas[col]).astype(float)
    velas["epoch"] = pd.to_numeric(velas["epoch"]).astype("int64")

    ventana = velas["epoch"].iloc[-(VOL_LEN + 1) : -1]
    precios, tiempos = await obtener_ticks(simbolo, int(ventana.iloc[0]))
    if not precios:
        ESTADO.motivo_datos = "no llegaron ticks del par"
        return None
    proxy = proxy_volumen(precios, tiempos)
    velas["vol_proxy"] = velas["epoch"].map(proxy).fillna(0.0).astype(float)

    faltan = int((~ventana.isin(proxy.index)).sum())
    cobertura = faltan == 0
    nota = ""
    if not cobertura:
        if estricto:
            ESTADO.motivo_datos = (
                f"los {len(precios)} ticks recibidos no cubren las últimas {VOL_LEN} velas (faltan {faltan})"
            )
            return None
        nota = "Aviso: sin filtro de volumen (los ticks recibidos no cubren las últimas velas)."
    if len(velas) < EMA_MACRO + 5:
        nota = (nota + " " if nota else "") + f"Aviso: pocas velas ({len(velas)}); la tendencia (EMA 200) es aproximada."

    ind = calcular_indicadores(velas)
    cerrada = ind.iloc[-2]
    if pd.isna(cerrada["z"]) or pd.isna(cerrada["atr"]):
        ESTADO.motivo_datos = "indicadores sin valor todavía (precio sin variación reciente)"
        return None
    atr1 = float(cerrada["atr"])
    marco = pd.DataFrame(velas15_r.get("candles") or [])
    atr15 = atr_cerrado(marco) if len(marco) > 20 else 0.0
    atr_stop = max(atr1 * SL_ATR_1M, atr15 * SL_ATR_15M)
    return Lectura(
        epoch=int(cerrada["epoch"]),
        cierre=float(cerrada["close"]),
        precio_vivo=precios[-1],
        z=float(cerrada["z"]),
        atr=atr1,
        atr_stop=atr_stop,
        ema_macro=float(cerrada["ema_macro"]),
        vol_inst=bool(cerrada["vol_inst"]) if cobertura else False,
        nota=nota,
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


def atr_cerrado(df: pd.DataFrame) -> float:
    cierre = df["close"].astype(float)
    alto = df["high"].astype(float)
    bajo = df["low"].astype(float)
    prev = cierre.shift(1)
    tr = pd.concat([alto - bajo, (alto - prev).abs(), (bajo - prev).abs()], axis=1).max(axis=1)
    serie = tr.ewm(alpha=1 / ATR_LEN, adjust=False).mean()
    if len(serie) < 3 or pd.isna(serie.iloc[-2]):
        return 0.0
    return float(serie.iloc[-2])


async def multiplicadores_del_simbolo(simbolo: str) -> tuple[list[int], bool]:
    """Devuelve (multiplicadores disponibles, si se encontró alguno)."""
    # La API nueva ya no acepta "currency" en contracts_for.
    respuesta = await DERIV.request({"contracts_for": simbolo})
    hallados: set[int] = set()
    for contrato in respuesta.get("contracts_for", {}).get("available", []):
        if contrato.get("contract_type") not in ("MULTUP", "MULTDOWN"):
            continue
        for valor in contrato.get("multiplier_range") or []:
            hallados.add(int(valor))
    return sorted(hallados), bool(hallados)


def planificar(direccion: int, entrada: float, atr_stop: float, permitidos: list[int]) -> tuple[Plan | None, str]:
    if not atr_stop or atr_stop <= 0 or entrada <= 0:
        return None, "ATR no valido"
    dist_sl = atr_stop
    viables = []
    for multiplicador in sorted(permitidos):
        riesgo = STAKE * multiplicador * dist_sl / entrada
        if 0.01 <= riesgo <= RIESGO_MAX_USD + 1e-9:
            viables.append((multiplicador, riesgo))
    if not viables:
        return None, "el ATR no cabe en el riesgo: no opero"
    multiplicador, riesgo = viables[0]
    riesgo = round(riesgo, 2)
    beneficio = round(riesgo * RATIO_RR, 2)
    if riesgo <= 0 or beneficio < round(riesgo * RATIO_RR, 2) - 0.001:
        return None, "ratio roto"
    if beneficio <= 0.03 or riesgo > RIESGO_MAX_USD + 1e-9:
        return None, "microganancia o riesgo fuera de tope"
    dist_tp = dist_sl * RATIO_RR
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


def plan_manual(direccion: int, entrada: float, atr_stop: float, sl_usd: float, tp_usd: float, permitidos: list[int]) -> tuple[Plan | None, str]:
    if entrada <= 0 or atr_stop <= 0:
        return None, "ATR no valido"
    if sl_usd < 0.15:
        return None, "stop demasiado bajo"
    if tp_usd <= sl_usd:
        return None, "el take profit tiene que ser mayor que el stop"
    lista = sorted(permitidos) or sorted(MULTIPLICADORES)
    techo = sl_usd * entrada / (STAKE * atr_stop)
    candidatos = [m for m in lista if m <= techo + 1e-9]
    if not candidatos:
        minimo = lista[0]
        necesario = STAKE * minimo * atr_stop / entrada
        return None, f"ese stop queda dentro del ruido. Para este ATR el minimo es ${necesario:.2f}"
    multiplicador = candidatos[-1]
    dist_sl = sl_usd * entrada / (STAKE * multiplicador)
    dist_tp = tp_usd * entrada / (STAKE * multiplicador)
    plan = Plan(
        direccion=direccion,
        multiplicador=multiplicador,
        entrada=entrada,
        sl=redondear(entrada - direccion * dist_sl),
        tp=redondear(entrada + direccion * dist_tp),
        riesgo_usd=round(sl_usd, 2),
        beneficio_usd=round(tp_usd, 2),
    )
    return plan, ""


def fmt(precio: float) -> str:
    return f"{precio:.7f}".rstrip("0").rstrip(".")


def texto_alerta(plan: Plan) -> str:
    tendencia = "alcista" if plan.direccion > 0 else "bajista"
    operacion = "compra" if plan.direccion > 0 else "venta"
    return (
        f"la tendencia es '{tendencia}' cuanto quieres ganar {plan.beneficio_usd:.2f} "
        f"y cuanto es el stop lose {plan.riesgo_usd:.2f}; entre en {operacion}\n"
        f"precio entrada {fmt(plan.entrada)} | precio tp {fmt(plan.tp)} | precio sl {fmt(plan.sl)}\n"
        f"multiplicador x{plan.multiplicador} | esos montos son dolares, no el precio de {nombre_simbolo(ESTADO.simbolo)}"
    )


def es_finde() -> bool:
    """True mientras el mercado de pares está cerrado: del viernes por la tarde al domingo por la tarde (hora local)."""
    ahora = datetime.now(ZONA_LOCAL)
    hora = ahora.hour + ahora.minute / 60
    dia = ahora.weekday()  # 0 = lunes ... 6 = domingo
    if dia == 4:
        return hora >= VIERNES_CIERRE_HORA
    if dia == 5:
        return True
    if dia == 6:
        return hora < DOMINGO_APERTURA_HORA
    return False


def hora_local_texto() -> str:
    dias = ["lun", "mar", "mié", "jue", "vie", "sáb", "dom"]
    ahora = datetime.now(ZONA_LOCAL)
    return f"{dias[ahora.weekday()]} {ahora.strftime('%H:%M')}"


def catalogo_hoy() -> dict[str, str]:
    return INDICES_FINDE if es_finde() else PARES_SEMANA


def nombre_simbolo(codigo: str) -> str:
    return PARES_SEMANA.get(codigo) or INDICES_FINDE.get(codigo) or codigo


def simbolo_permitido(codigo: str) -> bool:
    return codigo in catalogo_hoy()


def descripcion_dia() -> str:
    if es_finde():
        return "Fin de semana (mercado de pares cerrado): solo índices sintéticos, modo prueba del bot."
    return "Entre semana: solo pares (forex y oro)."


def simbolo_inicial() -> str:
    hoy = catalogo_hoy()
    return SYMBOL if SYMBOL in hoy else next(iter(hoy))


class Estado:
    def __init__(self) -> None:
        self.auto_activo = False
        self.ultima_vela: int | None = None
        self.pausa_hasta = 0.0
        self.multiplicadores: list[int] = list(MULTIPLICADORES)
        self.lock: asyncio.Lock | None = None
        self.tareas: list[asyncio.Task] = []
        self.vigilancias: set[asyncio.Task] = set()
        self.simbolo = simbolo_inicial()
        self.pip: float | None = None
        self.decimales = 5
        self.motivo_datos = ""
        self.ultimo_estado = ""
        self._ultimo_texto_auto = ""
        self.manual_paso = ""
        self.manual_n = 0
        self.manual_modo = ""
        self.manual_sl = 0.0
        self.manual_planes: list[Plan] = []


ESTADO = Estado()


def estado_auto(texto: str) -> None:
    """Guarda (y registra una sola vez) qué está haciendo el modo automático."""
    ESTADO.ultimo_estado = f"{datetime.now(ZONA_LOCAL).strftime('%H:%M')} {texto}"
    if texto != ESTADO._ultimo_texto_auto:
        ESTADO._ultimo_texto_auto = texto
        log.info("Auto (%s): %s", ESTADO.simbolo, texto)


async def cambiar_simbolo(codigo: str) -> str:
    """Cambia el par activo. Devuelve un aviso si no se pudo, o '' si todo salió bien."""
    nombre = nombre_simbolo(codigo)
    aviso = ""
    if not simbolo_permitido(codigo):
        aviso = f"{nombre} no está habilitado hoy. {descripcion_dia()}"
    else:
        try:
            lista, ok = await multiplicadores_del_simbolo(codigo)
        except Exception as exc:
            lista, ok = [], False
            aviso = f"No pude verificar {nombre}: {exc}"
        else:
            if not ok:
                aviso = f"{nombre} no tiene multiplicadores disponibles ahora (mercado cerrado o no ofrecido). Elige otro."
    if aviso:
        log.warning("Cambio de par rechazado (%s): %s", codigo, aviso)
        return aviso
    ESTADO.simbolo = codigo
    ESTADO.pip = None
    ESTADO.decimales = 5
    ESTADO.multiplicadores = lista
    ESTADO.ultima_vela = None
    cancelar_manual()
    log.info("Par cambiado a %s (multiplicadores %s)", codigo, lista)
    return ""


def verificar_modo() -> None:
    es_demo = bool(DERIV.info.get("is_virtual"))
    if (not es_demo) != MODO_REAL:
        raise RuntimeError("El token no coincide con MODO_REAL. Con MODO_REAL=false solo se opera en demo.")


async def abrir_operacion(plan: Plan, bot, multiple: bool = False, avisar: bool = True) -> str:
    simbolo = ESTADO.simbolo
    if not simbolo_permitido(simbolo):
        raise RuntimeError(f"{nombre_simbolo(simbolo)} no está habilitado hoy. {descripcion_dia()}")
    async with ESTADO.lock:
        verificar_modo()
        if not multiple:
            abiertos = await DERIV.request({"portfolio": 1})
            if abiertos["portfolio"].get("contracts"):
                raise RuntimeError("ya hay una operacion abierta; espero a que cierre")
        tipo = "MULTUP" if plan.direccion > 0 else "MULTDOWN"
        propuesta = (
            await DERIV.request(
                {
                    "proposal": 1,
                    "amount": STAKE,
                    "basis": "stake",
                    "contract_type": tipo,
                    "currency": "USD",
                    CLAVE_SIMBOLO: simbolo,
                    "multiplier": plan.multiplicador,
                    "limit_order": {
                        "stop_loss": plan.riesgo_usd,
                        "take_profit": plan.beneficio_usd,
                    },
                }
            )
        )["proposal"]
        # En la API nueva ask_price puede llegar como texto.
        compra = (
            await DERIV.request({"buy": propuesta["id"], "price": float(propuesta["ask_price"])})
        )["buy"]
        contrato = str(compra["contract_id"])
    if avisar:
        await bot.send_message(chat_id=CHAT_ID, text=texto_alerta(plan))
    tarea = asyncio.create_task(vigilar_contrato(contrato, bot))
    ESTADO.vigilancias.add(tarea)
    tarea.add_done_callback(ESTADO.vigilancias.discard)
    return contrato


async def vigilar_contrato(contrato: str, bot) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            respuesta = await DERIV.request({"proposal_open_contract": 1, "contract_id": int(contrato) if contrato.isdigit() else contrato})
        except Exception as exc:
            log.warning("No pude consultar el contrato %s: %s", contrato, exc)
            continue
        contrato_abierto = respuesta["proposal_open_contract"]
        if contrato_abierto.get("is_sold"):
            resultado = float(contrato_abierto.get("profit", 0.0))
            if resultado < 0:
                ESTADO.pausa_hasta = time.time() + PAUSA_TRAS_STOP
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"Contrato {contrato} cerrado. Resultado: ${resultado:+.2f}",
                )
            except Exception as exc:
                log.warning("No pude notificar el cierre: %s", exc)
            return


async def resumen_resultados(desde: int | None) -> tuple[int, float]:
    payload = {"profit_table": 1, "description": 1, "sort": "DESC", "limit": 100}
    if desde is not None:
        payload["date_from"] = desde
    respuesta = await DERIV.request(payload)
    txs = respuesta["profit_table"].get("transactions", [])
    ganancia = sum(float(t.get("sell_price", 0)) - float(t.get("buy_price", 0)) for t in txs)
    return len(txs), ganancia


async def texto_ganancias() -> str:
    ahora = datetime.now(timezone.utc)
    inicio_dia = int(ahora.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    n_hoy, g_hoy = await resumen_resultados(inicio_dia)
    n_tot, g_tot = await resumen_resultados(None)
    saldo = (await DERIV.request({"balance": 1}))["balance"]
    return (
        "Ganancias (cuenta Deriv)\n"
        f"Hoy: ${g_hoy:+.2f} en {n_hoy} operaciones\n"
        f"Ultimas {n_tot} operaciones: ${g_tot:+.2f}\n"
        f"Saldo actual: ${float(saldo['balance']):.2f} {saldo.get('currency', 'USD')}"
    )


async def ciclo_automatico(bot) -> None:
    if not simbolo_permitido(ESTADO.simbolo):
        estado_auto("el par actual no está habilitado hoy; elige otro en 'Cambiar par'")
        return
    lectura = await leer_mercado(estricto=True)
    if lectura is None:
        estado_auto(f"sin datos suficientes: {ESTADO.motivo_datos}")
        return
    if lectura.epoch == ESTADO.ultima_vela:
        return
    ESTADO.ultima_vela = lectura.epoch
    if time.time() < ESTADO.pausa_hasta:
        estado_auto("en pausa tras un stop")
        return
    direccion = direccion_senal(lectura)
    if direccion is None:
        estado_auto(f"sin señal (z={lectura.z:+.2f}, volumen {'alto' if lectura.vol_inst else 'normal'})")
        return
    plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr_stop, ESTADO.multiplicadores)
    if plan is None:
        estado_auto(f"señal descartada: {motivo}")
        return
    try:
        await abrir_operacion(plan, bot)
        estado_auto("operación abierta")
    except Exception as exc:
        estado_auto(f"orden no enviada: {exc}")


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


def fase_mercado(lectura: Lectura) -> str:
    arriba = lectura.cierre > lectura.ema_macro
    if lectura.z >= Z_UMBRAL and arriba:
        return "pico de compra"
    if lectura.z <= -Z_UMBRAL and not arriba:
        return "bache de ventas"
    if lectura.z >= Z_UMBRAL:
        return "pico de compra contra la tendencia"
    if lectura.z <= -Z_UMBRAL:
        return "bache de ventas contra la tendencia"
    if arriba:
        return "tendencia alcista, sin pico"
    return "tendencia bajista, sin bache"


def texto_manual(lectura: Lectura) -> str:
    tendencia = "alcista" if lectura.cierre > lectura.ema_macro else "bajista"
    extra = f"\n{lectura.nota}" if lectura.nota else ""
    return (
        f"Par: {nombre_simbolo(ESTADO.simbolo)}\n"
        f"Tendencia: {tendencia}\n"
        f"Fase: {fase_mercado(lectura)}\n"
        f"Z-Score: {lectura.z:+.2f}\n"
        f"Stop de precio que pide el ATR: {lectura.atr_stop:.2f}\n"
        f"Precio: {fmt(redondear(lectura.precio_vivo))}{extra}\n\n"
        "¿Cuántas operaciones quieres abrir?"
    )


def direccion_senal(lectura: Lectura) -> int | None:
    if not lectura.vol_inst:
        return None
    if lectura.z >= Z_UMBRAL and lectura.cierre > lectura.ema_macro:
        return 1
    if lectura.z <= -Z_UMBRAL and lectura.cierre < lectura.ema_macro:
        return -1
    return None


def texto_menu() -> str:
    cuenta = "sin conexión"
    if DERIV is not None and DERIV.info:
        tipo = "DEMO" if DERIV.info.get("is_virtual") else "REAL"
        cuenta = f"{DERIV.info.get('loginid')} ({tipo})"
    auto = "ACTIVO" if ESTADO.auto_activo else "apagado"
    lineas = [
        f"Par: {nombre_simbolo(ESTADO.simbolo)} ({ESTADO.simbolo})",
        f"Cuenta: {cuenta}",
        f"Hora del bot: {hora_local_texto()} (UTC{ZONA_LOCAL.utcoffset(None).total_seconds() / 3600:+.0f})",
        descripcion_dia(),
        f"Modo automático: {auto}",
    ]
    if ESTADO.auto_activo and ESTADO.ultimo_estado:
        lineas.append(f"Último análisis: {ESTADO.ultimo_estado}")
    lineas += [
        f"Stake ${STAKE:.2f} · stop hasta ${RIESGO_MAX_USD:.2f} · beneficio x{RATIO_RR:.0f}",
        "/start no abre operaciones.",
    ]
    return "\n".join(lineas)


def teclado_menu() -> InlineKeyboardMarkup:
    if ESTADO.auto_activo:
        boton_auto = InlineKeyboardButton("Detener Modo Automatico", callback_data="auto:off")
    else:
        boton_auto = InlineKeyboardButton("Modo Automatico", callback_data="auto:on")
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Modo Manual", callback_data="manual")],
            [boton_auto],
            [InlineKeyboardButton("Cambiar par", callback_data="pares")],
            [InlineKeyboardButton("Ganancias", callback_data="ganancias")],
        ]
    )


def teclado_pares() -> InlineKeyboardMarkup:
    botones = [
        InlineKeyboardButton(("✓ " if codigo == ESTADO.simbolo else "") + nombre, callback_data=f"par:{codigo}")
        for codigo, nombre in catalogo_hoy().items()
    ]
    filas = [botones[i : i + 2] for i in range(0, len(botones), 2)]
    filas.append([InlineKeyboardButton("Menu", callback_data="menu")])
    return InlineKeyboardMarkup(filas)


def teclado_numero() -> InlineKeyboardMarkup:
    botones = [InlineKeyboardButton(str(k), callback_data=f"n:{k}") for k in range(1, MAX_OPERACIONES + 1)]
    filas = [botones[i : i + 5] for i in range(0, len(botones), 5)]
    filas.append([InlineKeyboardButton("Menu", callback_data="menu")])
    return InlineKeyboardMarkup(filas)


def teclado_direccion(n: int) -> InlineKeyboardMarkup:
    filas = [
        [
            InlineKeyboardButton("Todas COMPRA", callback_data="dir:compra"),
            InlineKeyboardButton("Todas VENTA", callback_data="dir:venta"),
        ]
    ]
    if n >= 2:
        filas.append([InlineKeyboardButton("Mitad y mitad", callback_data="dir:mixto")])
    filas.append([InlineKeyboardButton("Menu", callback_data="menu")])
    return InlineKeyboardMarkup(filas)


def teclado_confirmar() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Confirmar", callback_data="ok"), InlineKeyboardButton("Cancelar", callback_data="menu")]]
    )


def direcciones_manuales(modo: str, n: int) -> list[int]:
    if modo == "compra":
        return [1] * n
    if modo == "venta":
        return [-1] * n
    return [1] * ((n + 1) // 2) + [-1] * (n // 2)


def resumen_manual(planes: list[Plan]) -> str:
    n = len(planes)
    compras = sum(1 for p in planes if p.direccion > 0)
    ventas = n - compras
    partes = []
    if compras:
        partes.append(f"{compras} compra")
    if ventas:
        partes.append(f"{ventas} venta")
    riesgo = sum(p.riesgo_usd for p in planes)
    ganancia = sum(p.beneficio_usd for p in planes)
    base = planes[0]
    return (
        f"Resumen en {nombre_simbolo(ESTADO.simbolo)}:\n"
        f"{n} operaciones ({' + '.join(partes)}), multiplicador x{base.multiplicador}\n"
        f"Cada una: stop ${base.riesgo_usd:.2f} · take profit ${base.beneficio_usd:.2f}\n"
        f"Riesgo total máximo: ${riesgo:.2f} · ganancia total objetivo: ${ganancia:.2f}\n"
        "¿Confirmas?"
    )


def boton_volver() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Menu", callback_data="menu")]])


async def editar(query, texto: str, markup=None) -> None:
    try:
        await query.edit_message_text(texto, reply_markup=markup)
    except BadRequest as exc:
        if "not modified" not in str(exc).lower():
            raise


async def mostrar_menu_manual(query) -> None:
    cancelar_manual()
    if not simbolo_permitido(ESTADO.simbolo):
        await editar(
            query,
            f"{nombre_simbolo(ESTADO.simbolo)} no está habilitado hoy. {descripcion_dia()}\nElige otro par.",
            teclado_pares(),
        )
        return
    await editar(query, "Analizando el mercado...")
    lectura = await leer_mercado(estricto=False)
    if lectura is None:
        await editar(
            query,
            f"Datos insuficientes por ahora: {ESTADO.motivo_datos or 'faltan velas o ticks'}.",
            boton_volver(),
        )
        return
    await editar(query, texto_manual(lectura), teclado_numero())


def cancelar_manual() -> None:
    ESTADO.manual_paso = ""
    ESTADO.manual_n = 0
    ESTADO.manual_modo = ""
    ESTADO.manual_sl = 0.0
    ESTADO.manual_planes = []


async def ejecutar_manual(query, bot) -> None:
    planes = list(ESTADO.manual_planes)
    if ESTADO.manual_paso != "confirmar" or not planes:
        await editar(query, "No hay ninguna orden pendiente de confirmar.", boton_volver())
        return
    cancelar_manual()  # evita que un doble toque abra las órdenes dos veces
    await editar(query, f"Abriendo {len(planes)} operación(es)...")
    abiertos: list[str] = []
    error = ""
    for plan in planes:
        try:
            abiertos.append(await abrir_operacion(plan, bot, multiple=True, avisar=False))
        except Exception as exc:
            error = str(exc)
            break
    texto = f"Abiertas {len(abiertos)} de {len(planes)} en {nombre_simbolo(ESTADO.simbolo)}."
    if abiertos:
        texto += "\nContratos: " + ", ".join(abiertos)
    if error:
        texto += f"\nSe detuvo: {error}"
    await editar(query, texto, boton_volver())


@solo_dueno
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cancelar_manual()
    await update.message.reply_text(texto_menu(), reply_markup=teclado_menu())


@solo_dueno
async def cmd_ganancias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        texto = await texto_ganancias()
    except Exception as exc:
        log.exception("Error en /ganancias")
        texto = f"No pude consultar las ganancias: {exc}"
    await update.message.reply_text(texto)


@solo_dueno
async def on_boton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    dato = query.data or ""
    log.info("Botón %s | par=%s | %s", dato, ESTADO.simbolo, descripcion_dia())
    try:
        if dato == "menu":
            cancelar_manual()
            await editar(query, texto_menu(), teclado_menu())
        elif dato == "manual":
            await mostrar_menu_manual(query)
        elif dato == "pares":
            cancelar_manual()
            await editar(query, f"{descripcion_dia()}\nElige el par que quieres operar:", teclado_pares())
        elif dato.startswith("par:"):
            aviso = await cambiar_simbolo(dato.split(":", 1)[1])
            if aviso:
                await editar(query, aviso, teclado_pares())
            else:
                await editar(query, texto_menu(), teclado_menu())
        elif dato.startswith("n:"):
            n = int(dato.split(":")[1])
            if not 1 <= n <= MAX_OPERACIONES:
                raise ValueError("cantidad de operaciones no válida")
            ESTADO.manual_n = n
            await editar(
                query,
                f"{n} operación(es) en {nombre_simbolo(ESTADO.simbolo)}.\n¿En qué dirección?",
                teclado_direccion(n),
            )
        elif dato.startswith("dir:"):
            modo = dato.split(":")[1]
            etiquetas = {"compra": "todas de COMPRA", "venta": "todas de VENTA", "mixto": "mitad compra y mitad venta"}
            if modo not in etiquetas or ESTADO.manual_n < 1:
                await editar(query, "Empieza otra vez por Modo Manual.", boton_volver())
                return
            ESTADO.manual_modo = modo
            ESTADO.manual_paso = "sl"
            ESTADO.manual_sl = 0.0
            await editar(query, f"{ESTADO.manual_n} operación(es), {etiquetas[modo]}.")
            await query.message.reply_text(
                "Escribe tu stop loss en dolares POR OPERACION, por ejemplo 0.40.\n"
                "No acepto 0.10. Ese stop es el que cerraba todo en perdida."
            )
        elif dato == "ok":
            await ejecutar_manual(query, context.bot)
        elif dato == "auto:on":
            ESTADO.auto_activo = True
            await editar(query, texto_menu(), teclado_menu())
        elif dato == "auto:off":
            ESTADO.auto_activo = False
            await editar(query, texto_menu(), teclado_menu())
        elif dato == "ganancias":
            await editar(query, await texto_ganancias(), boton_volver())
    except Exception as exc:
        log.exception("Error en el botón %s", dato)
        await editar(query, f"Error: {exc}", boton_volver())


@solo_dueno
async def on_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ESTADO.manual_paso not in ("sl", "tp"):
        return
    bruto = (update.message.text or "").replace(",", ".").replace("$", "").strip()
    try:
        valor = round(float(bruto), 2)
    except ValueError:
        await update.message.reply_text("Escribe solo el numero en dolares. Ejemplo: 0.40")
        return
    if ESTADO.manual_paso == "sl":
        if valor < 0.15:
            await update.message.reply_text("Ese stop queda dentro del ruido. Minimo 0.15. En automatico uso 0.40.")
            return
        if valor > 2:
            await update.message.reply_text("Para esta cuenta el stop maximo es 2.00.")
            return
        ESTADO.manual_sl = valor
        ESTADO.manual_paso = "tp"
        await update.message.reply_text(
            f"Stop ${valor:.2f} por operacion. Ahora escribe el take profit en dolares (tambien por operacion).\n"
            f"Relacion 1:3 seria ${valor * RATIO_RR:.2f}."
        )
        return
    if valor <= ESTADO.manual_sl:
        await update.message.reply_text("El take profit tiene que ser mayor que el stop.")
        return
    try:
        lectura = await leer_mercado(estricto=False)
    except Exception as exc:
        await update.message.reply_text(f"No pude leer el mercado: {exc}")
        return
    if lectura is None:
        cancelar_manual()
        await update.message.reply_text(
            f"No pude leer el mercado: {ESTADO.motivo_datos or 'faltan datos'}. Empieza otra vez por Modo Manual."
        )
        return
    planes: list[Plan] = []
    for direccion in direcciones_manuales(ESTADO.manual_modo, ESTADO.manual_n):
        plan, motivo = plan_manual(
            direccion,
            lectura.precio_vivo,
            lectura.atr_stop,
            ESTADO.manual_sl,
            valor,
            ESTADO.multiplicadores,
        )
        if plan is None:
            ESTADO.manual_paso = "sl"
            await update.message.reply_text(f"No entro: {motivo}\nEscribe otro stop.")
            return
        planes.append(plan)
    ESTADO.manual_planes = planes
    ESTADO.manual_paso = "confirmar"
    await update.message.reply_text(resumen_manual(planes), reply_markup=teclado_confirmar())


async def cargar_multiplicadores() -> None:
    await DERIV.listo.wait()
    try:
        lista, ok = await multiplicadores_del_simbolo(ESTADO.simbolo)
        if ok:
            ESTADO.multiplicadores = lista
        log.info(
            "Multiplicadores de %s: %s%s",
            ESTADO.simbolo,
            ESTADO.multiplicadores,
            "" if ok else " (lista por defecto: Deriv no devolvió ninguno)",
        )
    except Exception as exc:
        log.warning("Uso la lista por defecto de multiplicadores: %s", exc)


async def on_init(app: Application) -> None:
    global DERIV
    ESTADO.lock = asyncio.Lock()
    if MODO_DESCUBRIR:
        log.info("MODO DESCUBRIR: hay token pero falta DERIV_ACCOUNT_ID. Busco tus cuentas...")
        ESTADO.tareas.append(asyncio.create_task(descubrir_cuentas()))
        ESTADO.tareas.append(asyncio.create_task(keep_alive(RENDER_URL)))
        return
    DERIV = DerivClient(DERIV_TOKEN, DERIV_URL)
    DERIV.iniciar()
    ESTADO.tareas.append(asyncio.create_task(cargar_multiplicadores()))
    ESTADO.tareas.append(asyncio.create_task(keep_alive(RENDER_URL)))
    ESTADO.tareas.append(asyncio.create_task(bucle_automatico(app.bot)))
    log.info(
        "Credenciales Deriv -> DERIV_TOKEN:%s | DERIV_PAT:%s | DERIV_ACCOUNT_ID:%s | DERIV_APP_ID:%s (%s car.)",
        "si" if DERIV_TOKEN else "no",
        (f"si ({len(DERIV_PAT)} car., empieza con pat_: {'si' if DERIV_PAT.startswith('pat_') else 'NO'})" if DERIV_PAT else "no"),
        "si" if DERIV_ACCOUNT_ID else "no",
        DERIV_APP_ID,
        len(DERIV_APP_ID),
    )
    log.info(
        "Servicio iniciado (API %s). Keep-alive cada %ss hacia %s",
        "NUEVA" if USAR_NUEVA_API else "antigua",
        INTERVALO_KEEPALIVE,
        RENDER_URL,
    )
    log.info("Par inicial: %s. %s", ESTADO.simbolo, descripcion_dia())


async def on_shutdown(app: Application) -> None:
    for tarea in ESTADO.tareas:
        tarea.cancel()
    if DERIV is not None:
        await DERIV.detener()


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        log.warning(
            "Telegram: hay OTRA copia del bot usando este mismo token (Conflict). "
            "Es normal unos segundos al desplegar; si se repite, apaga la otra copia o cambia el token en BotFather."
        )
        return
    if isinstance(err, NetworkError):
        log.warning("Telegram: error de red temporal: %s", err)
        return
    log.error("Error no controlado en Telegram", exc_info=err)


def main() -> None:
    iniciar_servidor_web()
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(on_init)
        .post_shutdown(on_shutdown)
        .build()
    )
    app.add_error_handler(on_error)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ganancias", cmd_ganancias))
    app.add_handler(CallbackQueryHandler(on_boton))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_texto))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
