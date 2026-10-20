"""
bot_hibrido.py: bot híbrido (Modo Manual + Modo Automático) para Deriv,
controlado desde Telegram y desplegado como Web Service gratuito en Render.
"""
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
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot_hibrido")

# ═════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════════
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0").strip()) if os.environ.get("TELEGRAM_CHAT_ID", "").strip() else 0
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "").strip()

if not TELEGRAM_TOKEN or not CHAT_ID or not DERIV_TOKEN:
    raise SystemExit("Falta alguna de las variables de entorno principales en Render.")

SYMBOL = os.environ.get("DERIV_SYMBOL", "R_100").strip()
MODO_REAL = os.environ.get("MODO_REAL", "false").strip().lower() == "true"
MULTIPLICADORES = sorted({int(x) for x in os.environ.get("MULTIPLICADORES", "10,20,30,40,50,100,200").split(",") if x.strip()})
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://onrender.com")
PORT = int(os.environ.get("PORT", "5000"))

# Gestión de riesgo (fija, en dólares)
STAKE = 0.50              
RIESGO_MAX_USD = 0.05     
BENEFICIO_MIN_USD = 0.15  
RATIO_RR = 3.0            
SL_ATR_MULT = 1.5         

# Señal
ZLEN = 20
ATR_LEN = 14
EMA_MACRO = 200
VOL_LEN = 20
VOL_MULT = 2.5
Z_UMBRAL = 2.0            

# Datos y temporizadores
GRANULARIDAD = 60         
VELAS = 300
TICKS = 5000              
INTERVALO_KEEPALIVE = 600 
INTERVALO_AUTO = 20       

# Corrección de la variable de la APP ID
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089").strip()
DERIV_URL = f"wss://://derivws.com{DERIV_APP_ID}"

# ═════════════════════════════════════════════════════════════════
#  1. KEEP-ALIVE: servidor Flask en hilo secundario + autoping
# ═════════════════════════════════════════════════════════════════
flask_app = Flask(__name__)

@flask_app.route("/")
def inicio():
    return "Bot híbrido activo", 200

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

# ═════════════════════════════════════════════════════════════════
#  2. CLIENTE DERIV (WebSocket con reconexión automática)
# ═════════════════════════════════════════════════════════════════
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

    async def _mantener(self) -> None:
        espera = 2
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
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
                        if fut and not fut.done():
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

# ═════════════════════════════════════════════════════════════════
#  3. DATOS E INDICADORES
# ═════════════════════════════════════════════════════════════════
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
    c = df["close"]
    df["ema_z"] = c.ewm(span=ZLEN, adjust=False).mean()
    df["desv"] = c.rolling(ZLEN).std(ddof=0)
    df["z"] = (c - df["ema_z"]) / df["desv"].replace(0, np.nan)
    prev = c.shift(1)
    tr = pd.concat([df["high"] - df["low"],
                    (df["high"] - prev).abs(),
                    (df["low"] - prev).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / ATR_LEN, adjust=False).mean()
    df["ema_macro"] = c.ewm(span=EMA_MACRO, adjust=False).mean()
    df["vol_media"] = df["vol_proxy"].rolling(VOL_LEN).mean()
    df["vol_inst"] = df["vol_proxy"] > df["vol_media"] * VOL_MULT
    return df

async def obtener_pip() -> tuple[float, int]:
    r = await DERIV.request({"active_symbols": "brief"})
    for s in r.get("active_symbols", []):
        if s.get("symbol") == SYMBOL:
            pip = float(s.get("pip", 0.00001))
            return pip, max(0, int(round(-math.log10(pip))))
    return 0.00001, 5

def redondear(precio: float) -> float:
    return round(round(precio / PIP) * PIP, DECIMALES)

async def leer_mercado() -> Lectura | None:
    global PIP, DECIMALES
    if PIP is None:
        PIP, DECIMALES = await obtener_pip()

    velas_r, ticks_r = await asyncio.gather(
        DERIV.request({"ticks_history": SYMBOL, "style": "candles", "granularity": GRANULARIDAD, "count": VELAS, "end": "latest"}),
        DERIV.request({"ticks_history": SYMBOL, "style": "ticks", "count": TICKS, "end": "latest"}),
    )
    velas = pd.DataFrame(velas_r["candles"])
    if len(velas) < EMA_MACRO + 5:
        return None
    
    for col in ("open", "high", "low", "close"):
        velas[col] = velas[col].astype(float)

    precios = [float(p) for p in ticks_r["history"]["prices"]]
    tiempos = [int(t) for t in ticks_r["history"]["times"]]
    proxy = proxy_volumen(precios, tiempos)
    velas["vol_proxy"] = velas["epoch"].map(proxy).fillna(0.0).astype(float)

    ventana = velas["epoch"].iloc[-(VOL_LEN + 1):-1]
    if not ventana.isin(proxy.index).all():
        return None

    ind = calcular_indicadores(velas)
    c = ind.iloc[-2]
    if pd.isna(c["z"]) or pd.isna(c["atr"]):
        return None
    return Lectura(epoch=int(c["epoch"]), cierre=float(c["close"]), precio_vivo=precios[-1], z=float(c["z"]), atr=float(c["atr"]), ema_macro=float(c["ema_macro"]), vol_inst=bool(c["vol_inst"]))

# ═════════════════════════════════════════════════════════════════
#  4. PLAN DE OPERACIÓN CON FILTRO DE RIESGO
# ═════════════════════════════════════════════════════════════════
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
        return None, "ATR no valido"
    dist_min = SL_ATR_MULT * atr
    cota = RIESGO_MAX_USD * entrada / (STAKE * dist_min)
    candidatos = [m for m in MULTIPLICADORES if m <= cota]
    if not candidatos:
return Ninguno, f"Riesgo excedido" 
m = max(candidatos) 
dist_sl = RIESGO_MAX_USD * entrada / (STAKE * m) 
dist_tp = RATIO_RR * dist_sl 
riesgo = dist_sl * STAKE * m / entrada 
beneficio = dist_tp * STAKE * m / entrada 
if riesgo > RIESGO_MAX_USD + 1e-9 o beneficio < BENEFICIO_MIN_USD - 1e-9: 
return Ninguno, "Fallo en filtro de riesgo" 
plan = Plan(direccion=direccion, multiplicador=m, entrada=entrada, sl=redondear(entrada - direccion * dist_sl), tp=redondear(entrada + direccion * dist_tp), riesgo_usd=riesgo, beneficio_usd=beneficio) 
return plan, ""
def fmt(precio: float) -> str: 
return f"{precio:.7f}".rstrip("0").rstrip(".")
def texto_alerta(plan: Plan) -> str: 
tendencia = "alcista" if plan.direccion > 0 else "bajista" 
operacion = "compra" if plan.direccion > 0 else "venta" 
return f"la tendencia es '{tendencia}' cuanto quieres ganar {fmt(plan.tp)} y cuanto es el stop lose {fmt(plan.sl)}; entre en {operacion}"

# ═══════════════════════════════════════════════════════════════
#  5. EJECUCIÓN Y SEGUIMIENTO
# ═══════════════════════════════════════════════════════════════

class Estado: 
def init (self) -> None: 
self.auto_activo = False 
self.ultima_vela: int | None = None 
self.lock: asyncio.Lock | None = None 
self.tareas: list[asyncio.Task] = [] 
self.vigilancias: set[asyncio.Task] = set()
ESTADO = Estado()
def verificar_modo() -> Ninguno: 
es_demo = bool(DERIV.info.get("is_virtual")) 
if (not es_demo) != MODO_REAL: 
rise RuntimeError("El token no coincide con MODO_REAL. Con MODO_REAL=false solo se opera en demo.")
async def abrir_operacion(plan: Plan, bot) -> str: 
async with ESTADO.lock: 
verificar_modo() 
abiertos = await DERIV.request({"portfolio": 1}) 
if abierto["portfolio"].get("contracts"): 
rise RuntimeError("ya hay una operacion abierta; espero a que cierre")
tipo = "MULTUP" if plan.direccion > 0 else "MULTDOWN" 
prop = (await DERIV.request({ 
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
compra = (await DERIV.request({"buy": prop["id"], "price": prop["ask_price"]}))["buy"] 
contrato = str(compra["contract_id"])
await bot.send_message(chat_id=CHAT_ID, text=texto_alerta(plan)) 
tarea = asyncio.create_task(vigilar_contrato(contrato, bot)) 
ESTADO.vigilancias.add(tarea) 
tarea.add_done_callback(ESTADO.vigilancias.discard) 
return contrato
async def vigilar_contrato(contrato: str, bot) -> Ninguno: 
while True: 
await asyncio.sleep(5) 
try: 
r = await DERIV.request({"proposal_open_contract": 1, "contract_id": contrato}) 
excepto Excepción como e: 
log.warning("No pude consultar el contrato %s: %s", contrato, e) 
continue 
c = r["proposal_open_contract"] 
if c.get("is_sold"): 
resultado = float(c.get("profit", 0.0)) 
try: 
await bot.send_message(chat_id=CHAT_ID, text=f"Contrato {contrato} cerrado. Resultado: ${resultado:+.2f}") 
excepto excepción como e: 
log.warning("No pude notificar el cierre: %s", e) 
return
async def resumen_resultados(desde: int | None) -> tuple[int, float]: 
payload = {"profit_table": 1, "description": 1, "sort": "DESC", "limit": 100} 
if desde is not None: 
payload["date_from"] = desde 
r = await DERIV.request(payload) 
txs = r["profit_table"].get("transactions", []) 
ganancia = sum(float(t.get("sell_price", 0)) - float(t.get("buy_price", 0)) for t in txs) 
return len(txs),ganancia
async def texto_ganancias() -> str: 
ahora = datetime.now(timezone.utc) 
inicio_dia = int(ahora.replace(hora=0, minuto=0, segundo=0, microsegundo=0).timestamp()) 
n_hoy, g_hoy = await resumen_resultados(inicio_dia) 
n_tot, g_tot = await resumen_resultados(None) 
saldo = (await DERIV.request({"balance": 1}))["balance"] 
return f"Ganancias (cuenta Deriv)\nHoy: ${g_hoy:+.2f} en {n_hoy} operaciones\nUltimas {n_tot} operaciones: ${g_tot:+.2f}\nSaldo actual: ${float(saldo['balance']):.2f} {saldo.get('currency', 'DÓLAR ESTADOUNIDENSE')}"

# ═══════════════════════════════════════════════════════════════
#  6. MODO AUTOMÁTICO
# ═══════════════════════════════════════════════════════════════

async def ciclo_automatico(bot) -> Ninguno: 
lectura = await leer_mercado() 
si lectura es Ninguno o lectura.epoch == ESTADO.ultima_vela: 
regresa 
ESTADO.ultima_vela = lectura.epoch 
si no lectura.vol_inst: 
regresa
if lectura.z >= Z_UMBRAL y lectura.cierre > lectura.ema_macro: 
direccion = 1 
elif lectura.z <= -Z_UMBRAL y lectura.cierre < lectura.ema_macro: 
direccion = -1 
else: 
return
plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr) 
si el plan es Ninguno: 
log.info("Senal automatica descartada: %s", motivo) 
return 
try: 
await abrir_operacion(plan, bot) 
except Exception as e: 
log.warning("Orden automatica no enviada: %s", e)
async def bucle_automatico(bot) -> Ninguno: 
while True: 
if ESTADO.auto_activo: 
try: 
await ciclo_automatico(bot) 
excepto Excepción: 
log.exception("Error en el ciclo automático") 
await asyncio.sleep(INTERVALO_AUTO)

# ═══════════════════════════════════════════════════════════════
# 7. TELEGRAM: menú, modo manual y comandos
# ═══════════════════════════════════════════════════════════════

def solo_dueno(func): 
@functools.wraps(func) 
async def envoltorio(update: Update, context: ContextTypes.DEFAULT_TYPE): 
chat = update.effective_chat 
if chat is None or chat.id != CHAT_ID: 
return 
return await func(update, context) 
return envoltorio
def texto_menu() -> str: 
cuenta = "sin conexión" 
if DERIV no es Ninguno y DERIV.info: 
tipo = "DEMO" if DERIV.info.get("is_virtual") else "REAL" 
cuenta = f"{DERIV.info.get('loginid')} ({tipo})" 
auto = "ACTIVO" if ESTADO.auto_activo else "apagado" 
return f"Bot hibrido · {SYMBOL}\nCuenta: {cuenta}\nModo automático: {auto}\nStake ${STAKE:.2f} · riesgo ${RIESGO_MAX_USD:.2f} · beneficio ${BENEFICIO_MIN_USD:.2f}"
def teclado_menu() -> InlineKeyboardMarkup: 
if ESTADO.auto_activo: 
boton_auto = InlineKeyboardButton("Detener Modo Automatico", callback_data="auto:off") 
else: 
boton_auto = InlineKeyboardButton("Modo Automatico", callback_data="auto:on") 
return InlineKeyboardMarkup([ 
[InlineKeyboardButton("Modo Manual", callback_data="manual")], 
[boton_auto], 
[InlineKeyboardButton("Ganancias", callback_data="ganancias")], 
])
def boton_volver() -> InlineKeyboardMarkup: 
return InlineKeyboardMarkup([[InlineKeyboardButton("Menú", callback_data="menú")]])
async def editar(q, texto: str, markup=None) -> None: 
try:
await q.edit_message_text(texto, reply_markup=markup) 
except BadRequest as e: 
if "not modified" not in str(e).lower(): 
raise
def texto_lectura(l: Lectura, planes: dict, motivos: dict) -> str: 
hora = datetime.fromtimestamp(l.epoch, timezone.utc).strftime("%H:%M UTC") 
tendencia = "alcista" if l.cierre > l.ema_macro else "bajista" 
lineas = [ 
f"{SYMBOL} · vela cerrada {hora}", 
f"Precio: {fmt(redondear(l.precio_vivo))}", 
f"Z-Score (20): {lz:+.2f}", 
f"ATR (14): {l.atr:.5f}", 
f"Tendencia (EMA 200): {tendencia}", 
f"Volumen institucional (proxy): {'si' if l.vol_inst else 'no'}", 
] 
for d, nombre in ((1, "COMPRA"), (-1, "VENTA")): 
si d en planos: 
p = planos[d] 
lineas.append(f"{nombre}: SL {fmt(p.sl)} · TP {fmt(p.tp)} · x{p.multiplicador} · riesgo ${p.riesgo_usd:.2f} · beneficio ${p.beneficio_usd:.2f}") 
else: 
lineas.append(f"{nombre}: descartada ({motivos[d]})") 
return "\n".join(lineas)
async def mostrar_menu_manual(q) -> Ninguno: 
await editar(q, "Analizando el mercado...") 
lectura = await leer_mercado() 
si lectura es Ninguno: 
await editar(q, "Datos insuficientes por ahora (faltan velas o ticks).", boton_volver()) 
return
planos, motivos = {}, {} 
for d in (1, -1): 
plan, motivo = planificar(d, lectura.precio_vivo, lectura.atr) 
if plan: 
planos[d] = plan 
else: 
motivos[d] = motivo
botones = [] 
if 1 en planos: 
botones.append(InlineKeyboardButton(f"Comprar ${STAKE:.2f}", callback_data="ejecutar:1")) 
if -1 en planos: 
botones.append(InlineKeyboardButton(f"Vender ${STAKE:.2f}", callback_data="ejecutar:-1")) 
filas = [botones] if botones else [] 
filas.append([InlineKeyboardButton("Menu", callback_data="menu")]) 
await editar(q, texto_lectura(lectura, planos, motivos), InlineKeyboardMarkup(filas))
async def ejecutar_manual(q, direccion: int, bot) -> None: 
await editar(q, "Recalculando la señal y enviando la orden...") 
lectura = await leer_mercado() 
if lectura is None: 
rise RuntimeError("datos insuficientes para recalcular") 
plan, motivo = planificar(direccion, lectura.precio_vivo, lectura.atr) 
if plan is None: 
await editar(q, f"Senal descartada al confirmar: {motivo}", boton_volver()) 
return 
contrato = await abrir_operacion(plan, bot) 
await editar(q, f"Orden enviada (contrato {contrato}).", boton_volver())
@solo_dueno 
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE): 
await update.message.reply_text(texto_menu(), reply_markup=teclado_menu())
@solo_dueno 
async def cmd_ganancias(update: Update, context: ContextTypes.DEFAULT_TYPE): 
try: 
texto = await texto_ganancias() 
except Exception as e: 
log.exception("Error en /ganancias") 
texto = f"No pude consultar las ganancias: {e}" 
await update.message.reply_text(texto)
@solo_dueno 
async def on_boton(update: Update, context: ContextTypes.DEFAULT_TYPE): 
q = update.callback_query 
await q.answer() 
dato = q.data 
try: 
if dato == "menu": 
await editar(q, texto_menu(), teclado_menu()) 
elif dato == "manual":
 await mostrar_menu_manual(q)
 elif dato.startswith("ejecutar:"): 
await ejecutar_manual(q, int(dato.split(":")), context.bot) 
elif dato == "auto:on": 
ESTADO.auto_activo = True 
await editar(q, texto_menu(), teclado_menu()) 
elif dato == "auto:off": 
ESTADO.auto_activo = False 
await editar(q, texto_menu(), teclado_menu()) 
elif dato == "ganancias": 
await editar(q, await texto_ganancias(), boton_volver()) 
excepto Excepción como e: 
log.exception("Error en el botón %s", dato) 
await editar(q, f"Error: {e}", boton_volver())

# ═══════════════════════════════════════════════════════════════
#  8. ARRANQUE Y APAGADO
# ═══════════════════════════════════════════════════════════════

async def on_init(app: Application) -> None: 
global DERIV 
ESTADO.lock = asyncio.Lock() 
DERIV = DerivClient(DERIV_TOKEN, DERIV_URL) 
DERIV.iniciar() 
ESTADO.tareas.append(asyncio.create_task(keep_alive(RENDER_URL))) 
ESTADO.tareas.append(asyncio.create_task(bucle_automatico(app.bot))) 
log.info("Servicio iniciado. Keep-alive cada %ds hacia %s", INTERVALO_KEEPALIVE, RENDER_URL)
async def on_shutdown(app: Application) -> None: 
for t in ESTADO.tareas: 
t.cancel() 
if DERIV is not None: 
await DERIV.detener()
def main() -> Ninguno:
 iniciar_servidor_web() 
app = (Application.builder() 
.token(TELEGRAM_TOKEN) 
.post_init(on_init) 
.post_shutdown(on_shutdown) 
.build()) 
app.add_handler(CommandHandler("start", cmd_start)) 
app.add_handler(CommandHandler("ganancias", cmd_ganancias)) 
app.add_handler(CallbackQueryHandler(on_boton)) 
app.run_polling(drop_pending_updates=True)
Si nombre == " main ": 
main()
