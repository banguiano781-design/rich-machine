"""fundamentos.py: herramientas estadísticas que deciden si una estrategia tiene ventaja real frente al azar. No depende de Deriv ni de Telegram."""
from __future__ import annotations

import math

import numpy as np


def intervalo_wilson(exitos: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 1.0
    p = exitos / n
    denominador = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denominador
    margen = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominador
    return max(0.0, centro - margen), min(1.0, centro + margen)


def esperanza(p: float, ganancia: float, perdida: float) -> float:
    return p * ganancia - (1 - p) * perdida


def kelly(p: float, ganancia: float, perdida: float) -> float:
    b = ganancia / perdida
    return max(0.0, (p * (b + 1) - 1) / b)


def paseo_aleatorio_teorico(riesgo_bruto: float, beneficio_bruto: float) -> float:
    return riesgo_bruto / (riesgo_bruto + beneficio_bruto)


def paseo_aleatorio_gbm(sigma_min: float, r_sl: float, r_tp: float, minutos: int = 60,
                        subpasos: int = 50, trayectorias: int = 1000, semilla: int = 7) -> float:
    rng = np.random.default_rng(semilla)
    sigma = sigma_min / math.sqrt(subpasos)
    pasos = minutos * subpasos
    incrementos = rng.normal(-0.5 * sigma ** 2, sigma, size=(trayectorias, pasos))
    logprecio = np.cumsum(incrementos, axis=1)
    arriba = logprecio >= math.log1p(r_tp)
    abajo = logprecio <= math.log1p(-r_sl)
    tocó_arriba = arriba.any(axis=1)
    tocó_abajo = abajo.any(axis=1)
    primero_arriba = np.where(tocó_arriba, arriba.argmax(axis=1), pasos)
    primero_abajo = np.where(tocó_abajo, abajo.argmax(axis=1), pasos)
    decididas = tocó_arriba | tocó_abajo
    if not decididas.any():
        return float("nan")
    return float((primero_arriba < primero_abajo)[decididas].mean())


def riesgo_de_ruina(p: float, ganancia: float, perdida: float, capital: float,
                    operaciones: int = 100, simulaciones: int = 4000, semilla: int = 7) -> dict[str, float]:
    rng = np.random.default_rng(semilla)
    gana = rng.random((simulaciones, operaciones)) < p
    flujo = np.where(gana, ganancia, -perdida)
    curva = capital + np.cumsum(flujo, axis=1)
    return {
        "quiebra": float((curva <= 0).any(axis=1).mean()),
        "pierde_mitad": float((curva <= capital / 2).any(axis=1).mean()),
    }


def independencia(cierres: np.ndarray) -> dict[str, float]:
    subidas = np.diff(cierres) > 0
    previa = subidas[:-1]
    actual = subidas[1:]
    n_s = int(previa.sum())
    n_b = int((~previa).sum())
    p = float(actual.mean())
    p_s = float(actual[previa].mean()) if n_s else float("nan")
    p_b = float(actual[~previa].mean()) if n_b else float("nan")
    error = math.sqrt(p * (1 - p) * (1 / max(n_s, 1) + 1 / max(n_b, 1)))
    z = (p_s - p_b) / error if error > 0 else 0.0
    return {"n": float(len(actual)), "p_sube": p,
            "p_sube_tras_sube": p_s, "p_sube_tras_baja": p_b, "z": z}


def evaluar(resultados: list[str], riesgo: float, beneficio: float, minimo: int) -> dict:
    ganadas = resultados.count("ganada")
    perdidas = resultados.count("perdida")
    expiradas = resultados.count("expirada")
    decididas = ganadas + perdidas
    p = ganadas / decididas if decididas else 0.0
    p_bajo, p_alto = intervalo_wilson(ganadas, decididas)
    ev_bajo = esperanza(p_bajo, beneficio, riesgo)
    if decididas < minimo:
        motivo = f"muestra insuficiente ({decididas} de {minimo} señales)"
        apto = False
    elif ev_bajo <= 0:
        motivo = "la cota inferior del acierto no supera el equilibrio"
        apto = False
    else:
        motivo = "ventaja demostrada con 95% de confianza"
        apto = True
    return {
        "n": len(resultados), "ganadas": ganadas, "perdidas": perdidas,
        "expiradas": expiradas, "decididas": decididas,
        "p": p, "p_bajo": p_bajo, "p_alto": p_alto,
        "ev": esperanza(p, beneficio, riesgo), "ev_bajo": ev_bajo,
        "kelly": kelly(p_bajo, beneficio, riesgo),
        "apto": apto, "motivo": motivo,
    }
