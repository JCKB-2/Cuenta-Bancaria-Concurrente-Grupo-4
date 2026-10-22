"""Fase 3: auditoría nocturna paralela (CPU-bound) del Banco Andino Digital.

auditar_cuenta() recalcula el saldo y calcula un hash SHA-256 iterado 2.000
veces por cuenta. Al ser CPU-bound, solo multiprocessing (ProcessPoolExecutor)
puede acelerarlo: el GIL impide que los hilos exploren varios núcleos.
"""

from __future__ import annotations

import hashlib
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

ITERACIONES_HASH = 2_000


def generar_historiales(
    n_cuentas: int, movimientos: int = 40, semilla: int = 42
) -> list[list[tuple[str, int]]]:
    """Genera historiales sintéticos serializables: lista (por cuenta) de
    tuplas (tipo, monto)."""
    rnd = random.Random(semilla)
    historiales = []
    for _ in range(n_cuentas):
        historial = [("DEPOSITO", rnd.randint(10_000, 500_000))]
        for _ in range(movimientos - 1):
            historial.append(
                (rnd.choice(["DEPOSITO", "RETIRO"]), rnd.randint(1_000, 50_000))
            )
        historiales.append(historial)
    return historiales

def auditar_cuenta(
    historial: list[tuple[str, int]], iteraciones: int = ITERACIONES_HASH
) -> tuple[int, str]:
    """Recalcula el saldo y genera un hash de verificación (trabajo CPU-bound).
    A nivel de módulo para poder enviarse a los procesos (serializable)."""
    saldo = 0
    for tipo, monto in historial:
        saldo += monto if tipo == "DEPOSITO" else -monto
    digest = repr(historial).encode()
    for _ in range(iteraciones):
        digest = hashlib.sha256(digest).digest()
    return saldo, digest.hex()[:16]


def auditoria_secuencial(historiales: list[list[tuple[str, int]]]) -> list[tuple[int, str]]:
    return [auditar_cuenta(h) for h in historiales]


def auditoria_con_hilos(
    historiales: list[list[tuple[str, int]]], workers: int
) -> list[tuple[int, str]]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(auditar_cuenta, historiales))


def auditoria_con_procesos(
    historiales: list[list[tuple[str, int]]], workers: int
) -> list[tuple[int, str]]:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(auditar_cuenta, historiales))


def medir_auditorias(
    n_cuentas: int = 250, workers_list: tuple[int, ...] | None = None
) -> list[dict[str, float | int | str]]:
    """Compara secuencial, hilos y procesos; devuelve las filas de la tabla
    de tiempos con el speedup frente a la versión secuencial."""
    nucleos = os.cpu_count() or 1
    if workers_list is None:
        workers_list = tuple(
            dict.fromkeys([1, 2, 4, nucleos])  # 1, 2, 4 y N, sin duplicados
        )

    historiales = generar_historiales(n_cuentas)
    filas: list[dict[str, float | int | str]] = []

    t0 = time.perf_counter()
    base = auditoria_secuencial(historiales)
    t_sec = time.perf_counter() - t0
    filas.append(
        {"version": "secuencial", "workers": 1, "segundos": t_sec, "speedup": 1.0}
    )
    print(f"[Fase 3] secuencial ({n_cuentas:,} cuentas): {t_sec:.2f}s")

    for workers in workers_list:
        t0 = time.perf_counter()
        resultado = auditoria_con_hilos(historiales, workers)
        t_hilos = time.perf_counter() - t0
        assert resultado == base, "la auditoría con hilos no coincide con la secuencial"
        filas.append(
            {
                "version": "hilos",
                "workers": workers,
                "segundos": t_hilos,
                "speedup": t_sec / t_hilos,
            }
        )

        t0 = time.perf_counter()
        resultado = auditoria_con_procesos(historiales, workers)
        t_procesos = time.perf_counter() - t0
        assert resultado == base, "la auditoría con procesos no coincide con la secuencial"
        filas.append(
            {
                "version": "procesos",
                "workers": workers,
                "segundos": t_procesos,
                "speedup": t_sec / t_procesos,
            }
        )
    return filas


def imprimir_tabla(filas: list[dict[str, float | int | str]]) -> None:
    print(f"{'versión':<12} {'workers':>7} {'segundos':>10} {'speedup':>9}")
    for fila in filas:
        print(
            f"{fila['version']:<12} {fila['workers']:>7} "
            f"{fila['segundos']:>10.2f} {fila['speedup']:>8.2f}x"
        )


def ley_amdahl(speedup_maximo: float, fraccion_paralelizable: float) -> float:
    """Tiempos de Amdahl: 1 / (s + (1-s)/p) con s = 1 - fraccion_paralelizable."""
    return 1.0 / ((1 - fraccion_paralelizable) + fraccion_paralelizable / speedup_maximo)


if __name__ == "__main__":
    filas = medir_auditorias(n_cuentas=250)
    imprimir_tabla(filas)

