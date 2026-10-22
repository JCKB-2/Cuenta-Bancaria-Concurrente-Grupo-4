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


