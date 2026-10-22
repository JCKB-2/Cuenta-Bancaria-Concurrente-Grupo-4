"""Fase 4: servicios externos simulados con asyncio (I/O-bound).

Las corrutinas enviar_notificacion() y consultar_antifraude() esperan entre
0,2 y 1 segundo de red simulada. Tres formas de procesarlas:
  1. await secuencial (lento, un servicio tras otro),
  2. asyncio.gather (todas a la vez),
  3. gather limitado con asyncio.Semaphore(20) (no saturar el servicio).
"""

from __future__ import annotations

import asyncio
import random
import time

LATENCIA_MIN = 0.2
LATENCIA_MAX = 1.0
LIMITE_CONEXIONES = 20
PROBABILIDAD_FALLA_ANTIFRAUDE = 0.05
LIMITE_MONTO_APROBADO = 5_000_000


class ErrorAntifraude(Exception):
    """El servicio antifraude no respondió (5 % de las llamadas)."""


async def enviar_notificacion(cuenta: int, mensaje: str) -> str:
    await asyncio.sleep(random.uniform(LATENCIA_MIN, LATENCIA_MAX))
    return f"OK cuenta {cuenta}: {mensaje}"


async def consultar_antifraude(cuenta: int, monto: int) -> bool:
    await asyncio.sleep(random.uniform(LATENCIA_MIN, LATENCIA_MAX))
    if random.random() < PROBABILIDAD_FALLA_ANTIFRAUDE:
        raise ErrorAntifraude(f"Servicio antifraude no disponible (cuenta {cuenta})")
    return monto < LIMITE_MONTO_APROBADO


# ---------------------------------------------------------------------------
# Notificaciones: secuencial vs gather vs Semaphore
# ---------------------------------------------------------------------------
async def notificaciones_secuenciales(n: int) -> float:
    inicio = time.perf_counter()
    for i in range(n):
        await enviar_notificacion(i, "Movimiento registrado")
    return time.perf_counter() - inicio


async def notificaciones_gather(n: int) -> float:
    inicio = time.perf_counter()
    await asyncio.gather(*(enviar_notificacion(i, "Movimiento registrado") for i in range(n)))
    return time.perf_counter() - inicio


async def notificaciones_semaphore(n: int, limite: int = LIMITE_CONEXIONES) -> float:
    sem = asyncio.Semaphore(limite)

    async def una(i: int) -> str:
        async with sem:
            return await enviar_notificacion(i, "Movimiento registrado")

    inicio = time.perf_counter()
    await asyncio.gather(*(una(i) for i in range(n)))
    return time.perf_counter() - inicio


# ---------------------------------------------------------------------------
# time.sleep dentro de una corrutina: bloquea el event loop
# ---------------------------------------------------------------------------
def trabajo_pesado_en_cuenta(monto: int) -> int:
    """Cálculo CPU-bound/legacy que no puede esperar con asyncio.sleep."""
    time.sleep(0.05)  # bloquea el hilo del event loop
    return monto * 2


async def bloqueado_por_time_sleep(n: int) -> float:
    """Mal ejemplo: time.sleep dentro de la corrutina bloquea el event loop,
    así que gather no acelera nada (todo se serializa)."""
    inicio = time.perf_counter()
    await asyncio.gather(*(lenta_con_time_sleep() for _ in range(n)))
    return time.perf_counter() - inicio


async def lenta_con_time_sleep() -> int:
    await asyncio.sleep(0)  # cede una vez para que el event loop arranque
    return trabajo_pesado_en_cuenta(1)  # time.sleep real: serializa todo


async def corregido_con_to_thread(n: int) -> float:
    """Corrección: la función bloqueante se mueve a un hilo del pool con
    asyncio.to_thread, sin congelar el event loop."""
    inicio = time.perf_counter()
    await asyncio.gather(
        *(asyncio.to_thread(trabajo_pesado_en_cuenta, 1) for _ in range(n))
    )
    return time.perf_counter() - inicio


async def demo_time_sleep(n: int = 20) -> None:
    t_bloqueado = await bloqueado_por_time_sleep(n)
    t_corregido = await corregido_con_to_thread(n)
    print(
        f"[Fase 4] time.sleep dentro de corrutina: {t_bloqueado:.2f}s "
        f"| corregido con asyncio.to_thread: {t_corregido:.2f}s "
        f"(mejora {t_bloqueado / t_corregido:.1f}x con {n} tareas)"
    )


# ---------------------------------------------------------------------------
# Antifraude con manejo de errores
# ---------------------------------------------------------------------------
async def procesar_antifraude(n: int) -> tuple[int, int, int, float]:
    """Procesa n consultas antifraude; un fallo marca la transacción como
    "pendiente de revisión" sin detener las demás (return_exceptions=True).

    Devuelve (aprobadas, rechazadas, pendientes_revisión, segundos).
    """
    inicio = time.perf_counter()
    montos = [random.randint(1_000, 20_000_000) for _ in range(n)]
    resultados = await asyncio.gather(
        *(consultar_antifraude(i, monto) for i, monto in enumerate(montos)),
        return_exceptions=True,
    )
    aprobadas = rechazadas = pendientes = 0
    for r in resultados:
        if isinstance(r, ErrorAntifraude):
            pendientes += 1
        elif r:
            aprobadas += 1
        else:
            rechazadas += 1
    transcurrido = time.perf_counter() - inicio
    print(
        f"[Fase 4] antifraude de {n:,} consultas: {aprobadas:,} aprobadas | "
        f"{rechazadas:,} rechazadas | {pendientes:,} pendientes de revisión | "
        f"{transcurrido:.2f}s"
    )
    return aprobadas, rechazadas, pendientes, transcurrido


def tabla_tiempos(n: int = 200) -> list[dict[str, float | int]]:
    """Compara las tres formas de enviar notificaciones con el mismo n."""
    random.seed(7)
    filas = []
    t_sec = asyncio.run(notificaciones_secuenciales(n))
    filas.append({"version": "secuencial", "segundos": t_sec, "speedup": 1.0})
    random.seed(7)
    t_gather = asyncio.run(notificaciones_gather(n))
    filas.append({"version": "gather", "segundos": t_gather, "speedup": t_sec / t_gather})
    random.seed(7)
    t_sem = asyncio.run(notificaciones_semaphore(n))
    filas.append(
        {"version": f"semaphore({LIMITE_CONEXIONES})", "segundos": t_sem, "speedup": t_sec / t_sem}
    )
    return filas


if __name__ == "__main__":
    N_MUESTRA = 200  # el volumen completo de la variante (25.000) tarda minutos
    print(f"[Fase 4] tabla de tiempos con {N_MUESTRA} notificaciones:")
    print(f"{'versión':<18} {'segundos':>10} {'speedup':>9}")
    for fila in tabla_tiempos(N_MUESTRA):
        print(f"{fila['version']:<18} {fila['segundos']:>10.2f} {fila['speedup']:>8.1f}x")
    asyncio.run(demo_time_sleep())
    asyncio.run(procesar_antifraude(1_000))
