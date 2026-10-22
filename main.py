"""Fase 5: jornada simulada del Banco Andino Digital (integración).

Ejecuta en orden las cuatro fases con los parámetros de la variante del
Grupo 4 y justifica con tiempos medidos qué modelo de concurrencia corresponde
a cada componente:

  * cajeros (depósitos/retiros)      -> hilos + threading.Lock   (Fase 1)
  * transferencias entre cuentas     -> hilos + orden de candados (Fase 2)
  * auditoría nocturna CPU-bound     -> procesos                  (Fase 3)
  * notificaciones y antifraude      -> asyncio                   (Fase 4)

Uso: python main.py
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

from auditoria import imprimir_tabla, medir_auditorias
from banco import Banco, demo_condicion_carrera, demo_deadlock, transferencias_simultaneas
from cuenta import CuentaBancaria, FondosInsuficientes
from servicios_async import (
    LIMITE_CONEXIONES,
    demo_time_sleep,
    notificaciones_gather,
    notificaciones_secuenciales,
    notificaciones_semaphore,
    procesar_antifraude,
)

PARAMETROS_GRUPO = {
    "grupo": 4,
    "cuentas": 50,
    "hilos_cajero": 120,
    "transferencias": 3_000,
    "cuentas_a_auditar": 250,
    "notificaciones": 25_000,
}
# Con 25.000 notificaciones de 0,2-1 s, la tabla completa tarda minutos;
# se mide una muestra y se extrapola el tiempo total en el informe.
MUESTRA_NOTIFICACIONES = 200
SALDO_INICIAL = 1_000_000
DEPOSITOS_POR_CAJERO = 1_000


def fase1_cajeros(n_hilos: int = PARAMETROS_GRUPO["hilos_cajero"]) -> None:
    """Depósitos concurrentes sobre las mismas cuentas, con y sin Lock."""
    demo_condicion_carrera(n_hilos=n_hilos, depositos_por_hilo=DEPOSITOS_POR_CAJERO, seguro=False)
    demo_condicion_carrera(n_hilos=n_hilos, depositos_por_hilo=DEPOSITOS_POR_CAJERO, seguro=True)


def fase1_cajeros_reales(banco: Banco, n_hilos: int) -> None:
    """Jornada de cajeros: depósitos y retiros aleatorios sobre todas las
    cuentas con Lock. Verifica que ninguna cuenta quede con saldo negativo."""
    cuentas = list(banco.cuentas.values())
    rnd = random.Random(99)
    pares = [rnd.choice(cuentas) for _ in range(n_hilos * DEPOSITOS_POR_CAJERO)]

    def cajero(par: CuentaBancaria) -> None:
        try:
            if random.random() < 0.5:
                par.depositar(random.randint(1_000, 50_000))
            else:
                par.retirar(random.randint(1_000, 50_000))
        except FondosInsuficientes:
            pass  # retiro sin fondos: rechazado por regla de negocio

    inicio = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_hilos) as pool:
        list(pool.map(cajero, pares))
    transcurrido = time.perf_counter() - inicio
    negativas = [c for c in cuentas if c.saldo < 0]
    print(
        f"[Fase 1] jornada de cajeros: {len(pares):,} operaciones con {n_hilos} hilos | "
        f"negativas: {len(negativas)} | {transcurrido:.2f}s"
    )


def fase2_transferencias(banco: Banco) -> None:
    demo_deadlock()  # se reproduce con la versión insegura
    transferencias_simultaneas(
        banco,
        n_transferencias=PARAMETROS_GRUPO["transferencias"],
        max_workers=PARAMETROS_GRUPO["hilos_cajero"],
    )


def fase3_auditoria() -> None:
    # La variante del grupo audita 250 cuentas, pero ese volumen es menor que
    # el arranque de los procesos y enmascara el speedup. Se mide también con
    # 2.000 cuentas (volumen del código base) para comparar con Amdahl.
    print(f"[Fase 3] volumen de la variante ({PARAMETROS_GRUPO['cuentas_a_auditar']} cuentas):")
    imprimir_tabla(medir_auditorias(n_cuentas=PARAMETROS_GRUPO["cuentas_a_auditar"]))
    print("\n[Fase 3] volumen amplificado (2,000 cuentas) para medir el speedup real:")
    imprimir_tabla(medir_auditorias(n_cuentas=2_000))


def fase4_servicios() -> None:
    n = MUESTRA_NOTIFICACIONES
    print(f"[Fase 4] notificaciones (muestra de {n} de {PARAMETROS_GRUPO['notificaciones']:,}):")
    print(f"{'versión':<20} {'segundos':>10} {'speedup':>9}")
    t_sec = asyncio.run(notificaciones_secuenciales(n))
    print(f"{'secuencial':<20} {t_sec:>10.2f} {'1.0x':>9}")
    t_gather = asyncio.run(notificaciones_gather(n))
    print(f"{'gather':<20} {t_gather:>10.2f} {t_sec / t_gather:>8.1f}x")
    t_sem = asyncio.run(notificaciones_semaphore(n, LIMITE_CONEXIONES))
    print(
        f"{'semaphore(' + str(LIMITE_CONEXIONES) + ')':<20} {t_sem:>10.2f} "
        f"{t_sec / t_sem:>8.1f}x"
    )
    extrapolado = t_sem * PARAMETROS_GRUPO["notificaciones"] / n
    print(
        f"[Fase 4] extrapolado a {PARAMETROS_GRUPO['notificaciones']:,} "
        f"notificaciones con semaphore: {extrapolado / 60:.1f} min"
    )
    asyncio.run(demo_time_sleep())
    asyncio.run(procesar_antifraude(1_000))


def main() -> None:
    p = PARAMETROS_GRUPO
    nucleos = os.cpu_count() or 1
    print("=" * 72)
    print(f"Banco Andino Digital - Grupo {p['grupo']} - Python {os.sys.version.split()[0]}")
    print(
        f"cuentas={p['cuentas']} | hilos_cajero={p['hilos_cajero']} | "
        f"transferencias={p['transferencias']:,} | auditar={p['cuentas_a_auditar']} | "
        f"notificaciones={p['notificaciones']:,} | núcleos={nucleos}"
    )
    print("=" * 72)

    inicio_total = time.perf_counter()

    print("\n--- Fase 1: cajeros simultáneos (Lock) ---")
    fase1_cajeros()

    print("\n--- Fase 2: transferencias sin deadlock ---")
    banco = Banco()
    for i in range(p["cuentas"]):
        banco.abrir_cuenta(f"Cliente {i + 1}", SALDO_INICIAL)
    fase1_cajeros_reales(banco, p["hilos_cajero"])
    fase2_transferencias(banco)

    print("\n--- Fase 3: auditoría nocturna (hilos vs procesos) ---")
    fase3_auditoria()

    print("\n--- Fase 4: notificaciones y antifraude (asyncio) ---")
    fase4_servicios()

    print("\n" + "=" * 72)
    print(f"Jornada completa: {time.perf_counter() - inicio_total:.2f}s")


if __name__ == "__main__":
    main()
