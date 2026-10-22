"""Banco Andino Digital: modelo y demos de las Fases 1 y 2.

Fase 1: demo_condicion_carrera() reproduce la carrera sobre un saldo y
        verifica la corrección con threading.Lock.
Fase 2: transferir() evita el deadlock con orden de adquisición de candados;
        demo_deadlock() reproduce el interbloqueo con la versión insegura.
"""

from __future__ import annotations

import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from cuenta import CuentaBancaria, FondosInsuficientes, Movimiento


class Banco:
    def __init__(self) -> None:
        self.cuentas: dict[int, CuentaBancaria] = {}

    def abrir_cuenta(self, titular: str, saldo_inicial: int) -> CuentaBancaria:
        numero = len(self.cuentas) + 1
        cuenta = CuentaBancaria(numero, titular, saldo_inicial)
        self.cuentas[numero] = cuenta
        return cuenta

    def saldo_total(self) -> int:
        return sum(c.saldo for c in self.cuentas.values())

    def transferir_inseguro(self, origen: CuentaBancaria, destino: CuentaBancaria, monto: int) -> None:
        """INSEGURO a propósito: bloquea origen y luego destino (deadlock en Fase 2)."""
        with origen.lock:
            time.sleep(0.01)  # simula latencia de validación
            with destino.lock:
                if origen.saldo < monto:
                    raise FondosInsuficientes(f"Cuenta {origen.numero} sin fondos")
                origen.saldo -= monto
                destino.saldo += monto
                origen.historial.append(Movimiento("TRANSFER_OUT", monto))
                destino.historial.append(Movimiento("TRANSFER_IN", monto))

    def transferir(self, origen: CuentaBancaria, destino: CuentaBancaria, monto: int) -> None:
        """Transferencia atómica y sin deadlock: candados siempre en orden
        ascendente de número de cuenta (Fase 2)."""
        if origen.numero == destino.numero:
            raise ValueError("La transferencia requiere dos cuentas distintas")
        if monto <= 0:
            raise ValueError("El monto debe ser positivo")
        primero, segundo = (
            (origen, destino) if origen.numero < destino.numero else (destino, origen)
        )
        with primero.lock:
            with segundo.lock:
                if origen.saldo < monto:
                    raise FondosInsuficientes(
                        f"Cuenta {origen.numero} sin fondos (saldo {origen.saldo})"
                    )
                origen.saldo -= monto
                destino.saldo += monto
                origen.historial.append(Movimiento("TRANSFER_OUT", monto))
                destino.historial.append(Movimiento("TRANSFER_IN", monto))


# ---------------------------------------------------------------------------
# Fase 1: demostración de la condición de carrera
# ---------------------------------------------------------------------------
def demo_condicion_carrera(
    n_hilos: int = 120,
    depositos_por_hilo: int = 1_000,
    seguro: bool = True,
) -> tuple[int, int]:
    """Depósitos concurrentes sobre la misma cuenta.

    seguro=False usa depositar_inseguro (carrera visible).
    seguro=True  usa depositar con Lock (siempre correcto).
    Devuelve (esperado, obtenido).
    """
    cuenta = CuentaBancaria(1, "Cliente Demo", 0)
    depositar = cuenta.depositar if seguro else cuenta.depositar_inseguro

    def cajero() -> None:
        for _ in range(depositos_por_hilo):
            depositar(1)

    hilos = [threading.Thread(target=cajero) for _ in range(n_hilos)]
    inicio = time.perf_counter()
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    esperado = n_hilos * depositos_por_hilo
    etiqueta = "SEGURO (Lock)" if seguro else "INSEGURO (sin Lock)"
    print(
        f"[Fase 1] {etiqueta} | Esperado: {esperado:,} | Obtenido: {cuenta.saldo:,} | "
        f"Perdido: {esperado - cuenta.saldo:,} | {time.perf_counter() - inicio:.2f}s"
    )
    return esperado, cuenta.saldo


# ---------------------------------------------------------------------------
# Fase 2: demostración del interbloqueo
# ---------------------------------------------------------------------------
def demo_deadlock(timeout: float = 3.0) -> bool:
    """Dos hilos transfieren A→B y B→A con la versión insegura.

    Devuelve True si se detectó deadlock (hilos bloqueados tras el timeout).
    """
    banco = Banco()
    a = banco.abrir_cuenta("Ana", 1_000_000)
    b = banco.abrir_cuenta("Beto", 1_000_000)

    def ida() -> None:
        for _ in range(20):
            banco.transferir_inseguro(a, b, 10)

    def vuelta() -> None:
        for _ in range(20):
            banco.transferir_inseguro(b, a, 10)

    # daemon=True para que el programa pueda terminar aunque queden bloqueados
    h1 = threading.Thread(target=ida, daemon=True)
    h2 = threading.Thread(target=vuelta, daemon=True)
    h1.start()
    h2.start()
    h1.join(timeout)
    h2.join(timeout)
    bloqueado = h1.is_alive() or h2.is_alive()
    if bloqueado:
        print(f"[Fase 2] DEADLOCK detectado: los hilos siguen bloqueados tras {timeout}s")
    else:
        print(f"[Fase 2] Terminó sin deadlock (en esta corrida). Repitan varias veces.")
    return bloqueado


# ---------------------------------------------------------------------------
# Fase 2: ráfaga de transferencias concurrentes + invariante de conservación
# ---------------------------------------------------------------------------
def transferencias_simultaneas(
    banco: Banco,
    n_transferencias: int = 3_000,
    max_workers: int = 60,
    fondos_por_cuenta: int = 1_000_000,
) -> tuple[int, int, int]:
    """Lanza n transferencias aleatorias concurrentes sobre las cuentas del banco.

    Devuelve (saldo_antes, saldo_despues, fondos_insuficientes).
    Con transferir() (orden de candados) la invariante de conservación se
    mantiene: saldo_antes == saldo_despues.
    """
    rnd = random.Random(1234)
    cuentas = list(banco.cuentas.values())
    for i, cuenta in enumerate(cuentas, start=1):
        if cuenta.saldo == 0:
            cuenta.saldo = fondos_por_cuenta
    antes = banco.saldo_total()

    # Se precomputan las operaciones en el hilo principal para no compartir
    # el estado del generador aleatorio entre hilos.
    operaciones = []
    for _ in range(n_transferencias):
        origen, destino = rnd.sample(cuentas, 2)
        operaciones.append((origen, destino, rnd.randint(1, 10_000)))

    def aplicar(operacion: tuple[CuentaBancaria, CuentaBancaria, int]) -> None:
        origen, destino, monto = operacion
        try:
            banco.transferir(origen, destino, monto)
        except FondosInsuficientes:
            pass  # saldo insuficiente en origen: la regla de negocio la rechaza

    inicio = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(aplicar, operaciones))
    transcurrido = time.perf_counter() - inicio
    despues = banco.saldo_total()
    print(
        f"[Fase 2] {n_transferencias:,} transferencias con {max_workers} hilos | "
        f"Antes: {antes:,} | Después: {despues:,} | "
        f"Invariante: {'OK' if antes == despues else 'VIOLADA'} | {transcurrido:.2f}s"
    )
    return antes, despues, 0


class RegistradorHistorial:
    """Productor-consumidor (opcional de la Fase 2): un solo hilo escribe el
    historial de todas las cuentas a partir de una cola."""

    def __init__(self, tamaño_cola: int = 10_000) -> None:
        self.cola: queue.Queue[tuple[CuentaBancaria, Movimiento]] = queue.Queue(tamaño_cola)
        self._detener = object()
        self._hilo = threading.Thread(target=self._consumir, daemon=True)
        self.registrados = 0

    def iniciar(self) -> None:
        self._hilo.start()

    def registrar(self, cuenta: CuentaBancaria, movimiento: Movimiento) -> None:
        self.cola.put((cuenta, movimiento))

    def detener(self) -> None:
        self.cola.put(self._detener)  # type: ignore[arg-type]
        self._hilo.join()

    def _consumir(self) -> None:
        while True:
            item = self.cola.get()
            if item is self._detener:
                return
            cuenta, movimiento = item  # type: ignore[misc]
            cuenta.historial.append(movimiento)
            self.registrados += 1


if __name__ == "__main__":
    demo_condicion_carrera(seguro=False)
    demo_condicion_carrera(seguro=True)
    demo_deadlock()

    banco = Banco()
    for i in range(50):
        banco.abrir_cuenta(f"Cliente {i + 1}", 1_000_000)
    transferencias_simultaneas(banco, n_transferencias=3_000, max_workers=60)
