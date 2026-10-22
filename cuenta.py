"""Modelo de cuenta bancaria sincronizado (Fase 1).

Contiene FondosInsuficientes, Movimiento y CuentaBancaria.
depositar/retirar usan threading.Lock por cuenta (exclusión mutua).
depositar_inseguro/retirar_inseguro SOLO existen para reproducir la
condición de carrera en la demostración de la Fase 1.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime


class FondosInsuficientes(Exception):
    """Se lanza cuando un retiro o transferencia supera el saldo disponible."""


@dataclass
class Movimiento:
    tipo: str          # "DEPOSITO" | "RETIRO" | "TRANSFER_IN" | "TRANSFER_OUT"
    monto: int         # en pesos, siempre positivo
    fecha: datetime = field(default_factory=datetime.now)


class CuentaBancaria:
    def __init__(self, numero: int, titular: str, saldo_inicial: int = 0) -> None:
        self.numero = numero
        self.titular = titular
        self.saldo = saldo_inicial
        self.historial: list[Movimiento] = []
        self.lock = threading.Lock()

    def depositar(self, monto: int) -> None:
        if monto <= 0:
            raise ValueError("El monto debe ser positivo")
        with self.lock:
            self.saldo += monto
            self.historial.append(Movimiento("DEPOSITO", monto))

    def retirar(self, monto: int) -> None:
        if monto <= 0:
            raise ValueError("El monto debe ser positivo")
        with self.lock:
            if self.saldo < monto:
                raise FondosInsuficientes(
                    f"Cuenta {self.numero}: saldo {self.saldo} < {monto}"
                )
            self.saldo -= monto
            self.historial.append(Movimiento("RETIRO", monto))

    def depositar_inseguro(self, monto: int) -> None:
        """INSEGURO a propósito: solo para la demostración de la Fase 1."""
        saldo_actual = self.saldo
        time.sleep(0)                      # cede el GIL: hace visible la carrera
        self.saldo = saldo_actual + monto
        self.historial.append(Movimiento("DEPOSITO", monto))

    def retirar_inseguro(self, monto: int) -> None:
        """INSEGURO a propósito: solo para la demostración de la Fase 1."""
        if self.saldo < monto:
            raise FondosInsuficientes(
                f"Cuenta {self.numero}: saldo {self.saldo} < {monto}"
            )
        saldo_actual = self.saldo
        time.sleep(0)
        self.saldo = saldo_actual - monto
        self.historial.append(Movimiento("RETIRO", monto))

    def __repr__(self) -> str:
        return f"Cuenta({self.numero}, {self.titular}, saldo={self.saldo})"
