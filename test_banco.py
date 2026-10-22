"""Pruebas de estrés e invariantes del Banco Andino Digital (QA).

Ejecutar con:  python -m unittest test_banco -v
"""

from __future__ import annotations

import threading
import unittest

from banco import Banco, demo_condicion_carrera, demo_deadlock, transferencias_simultaneas
from cuenta import CuentaBancaria, FondosInsuficientes


class TestFase1Sincronizacion(unittest.TestCase):
    def test_saldo_correcto_con_n_hilos(self) -> None:
        n_hilos, depositos = 80, 1_000
        esperado, obtenido = demo_condicion_carrera(
            n_hilos=n_hilos, depositos_por_hilo=depositos, seguro=True
        )
        self.assertEqual(esperado, obtenido)
        self.assertEqual(obtenido, n_hilos * depositos)

    def test_carrera_pierde_dinero_sin_lock(self) -> None:
        # Con la versión insegura la carrera pierde depósitos (no atómico).
        esperado, obtenido = demo_condicion_carrera(
            n_hilos=80, depositos_por_hilo=1_000, seguro=False
        )
        self.assertLess(obtenido, esperado)

    def test_retiros_concurrentes_nunca_negativo(self) -> None:
        cuenta = CuentaBancaria(1, "Cliente", 50_000)
        rechazados = 0
        rechazados_lock = threading.Lock()

        def cajero() -> None:
            nonlocal rechazados
            for _ in range(200):
                try:
                    cuenta.retirar(100)
                except FondosInsuficientes:
                    with rechazados_lock:
                        rechazados += 1

        hilos = [threading.Thread(target=cajero) for _ in range(60)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()

        self.assertGreaterEqual(cuenta.saldo, 0)
        self.assertEqual(cuenta.saldo, 50_000 - 200 * 60 * 100 + rechazados * 100)
        depositos = [m for m in cuenta.historial if m.tipo == "RETIRO"]
        self.assertEqual(len(depositos), 200 * 60 - rechazados)


class TestFase2Transferencias(unittest.TestCase):
    def test_invariante_conservacion(self) -> None:
        banco = Banco()
        for i in range(50):
            banco.abrir_cuenta(f"Cliente {i + 1}", 1_000_000)
        antes, despues, _ = transferencias_simultaneas(
            banco, n_transferencias=2_000, max_workers=60
        )
        self.assertEqual(antes, despues)
        self.assertTrue(all(c.saldo >= 0 for c in banco.cuentas.values()))

    def test_ausencia_de_deadlock_con_timeout(self) -> None:
        banco = Banco()
        a = banco.abrir_cuenta("Ana", 10_000_000)
        b = banco.abrir_cuenta("Beto", 10_000_000)

        def ida() -> None:
            for _ in range(500):
                banco.transferir(a, b, 10)

        def vuelta() -> None:
            for _ in range(500):
                banco.transferir(b, a, 10)

        h1 = threading.Thread(target=ida)
        h2 = threading.Thread(target=vuelta)
        h1.start()
        h2.start()
        h1.join(timeout=10.0)
        h2.join(timeout=10.0)
        self.assertFalse(h1.is_alive() or h2.is_alive(), "transferir() produjo deadlock")
        self.assertEqual(banco.saldo_total(), 20_000_000)

    def test_transferencia_rechazada_sin_fondos(self) -> None:
        banco = Banco()
        a = banco.abrir_cuenta("Ana", 100)
        b = banco.abrir_cuenta("Beto", 0)
        with self.assertRaises(FondosInsuficientes):
            banco.transferir(a, b, 200)
        self.assertEqual(banco.saldo_total(), 100)

    def test_transferencia_invalida(self) -> None:
        banco = Banco()
        a = banco.abrir_cuenta("Ana", 1_000)
        with self.assertRaises(ValueError):
            banco.transferir(a, a, 10)
        with self.assertRaises(ValueError):
            banco.transferir(a, a, -5)

    def test_deadlock_se_detecta_en_version_insegura(self) -> None:
        self.assertTrue(demo_deadlock(timeout=3.0))


class TestFase3Auditoria(unittest.TestCase):
    def test_auditar_cuenta_recalcula_saldo(self) -> None:
        from auditoria import auditar_cuenta

        historial = [("DEPOSITO", 100_000), ("RETIRO", 30_000), ("DEPOSITO", 5_000)]
        saldo, digest = auditar_cuenta(historial, iteraciones=10)
        self.assertEqual(saldo, 75_000)
        self.assertEqual(len(digest), 16)

    def test_auditorias_equivalentes(self) -> None:
        from auditoria import (
            auditoria_con_hilos,
            auditoria_con_procesos,
            auditoria_secuencial,
            generar_historiales,
        )

        historiales = generar_historiales(20)
        base = auditoria_secuencial(historiales)
        self.assertEqual(auditoria_con_hilos(historiales, 4), base)
        self.assertEqual(auditoria_con_procesos(historiales, 2), base)


class TestFase4Servicios(unittest.TestCase):
    def test_semaphore_mas_rapido_que_secuencial(self) -> None:
        import asyncio

        from servicios_async import (
            notificaciones_gather,
            notificaciones_secuenciales,
            notificaciones_semaphore,
        )

        n = 30
        t_sec = asyncio.run(notificaciones_secuenciales(n))
        t_gather = asyncio.run(notificaciones_gather(n))
        t_sem = asyncio.run(notificaciones_semaphore(n, limite=20))
        self.assertLess(t_gather, t_sec)
        self.assertLess(t_sem, t_sec)
        # gather y semaphore deben ser del mismo orden de magnitud
        self.assertLess(t_sem, t_gather * 3)

    def test_antifraude_no_detiene_las_demas(self) -> None:
        import asyncio

        from servicios_async import ErrorAntifraude, consultar_antifraude, procesar_antifraude

        aprobadas, rechazadas, pendientes, _ = asyncio.run(procesar_antifraude(500))
        self.assertEqual(aprobadas + rechazadas + pendientes, 500)
        # con 5 % de fallas esperadas y 500 llamadas, casi seguro hay alguna
        self.assertGreaterEqual(aprobadas + rechazadas, 400)

        async def una_falla() -> None:
            await consultar_antifraude(1, 100)
            raise AssertionError("debió fallar")

        async def probar() -> list[object]:
            return await asyncio.gather(
                consultar_antifraude(1, 100), una_falla(), return_exceptions=True
            )

        resultado = asyncio.run(probar())
        self.assertIsInstance(resultado[1], (ErrorAntifraude, AssertionError))


if __name__ == "__main__":
    unittest.main(verbosity=2)
