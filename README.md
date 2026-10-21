# Cuenta-Bancaria-Concurrente-Grupo-4
Said Fandiño, Juan Kammerer, Andrea Paz, Ivanna Pinto, Janny Vergara

# Banco Andino Digital – Cuenta Bancaria Concurrente (Grupo 4)

Unidad 02 – Programación concurrente y paralela en Python.
Se convierte un sistema bancario secuencial e inseguro (`banco_base.py`) en uno
concurrente, correcto y medible usando **threading**, **multiprocessing** y
**asyncio**, cada uno donde corresponde.

## Requisitos

* Python 3.10 o superior (probado con 3.14.7)
* Solo biblioteca estándar

## Parámetros de la variante (Grupo 4)

| Parámetro                  | Valor                                                    |
|----------------------------|----------------------------------------------------------|
| Cuentas                    | 50                                                       |
| Hilos cajero               | 120                                                      |
| Transferencias simultáneas | 3.000                                                    |
| Cuentas a auditar          | 250 (tabla de speedup amplificada a 2.000)               |
| Notificaciones             | 25.000 (tabla medida con muestra de 200 y extrapolación) |

## Archivos

| Archivo              | Contenido                                                                                                                                                      |  Fase |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| `cuenta.py`          | `CuentaBancaria` con `threading.Lock` en `depositar`/`retirar` (+ versiones inseguras para la demo)                                                            |   1   |
| `banco.py`           |`Banco.transferir()`con orden de adquisición de candados,`demo_condicion_carrera()`,`demo_deadlock()`,`transferencias_simultaneas()`e invariante de conservación| 1 y 2 |
| `auditoria.py`       | Auditoría CPU-bound: secuencial, `ThreadPoolExecutor` y `ProcessPoolExecutor`, speedup con 1, 2, 4 y N núcleos                                                 |   3   |
| `servicios_async.py` | Notificaciones y antifraude con `asyncio.gather`, `Semaphore(20)`, corrección con `asyncio.to_thread` y manejo de fallos                                       |   4   |
| `main.py`            | Jornada simulada integrada con los parámetros del grupo                                                                                                        |   5   |
| `test_banco.py`      | Pruebas de estrés e invariantes (`unittest`)                                                                                                                   |  QA   |
| `banco_base.py`      | Código base original (inseguro a propósito), sin modificar                                                                                                     |   –   |

## Ejecución

```bash
# jornada completa (todas las fases, tarda ~3 minutos)
python main.py

# por fase
python banco.py          # Fases 1 y 2 (carrera y deadlock)
python auditoria.py      # Fase 3 (tabla de tiempos y speedup)
python servicios_async.py # Fase 4 (notificaciones y antifraude)

# pruebas automatizadas
python -m unittest test_banco -v
```

## Decisiones de arquitectura (medidas)

| Componente                   | Modelo                                         | Por qué (datos)                                                                                                                        |
|------------------------------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------- |
| Depósitos/retiros de cajeros | Hilos + `threading.Lock` por cuenta            | E/O breve + estado compartido; con 120 hilos × 1.000 depósitos el saldo siempre es 120.000 (sin Lock se pierden ~117000)               |
| Transferencias entre cuentas | Hilos + orden de candados por número de cuenta | Evita el deadlock de A↔B y mantiene la invariante: la suma total no cambia en 3.000 transferencias                                     |
| Auditoría nocturna           | `ProcessPoolExecutor`                          | CPU-bound; los hilos no aceleran por el GIL (~1,0x). Los procesos se acercan al speedup ideal según Amdahl                             |
| Notificaciones/antifraude    | `asyncio` + `Semaphore(20)`                    |I/O de red puro; `gather` ilimitado da ~116x sobre lo secuencial, el semaphore evita saturar el servicio (~18x) con el mismo correctness|

## Pruebas incluidas

* Saldo correcto con N hilos (y la carrera visiblemente pierde dinero sin Lock)
* Ningún saldo negativo bajo retiros concurrentes
* Invariante de conservación en transferencias simultáneas
* Ausencia de deadlock con `join(timeout)` (y detección del deadlock en la versión insegura)
* Equivalencia de las tres versiones de auditoría
* `gather`/`semaphore` más rápidos que lo secuencial; fallos del antifraude no detienen las demás consultas

## Integrantes y roles (Grupo 4)

| Rol                      |Fases                 | Integrante           | 
|--------------------------|----------------------|----------------------|
|Líder técnico / integrador|          1           | Juan Kammerer        |
|Especialista en hilos     |        1 y 2         | Andrea Paz           |
|Especialista en procesos  |          3           | Janny Vergara        |
|Especialista en asyncio   |          4           | Ivanna Pinto         |
|Analista de calidad       |          QA          | Said Fandiño         |
