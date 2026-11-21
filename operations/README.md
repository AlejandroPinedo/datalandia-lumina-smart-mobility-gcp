# Implementación y validación del alcance académico de Lumina

El Word exige diseñar una arquitectura para cuatro verticales. El PDF exige una
implementación híbrida de procesamiento y una demostración funcional end-to-end de
al menos un proceso clave. El proceso demostrado es OLAP. No se presenta una
operación de transporte comercial ni cuatro servicios en producción.

## Implementación existente y comprobación

| Vertical | Código ejecutable y verificación | Límite de los datos |
| --- | --- | --- |
| Decisiones en tiempo real | `dispatch.py`: caminos de costo mínimo, filtros de capacidad, accesibilidad, turno, frescura y cierres; asignación global por flujo de costo mínimo con exclusión de conductor/vehículo. Pruebas de contratos aisladas. | No se inventa flota, solicitudes, GPS ni horarios. Sin fuentes devuelve `requires_sources`. Las pruebas unitarias no se presentan como viajes reales. |
| Planificación diaria | `forecast.py`: modelos estacionales y mediana reciente, selección en validación y evaluación posterior independientes. Se ejecutaron 32 series históricas de 59 días. | Con 266 días de antigüedad no se publican pronósticos para mañana ni cantidades de vehículos ficticias. |
| Informes OLAP | `validation_sql.py` y `P3/completar_validacion_fondo.py`: Bronze → Silver → cuatro reglas DQ → compuerta ≥95% → Gold → agregados → API → React. Ejecutado en BigQuery sobre las fuentes existentes. | Tarifas y referencia del auto observadas en las tablas; no equivalen a costos operativos o ahorro causal. |
| Mantenimiento | `event_store.py`, `maintenance.py`: estado incremental por activo, event time, control de frescura, tendencias cuando hay historial suficiente y alertas de la fuente. | Hay una observación por sensor y no hay fallas confirmadas. No se entrena ni se inventa una probabilidad de falla. |

`contracts.json` define entradas, llaves, tiempos, restricciones y respuestas ante
ausencias. No contiene registros sintéticos ni defaults de disponibilidad.

## Resultado del pipeline de esta versión

Dataset aislado: `lumina_validacion_20261121_053327_c8cc78`.
Las tablas Bronze/Silver/Gold históricas se conservaron y su fecha de modificación
fue contrastada antes/después.

- Entrada: 5.000.000 viajes Bronze. Salida: 4.996.009 viajes únicos válidos.
- Se excluyeron 3.991 registros según limpieza y calidad; no se reemplazan por otros.
- La calidad financiera/temporal fue 99,92018%; las otras tres reglas, 100%.
- La dimensión censal coincide con todos los viajes válidos.
- Viajes: enero/febrero de 2026. Tráfico: 10/11/2026. IoT: 11/11/2026.
- La cobertura temporal de tráfico/IoT sobre esos viajes es cero. Se conserva
  ausencia y se expone el contexto territorial separado, con sus fechas.

La web del pasajero y el tablero original continúan consultando el Gold histórico
de 4.998.926 registros. **Validación integral** consulta el Gold nuevo de esta
ejecución, con 4.996.009 registros. Son fuentes identificadas distintas; la diferencia
no se oculta ni se mezcla entre indicadores.

## Procesamiento híbrido, portabilidad y recuperación

El batch corre en BigQuery SQL. El procesador abierto incremental usa SQLite WAL,
transacciones durables, deduplicación por `event_id` y digest, cuarentena y estado por
tiempo del evento. La reentrega no duplica registros y un evento tardío no reemplaza
el estado más reciente.

Se reprodujeron los 11 eventos existentes, con sus identificadores y fechas originales.
Los 11 fueron aceptados; al reentregarlos se reconocieron 11 duplicados. Se respaldó
la base y se contrastaron integridad y recuentos al restaurarla.

`stream_worker.py` admite JSONL continuo (`--follow`) y un adaptador de consumo
Pub/Sub que confirma después del commit. El fallo de almacenamiento no emite ACK.
No se publican eventos inventados en Pub/Sub para demostrar el procesamiento.

Ejecutar desde la raíz, usando el archivo real exportado por la validación:

```powershell
python -m operations.stream_worker --input .runtime/operations/20261121_053327_c8cc78/events_existing.jsonl --db .runtime/operations/replay.sqlite3
```

El motor abierto `portable.py` ejecutó el cruce sobre 960 registros reales exportados,
sin multiplicación de filas. Esto comprueba portabilidad funcional del cruce; no
acredita escala metropolitana ni equivalencia de rendimiento con un clúster distribuido.
Python, SQL, SQLite, JSON, React y Node son componentes abiertos. BigQuery es el
servicio gestionado empleado por el curso; no se lo denomina tecnología abierta.

## APIs y comprobaciones reproducibles

- `/api/v1/operations`: ejecución identificada, fuentes, calidad, Gold y modelos.
- `/api/v1/validation/daily-plan`: resultados históricos, sin despacho ficticio.
- `/api/v1/validation/maintenance`: diagnóstico histórico, sin probabilidades de falla inventadas.
- `http://127.0.0.1:3000/p3#operations`: consumidor React del pipeline nuevo.

```powershell
python -m unittest discover -s tests -v
python P3/verificar_fondo.py
python P3/verificar_carga_local.py
npm.cmd --prefix frontend run build
npm.cmd --prefix frontend test
```

La carga HTTP mide lectura local con respuestas reales. No equivale a probar un
desfile metropolitano, una tasa de 15.000 eventos/s, DRP multirregión o un SLA cloud.
La recuperación SQLite comprobada complementa el diseño DRP/HA de la arquitectura;
no lo transforma en infraestructura multirregional desplegada.

Para repetir la demo cloud usar `P3/completar_validacion_fondo.ps1`. Si la última
ejecución quedó incompleta, retoma etapas con el mismo SQL y fuentes sin cambios;
si ya terminó, crea un dataset aislado nuevo. Cada consulta tiene un máximo de
10 GiB facturables. El flujo nunca apunta sus escrituras a las tablas históricas.
