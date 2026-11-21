# Sustentación técnica económica y de resiliencia de Lumina

El proyecto tiene evidencia histórica de almacenamiento y analítica en GCP, y correcciones con
validación local. Esta sustentación presenta supuestos y criterios de diseño; no acredita
despliegue multirregión, pruebas de carga, ahorros económicos observados ni SLAs medidos.

## Decisiones tecnológicas

| Capa | Elección actual | Justificación y límite |
| --- | --- | --- |
| Ingesta | Pub/Sub + Python/Beam | Desacopla productor/consumidor. El SDK usado es Pub/Sub; no se implementa un protocolo Kafka. |
| Batch | BigQuery SQL | Agregación y cruces masivos; depende de dialecto y servicio GCP. |
| Procesamiento abierto | Beam y Spark | Beam usa validador compartido; Spark tiene modo real separado de demo Python. Faltan ejecuciones cloud de esta versión. |
| Storage | BigQuery nativo y GCS | Parquet histórico evidenciado; Iceberg/BigLake portable pendiente. |
| Exposición | Flask REST | Consulta, simulación y planes. No hay motor de despacho ni pagos. |
| Orquestación | DAG Airflow y CLI | Mismos constructores SQL y compuerta de calidad; despliegue Composer pendiente. |

La alternativa portable objetivo es broker Kafka, Beam/Flink, Spark/Trino e Iceberg/Parquet.
Se debe implementar y probar esa ruta antes de afirmar cumplimiento estricto de código abierto.

## Dimensionamiento consistente

Las siguientes cifras son supuestos, no mediciones ni tarifas actuales:

| Supuesto | Cálculo |
| --- | --- |
| 2.5 millones de viajes/día | 75 millones de viajes en 30 días |
| 15,000 eventos/s sostenidos | 1,296 millones de eventos/día; 38,880 millones en 30 días |
| 10 KB decimales/evento | 12.96 TB/día; 388.8 TB/mes antes de compresión y réplicas |
| Pico de 120,000 eventos/s | Ocho veces la base; requiere especificar duración y ensayar backlog |

El cálculo anterior sustituye la mezcla previa de 150 millones de eventos/mes con 15,000 eventos/s:
son escenarios diferentes y no pueden compartir el mismo presupuesto sin redimensionar.
Retención, compresión, filtros, muestreo y exportación cambian el volumen efectivo y deben medirse.

## Economía de movilidad y FinOps

El simulador calcula costo del auto = cargo fijo + distancia * costo variable/km.
Los valores por defecto 3.20 USD y 1.18 USD/km son supuestos configurables, no precios observados.
Las tarifas Lumina y velocidades del simulador también son supuestos. La comparación considera
la misma distancia, pero aún falta calibrar espera, acceso, transbordos, estacionamiento,
ocupación, subsidios y costos de operación para demostrar que el sistema es más económico.

El ingreso tarifario por km se calcula como suma de tarifas / suma de distancia; no es rentabilidad.
Se necesita una muestra observada comparable, análisis por franja/condado y sensibilidad de costos.

El antiguo presupuesto de 895 USD/mes se conserva únicamente como escenario aritmético de referencia,
sin validación de precios ni adecuación al dimensionamiento. Con 75 millones de viajes/mes su división
es 0.00001193 USD/viaje, no 0.000119. No se usa como costo cloud real ni como presupuesto aprobado.
El costo final requiere cotización por región/servicio, volumen publicado y entregado, retención,
procesamiento, consultas, red, observabilidad, redundancia y personal. No se prometen costos cero.

## Escalabilidad e interoperabilidad

El desfile/concierto del condado central debe probarse con escalones de carga y presupuesto acotado.
Registrar tasa de eventos, tamaño medio, p95/p99 end-to-end, backlog, errores, duplicados y tiempo
de recuperación. Verificar cuotas y capacidad de consumidores antes de declarar escalabilidad.
La expansión a otra región requiere catálogo de zonas/agencias y políticas de datos aisladas.

La API REST expone salud, resumen, IoT, simulador, consultas predefinidas, plan diario y mantenimiento.
Los datos de caché tienen fecha de generación. Las respuestas no disponibles no inventan métricas;
el simulador declara su naturaleza. La operación de rutas requiere integrar fuentes adicionales.

## Fiabilidad y recuperación

El consumidor pull confirma solo inserciones aceptadas; los fallos se reentregan.
La semántica es al menos una vez, con insertId estable y deduplicación Silver.
JSON inválido, números no finitos y lecturas fuera de rango se aíslan en DLQ.
La pérdida completa de un sensor también requiere un monitor de heartbeat; una DLQ sola no detecta silencio.

La evidencia actual es regional. La arquitectura dual us-central1/us-east1 y un almacén operacional
redundante son propuestas, no infraestructura desplegada. Metas RPO <= 1 minuto, RTO <= 15 minutos
y disponibilidad 99.95% requieren evaluación de viabilidad y simulacro; no son garantías medidas.

Runbook propuesto: detectar incidente; congelar publicaciones; activar recursos secundarios previamente
aprovisionados; restaurar/reproducir lotes y mensajes; deduplicar; contrastar conteos y ventanas; probar
lecturas/escrituras; conmutar tráfico solo tras validación. Documentar pérdida real y tiempo de recuperación.
No se incluyen comandos de failover que dependan de recursos inexistentes o parámetros sin verificar.

## Privacidad y controles pendientes

SHA-256 de identificadores sintéticos es seudonimización de demo. El salt fijo de ejemplo no es
gestión de secretos ni prueba de Cloud DLP. Para datos personales se requieren análisis de riesgo,
secretos gestionados, control de accesos, minimización y políticas verificadas. TLS, CMEK, VPC Service
Controls e IAM por capa deben acreditarse con configuración/exportes; no se presumen por usar GCP.

## Validación pendiente de infraestructura

Se requieren pruebas en datasets de ensayo de GCP, ejecución Beam y Spark real con registros de job,
importación/ejecución del DAG en Airflow, backtesting de baselines, medición de latencia/costos y simulacro DRP.
Las capturas históricas C1-C3/G4/G5 y las pruebas locales no sustituyen esas verificaciones.
