# Procesamiento y modelado de DataLandia Lumina

El pipeline cruza cuatro fuentes canónicas y construye estructuras analíticas. Esta revisión corrige
el grano temporal, el control de calidad, la persistencia IoT y la reproducibilidad. Hay evidencia histórica
de materialización en BigQuery; la nueva versión solo tiene validación local.

## Matriz de cumplimiento del taller

| Requisito | Implementación y límite |
| --- | --- |
| Clasificación de fuentes | Viajes y tráfico batch; demografía de referencia; IoT streaming. Los generadores producen datos sintéticos, no descargan las fuentes sugeridas. |
| Integración multifuente | Silver deduplicada; tráfico por origen, destino, fecha UTC y hora; IoT por zona, estación, fecha UTC y hora. |
| Procesamiento híbrido | Pull acotado o Beam continuo explícito; batch SQL; Spark real separado de demo Python. |
| Almacenamiento | BigQuery nativo y GCS CSV/Parquet. Iceberg es arquitectura objetivo pendiente. |
| Decisiones en tiempo real | API de simulación con supuestos; no asigna vehículos ni optimiza una red de transporte en vivo. |
| Planificación diaria | Promedio por día semanal en 28 días y vehículos equivalentes bajo supuesto configurable. Falta disponibilidad de flota/personal y eventos. |
| Informes operativos | Distancia, duración, tarifas/km, ingreso tarifario/km, ocupación y cobertura del cruce. Espera y utilización de flota son NULL por falta de fuentes. |
| Mantenimiento | Umbrales y extrapolación lineal por activo de las últimas 24 horas. No es un modelo de probabilidad de falla validado. |
| Seguridad y calidad | Validación, seudonimización de demo, auditoría SQL y bloqueo del SLA. IAM, red privada y CMEK pendientes de evidencias. |
| Escalabilidad y expansión | Configuración de proyecto y datasets; faltan pruebas de carga y parametrización completa de territorio/catálogos. |

## Etapas compartidas de ejecución

El script 06 y el DAG 07 importan los mismos constructores de pipeline_sql.py:

1. Bronze -> Silver: deduplicación y filtros; zonas y puntos geográficos canónicos.
2. Auditoría SQL: cuatro reglas sobre datos Bronze y Silver.
3. Compuerta: exige cuatro reglas con filas evaluadas y porcentaje exacto >= 95%.
4. Silver -> Gold: dimensión, hechos, KPI, plan diario y propuesta de mantenimiento.
5. Solo en CLI: exportación de caché JSON local. Airflow publica tablas; no promete sincronización de un archivo local con Cloud Run.

Si falla la compuerta, no se ejecuta la etapa Gold ni se exporta caché. El DAG se ejecuta
a las 04:00 America/Lima y no permite ejecuciones simultáneas. Requiere Bronze cargado;
no finge haber detectado archivos ni recarga automáticamente las fuentes.

## Grano y cobertura del cruce

fact_movilidad_multimodal_gold conserva una fila por trip_id deduplicado.
La dimensión de tráfico es única por origin_zone_id, dest_zone_id, travel_date y hod.
La telemetría se agrega por zone_id, station_id, fecha y hora UTC. Es un enriquecimiento
analítico por ventana horaria, no una decisión causal en tiempo real.

La ausencia de tráfico o sensores se conserva como NULL. traffic_match_found e iot_match_found
permiten medir la cobertura. Ya no se inventan velocidad vial, ocupación ni ausencia de alertas.
Las fechas distintas de los datos históricos y el IoT actual pueden producir baja cobertura legítima.

## Calidad y fiabilidad

Las reglas verifican integridad espacial Bronze, validez temporal/financiera Bronze,
validez física IoT y unicidad Silver. Un porcentaje se compara sin redondear;
una tabla vacía produce SIN_DATOS. Las filas descartadas de Bronze no desaparecen del denominador.
El SLA no equivale a garantizar que toda métrica sea válida o que no existan problemas de origen.

El validador telemetry.py se comparte con pull y Beam. Rechaza JSON malformado,
objetos no válidos, identificadores ausentes, fechas sin zona, números no convertibles,
NaN/Infinity, coordenadas inválidas y ocupación fuera de rango.

En pull solo se confirma cada fila que BigQuery aceptó, incluida la DLQ. Los errores de
transporte o inserción dejan mensajes pendientes para reentrega. insertId estable reduce
duplicados, pero no garantiza exactly-once; Silver deduplica por event_id. Se requiere
monitorizar backlog y establecer una política operacional de reintentos/DLQ del broker.

## Planeamiento y mantenimiento exploratorios

plan_diario_recursos_gold calcula demanda para mañana con promedio del mismo día semanal
en cuatro semanas. Requiere al menos 14 días observados y dos comparables; si no, devuelve
HISTORIA_INSUFICIENTE y valores NULL. Los días ausentes no se suponen demanda cero.
El cálculo de vehículos usa PLANNING_TRIPS_PER_VEHICLE_DAY (20 por defecto), un supuesto
de capacidad equivalente; no es un despacho ni un dimensionamiento por modo validado.

mantenimiento_proactivo_gold estima pendientes de vibración y temperatura por nodo,
con al menos tres instantes y 60 minutos observados. Proyecta horas hasta umbrales de demo
(7.5 mm/s o 80 °C), identifica sensores desactualizados y propone inspección. Una alerta actual
puede priorizarse sin ajustar una tendencia. No emite órdenes de trabajo ni certifica seguridad.

Antes de producción: backtesting temporal, error de pronóstico, estacionalidad/eventos,
etiquetas de fallas y umbrales aprobados por ingeniería de mantenimiento.

## Economía e informes

El costo contrafactual del auto usa CAR_FIXED_COST_USD y CAR_VARIABLE_COST_USD_KM.
benchmark_source declara SUPUESTO_TARIFARIO_NO_OBSERVADO. Los ahorros son de escenario;
no prueban ahorro real de la población. ingreso_km_lumina_usd = suma de tarifas / suma de km
para viajes Lumina; no representa margen ni costo de operación de flota.

## Evidencias históricas

| Captura | Qué acredita y qué no |
| --- | --- |
| C1 | Tabla Gold con 4,998,926 filas y particionamiento. No acredita esta nueva versión. |
| C2 | Resultados de tres reglas SQL históricas. No acredita Dataplex. 100.0% financiero es redondeado. |
| C3 | Tópico y suscripción Pub/Sub; no mide latencia end-to-end. |
| G4 | Objetos Parquet en GCS regional; no demuestra tablas Iceberg ni DRP dual. |
| G5 | Java/Spark falló; luego se ejecutó Python sobre 600 filas. No acredita Spark real. |

## Validación reproducible

python -m unittest discover -s tests -v ejecuta pruebas locales de fallos IoT, confirmación selectiva,
cruces con fechas/horas/estaciones distintas, estados DQ, bloqueo antes de Gold, carga segura y
respuestas sin datos inventados. SQLite valida las relaciones y reglas con adaptación de dialecto.
Esto no ejecuta el SQL completo en BigQuery ni valida los baselines en producción.
