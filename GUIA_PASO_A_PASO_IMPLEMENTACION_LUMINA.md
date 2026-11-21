# Guía de ejecución y validación de Lumina

La versión corregida se valida primero localmente. Los pasos cloud escriben recursos y tablas;
ejecutarlos en un proyecto/datasets de ensayo con credenciales propias. No se ejecutaron en esta revisión.

## Validación local sin dependencias cloud

Desde la raíz del repositorio:

```bash
python -m unittest discover -s tests -v
python build_master_html.py
python build_entrega2_html.py
cd Avance1_P1_Lumina_Ingesta_Storage
python 02_ingesta_batch_multifuente.py
python 06b_pipeline_pyspark_dataproc_silver_gold.py --mode=demo-python
python 08_app_exposicion_frontend_lumina.py
```

La generación local es sintética. Sin dependencias GCP, la API indica unavailable para consultas;
el dashboard puede mostrar datos ilustrativos y caché histórica, siempre identificados como tales.

## Configuración para ensayo en GCP

Instalar requirements.txt en un entorno virtual y usar Application Default Credentials.
Definir GCP_PROJECT_ID, GCP_REGION, GCP_LOCATION y GCS_BUCKET_DATALAKE. Para aislar el ensayo,
definir BQ_DATASET_BRONZE, BQ_DATASET_SILVER, BQ_DATASET_GOLD y BQ_DATASET_AUDIT con nombres nuevos.
La región del bucket y la localización BigQuery deben ser compatibles para cargas y exportaciones;
US y us-central1 no significan réplica dual ni se modifican datasets existentes con CREATE IF NOT EXISTS.

```bash
python 01_setup_infraestructura_lumina.py
python 02_ingesta_batch_multifuente.py --upload
python 03_simulador_streaming_iot_aot_pubsub.py --max-messages 50 --interval 0.3
python 04_pipeline_streaming_beam_bq.py --mode=pull
python 06_pipeline_medallion_silver_gold_ml.py
```

--upload usa staging y MERGE, no reemplaza Bronze. Para procesar CSV existentes:

```bash
python 02_ingesta_batch_multifuente.py --input-dir data_landing_local --upload
```

Se requieren censo_demografia_ine_datalandia.csv, uber_movement_speeds_datalandia.csv y
nyc_tlc_trips_lumina_multimodal.csv con los esquemas del setup. No se importa directamente NYC TLC
sin mapear su esquema y territorio. Los 5M históricos de GCP no se regeneran a partir de 600 filas.

## Procesamiento y control de publicación

06 reconstruye Silver, calcula DQ sobre Bronze/Silver y ejecuta ASSERT antes de Gold.
SIN_DATOS o incumplimiento detienen la publicación. Conocer los conteos de entrada, descartes,
filas únicas y cobertura de las cuatro fuentes; no esperar un 100% de coincidencia temporal cuando
las fechas históricas de viajes difieren del IoT actual.

Comprobar Gold: COUNT(*) = COUNT(DISTINCT trip_id), sin multiplicación tras los JOIN.
Medir COUNTIF(traffic_match_found)/COUNT(*) y COUNTIF(iot_match_found)/COUNT(*).
Las comparaciones sin tráfico coincidente quedan NULL. Registrar los job IDs y el costo real de las consultas.
El plan diario puede responder HISTORIA_INSUFICIENTE con muestras pequeñas o antiguas.

## Beam y Spark

Beam continuo requiere dependencias y empaquetado del validador:

```bash
python 04_pipeline_streaming_beam_bq.py --mode=beam --runner=DataflowRunner
```

setup.py empaqueta telemetry.py para workers. Verificar permisos, localización, cuotas,
errores de escritura y DLQ en el runner. No se afirma que este job se haya ejecutado aquí.

Spark real requiere PySpark/Java compatibles, además de conector GCS si se usan URI gs://:

```bash
python 06b_pipeline_pyspark_dataproc_silver_gold.py --mode=spark --input data_landing_local/nyc_tlc_trips_lumina_multimodal.csv --output salida_silver_nueva
```

La salida debe ser nueva: el modo real falla si ya existe y materializa Silver Parquet.
No usa fallback automático. Gold multifuente se construye con SQL, no con el job Spark.
La captura histórica G5 es demo Python después de fallo Java; obtener nueva evidencia si se evalúa Spark real.

## Airflow

Copiar 07_dag_managed_airflow_composer.py, config.py, pipeline_sql.py y operational_sql.py a dags/.
Requiere Airflow 2.x y provider Google compatibles con el entorno. Configurar las mismas variables
y credenciales del proyecto. El DAG comienza con Bronze ya cargado y ejecuta a las 04:00 America/Lima.
Comparte las etapas Silver, auditoría, compuerta y Gold con CLI; no exporta caché a un disco ajeno.
Validar importación del DAG y correr un ensayo con calidad inválida para comprobar bloqueo.

## API y evidencias

La API expone /api/v1/health, /api/v1/dashboard-summary, /api/v1/iot-telemetry,
/api/v1/route-simulator, /api/v1/agent-query, /api/v1/daily-plan y /api/v1/maintenance-plan.
Los planes cloud no reciben valores sintéticos de respaldo. La simulación de rutas no despacha ni cobra.
Conservar fecha, procedencia, parámetros, job ID, conteos y limitaciones en cada evidencia nueva.

## Cierre de brechas del taller

Integrar solicitudes/GPS/flota/personal/horarios/eventos/accesibilidad para decisiones reales.
Añadir observaciones de espera y disponibilidad para sus KPI; datos financieros observados para probar ahorro.
Validar modelos con partición temporal, errores de predicción y etiquetas de falla.
Ensayar picos del desfile, fallos y recuperación; desplegar y acreditar seguridad/DRP y alternativa abierta.
