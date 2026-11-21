# Código de Lumina

Ver la [guía de ejecución](../GUIA_PASO_A_PASO_IMPLEMENTACION_LUMINA.md) y el
[estado del proyecto](../README.md) antes de usar servicios cloud.

| Archivo | Función y alcance |
| --- | --- |
| 01_setup_infraestructura_lumina.py | Crea recursos base y tablas Bronze; no acredita red privada, CMEK ni DRP. |
| 02_ingesta_batch_multifuente.py | Generación sintética local por defecto; --upload usa staging y MERGE. |
| 03_simulador_streaming_iot_aot_pubsub.py | Publica eventos sintéticos, incluidas anomalías. |
| 04_pipeline_streaming_beam_bq.py | Pull acotado o Beam continuo; validador compartido y confirmación selectiva en pull. |
| telemetry.py y setup.py | Validación sin dependencias cloud y empaquetado Beam. |
| 05_verificar_cruce_multifuente_bq.py | Consultas de diagnóstico; no certifica las cuatro verticales completas. |
| 06_pipeline_medallion_silver_gold_ml.py | Ejecuta etapas compartidas y exporta caché; no entrena modelos ML. |
| pipeline_sql.py | SQL de Silver, auditoría, compuerta y Gold. |
| operational_sql.py | Baselines de demanda diaria y tendencia de mantenimiento; pendientes de validación cloud. |
| 06b_pipeline_pyspark_dataproc_silver_gold.py | Spark real materializa Silver Parquet; --mode=demo-python es una demo explícita. |
| 07_dag_managed_airflow_composer.py | Mismas etapas SQL desde Airflow; Bronze debe estar cargado. |
| 08_app_exposicion_frontend_lumina.py | APIs, consultas de lectura y simulación con supuestos identificados. |

No se promete costo cero, ejecución Spark distribuida probada ni operación en vivo a partir de la demo.
