"""DAG y CLI con las mismas etapas; Bronze debe estar previamente cargado.

Para Composer copiar este archivo, config.py, pipeline_sql.py y operational_sql.py a dags/ e instalar
el provider Google compatible con el entorno. No aprovisiona Composer.
"""
from datetime import timedelta
import importlib
import config
from pipeline_sql import build_silver_sql, build_dq_sql, build_quality_gate_sql, build_gold_sql


def ejecutar_dag_local_serverless():
    importlib.import_module("06_pipeline_medallion_silver_gold_ml").construir_capas_silver_y_gold()


try:
    from airflow import DAG
except ModuleNotFoundError as exc:
    if exc.name != "airflow":
        raise
    dag = None
else:
    import pendulum
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

    with DAG(
        dag_id="lumina_medallion_end_to_end_dag",
        description="Bronze existente -> Silver -> auditoria SQL -> bloqueo -> Gold",
        schedule_interval="0 4 * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "lumina-data-engineering-group7", "retries": 2,
                      "retry_delay": timedelta(minutes=5)},
        tags=["lumina", "medallion"],
    ) as dag:
        tasks = []
        for task_id, builder in (
            ("transicion_bronze_a_silver", build_silver_sql),
            ("auditoria_sql_calidad", build_dq_sql),
            ("bloquear_publicacion_si_falla_sla", build_quality_gate_sql),
            ("transicion_silver_a_gold_estrella", build_gold_sql),
        ):
            task = BigQueryInsertJobOperator(
                task_id=task_id, project_id=config.GCP_PROJECT_ID, location=config.GCP_LOCATION,
                configuration={"query": {"query": builder(), "useLegacySql": False}},
            )
            if tasks:
                tasks[-1] >> task
            tasks.append(task)


if __name__ == "__main__":
    ejecutar_dag_local_serverless()
