#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_pipeline_streaming_beam_bq.py
Pipeline de Ingesta y Procesamiento Streaming para la Fuente F3 (Array of Things + Telemetría OART).
Soporta dos modos de ejecución pensados para optimizar el presupuesto de $250 USD en GCP:
  1. Modo Rápido Serverless (`--mode=pull`, por defecto):
     Drena los mensajes en vivo desde la suscripción de Cloud Pub/Sub, aplica la lógica
     de validación física y enrutamiento Dead-Letter Queue (DLQ) e inserta en BigQuery
     sin prometer latencia ni costo medidos. Extracción acotada, no servicio continuo.
  2. Modo Apache Beam / Cloud Dataflow (`--mode=beam --runner=DirectRunner` o `DataflowRunner`):
     Ejecuta el grafo completo de Apache Beam (`beam.DoFn` con `pvalue.TaggedOutput('deadletter')`,
     `ReadFromPubSub` y `WriteToBigQuery`) siguiendo el estándar del Laboratorio 01 de clase.
"""

import argparse
from pathlib import Path
from datetime import datetime, timezone
import json
from google.cloud import bigquery, pubsub_v1
import config


from telemetry import validar_y_enriquecer_evento, persistir_y_confirmar


def consumir_pubsub_hacia_bigquery(max_pull: int = 100):
    config.mostrar_resumen_config()

    subscriber = pubsub_v1.SubscriberClient()
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)

    print(f"\n🌊 Drenando suscripción '{config.PUBSUB_SUB_PATH}' y escribiendo en BigQuery Bronze...")
    response = subscriber.pull(
        request={"subscription": config.PUBSUB_SUB_PATH, "max_messages": max_pull},
        timeout=15.0,
    )

    if not response.received_messages:
        print("ℹ️  No hay mensajes pendientes en la suscripción Pub/Sub. Ejecuta primero el paso 03.")
        return

    total = persistir_y_confirmar(
        response.received_messages, bq_client, subscriber, config.PUBSUB_SUB_PATH,
        f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_IOT_AOT}",
        f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_DEADLETTER}",
    )
    print(f"Persistidos y confirmados: {total} mensajes. Entrega al menos una vez.")



def ejecutar_pipeline_apache_beam(runner: str, extra_args: list[str]):
    """
    Grafo nativo de Apache Beam (compatible con DirectRunner y Cloud DataflowRunner).
    """
    import apache_beam as beam
    from apache_beam import pvalue
    from apache_beam.io import ReadFromPubSub, WriteToBigQuery
    from apache_beam.io.gcp.bigquery import BigQueryDisposition
    from apache_beam.options.pipeline_options import (
        GoogleCloudOptions,
        PipelineOptions,
        SetupOptions,
        StandardOptions,
    )

    class ProcesarTelemetriaLuminaDoFn(beam.DoFn):
        OUTPUT_TAG_DLQ = "deadletter"

        def process(self, element: bytes):
            from telemetry import validar_y_enriquecer_evento
            from apache_beam import pvalue
            raw = element.decode("utf-8", errors="replace") if isinstance(element, bytes) else str(element)
            valid, record = validar_y_enriquecer_evento(raw)
            if valid:
                yield record
            else:
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_DLQ, record)

    options = PipelineOptions(extra_args)
    std_opts = options.view_as(StandardOptions)
    std_opts.runner = runner
    std_opts.streaming = True
    options.view_as(SetupOptions).save_main_session = False
    options.view_as(SetupOptions).setup_file = str(Path(__file__).with_name("setup.py"))

    gcp_opts = options.view_as(GoogleCloudOptions)
    gcp_opts.project = config.GCP_PROJECT_ID
    gcp_opts.region = config.GCP_REGION
    gcp_opts.job_name = f"{config.DATAFLOW_JOB_NAME}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    gcp_opts.temp_location = config.GCS_TEMP_LOCATION
    gcp_opts.staging_location = config.GCS_STAGING_LOCATION

    print(f"🚀 Iniciando Pipeline Apache Beam con runner='{runner}'...")
    with beam.Pipeline(options=options) as p:
        resultados = (
            p
            | "1_ReadFromPubSub" >> ReadFromPubSub(subscription=config.PUBSUB_SUB_PATH)
            | "2_ValidarYClasificarPdM"
            >> beam.ParDo(ProcesarTelemetriaLuminaDoFn()).with_outputs(
                ProcesarTelemetriaLuminaDoFn.OUTPUT_TAG_DLQ, main="validos"
            )
        )

        _ = (
            resultados.validos
            | "3A_WriteValidosBigQuery"
            >> WriteToBigQuery(
                table=config.BQ_FULL_IOT,
                schema=config.BQ_SCHEMA_IOT_AOT,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

        _ = (
            resultados[ProcesarTelemetriaLuminaDoFn.OUTPUT_TAG_DLQ]
            | "3B_WriteDLQBigQuery"
            >> WriteToBigQuery(
                table=config.BQ_FULL_DLQ,
                schema=config.BQ_SCHEMA_DEADLETTER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Streaming IoT AoT + OART hacia BigQuery")
    parser.add_argument(
        "--mode",
        choices=["pull", "beam"],
        default="pull",
        help="'pull' drena la suscripción instantáneamente sin costo de VMs; 'beam' lanza Apache Beam continuo.",
    )
    parser.add_argument("--runner", default=config.DATAFLOW_RUNNER, help="DirectRunner o DataflowRunner")
    parser.add_argument("--max-pull", type=int, default=100)
    args, extra = parser.parse_known_args()

    if args.mode == "beam":
        ejecutar_pipeline_apache_beam(args.runner, extra)
    else:
        consumir_pubsub_hacia_bigquery(max_pull=args.max_pull)
