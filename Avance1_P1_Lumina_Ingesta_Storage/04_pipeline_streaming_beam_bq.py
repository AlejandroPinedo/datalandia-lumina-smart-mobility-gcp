#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_pipeline_streaming_beam_bq.py
Pipeline de Ingesta y Procesamiento Streaming para la Fuente F3 (Array of Things + Telemetría OART).
Soporta dos modos de ejecución pensados para optimizar el presupuesto de $250 USD en GCP:
  1. Modo Rápido Serverless (`--mode=pull`, por defecto):
     Drena los mensajes en vivo desde la suscripción de Cloud Pub/Sub, aplica la lógica
     de validación física y enrutamiento Dead-Letter Queue (DLQ) e inserta en BigQuery
     en < 3 segundos con costo $0.00 en máquinas virtuales.
  2. Modo Apache Beam / Cloud Dataflow (`--mode=beam --runner=DirectRunner` o `DataflowRunner`):
     Ejecuta el grafo completo de Apache Beam (`beam.DoFn` con `pvalue.TaggedOutput('deadletter')`,
     `ReadFromPubSub` y `WriteToBigQuery`) siguiendo el estándar del Laboratorio 01 de clase.
"""

import argparse
from datetime import datetime, timezone
import json
from google.cloud import bigquery, pubsub_v1
import config


def validar_y_enriquecer_evento(raw_payload: str) -> tuple[bool, dict]:
    """
    Evalúa la calidad y validez física del evento IoT de Array of Things / OART.
    Retorna (es_valido, registro_formateado).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        data = json.loads(raw_payload)
    except Exception as exc:
        return False, {
            "payload_crudo": raw_payload[:1000],
            "origen_fuente": "IOT_AOT_STREAM",
            "motivo_error": f"JSON_MALFORMADO: {exc}",
            "ts_error": now_iso,
        }

    node_id = str(data.get("node_id") or "").strip()
    vib = float(data.get("vibration_mms", -1.0))
    temp = float(data.get("temperature_c", -999.0))

    if not node_id:
        return False, {
            "payload_crudo": raw_payload[:1000],
            "origen_fuente": "IOT_AOT_STREAM",
            "motivo_error": "SENSOR_SIN_NODE_ID",
            "ts_error": now_iso,
        }

    if vib <= 0.0 or vib > 100.0:
        return False, {
            "payload_crudo": raw_payload[:1000],
            "origen_fuente": "IOT_AOT_STREAM",
            "motivo_error": f"FALLO_SENSOR_VIBRACION_FUERA_RANGO ({vib} mm/s)",
            "ts_error": now_iso,
        }

    if temp < -40.0 or temp > 180.0:
        return False, {
            "payload_crudo": raw_payload[:1000],
            "origen_fuente": "IOT_AOT_STREAM",
            "motivo_error": f"FALLO_TERMOPAR_OART_FUERA_ESCALA ({temp} C)",
            "ts_error": now_iso,
        }

    # Clasificación de Mantenimiento Predictivo (Vertical 4 de Lumina)
    if vib >= 7.5 or temp >= 80.0:
        status = "CRITICAL_PDM"
        req_pdm = True
    elif vib >= 5.5 or temp >= 65.0:
        status = "WARNING_PDM"
        req_pdm = False
    else:
        status = "NORMAL"
        req_pdm = False

    return True, {
        "event_id": data.get("event_id", "EVT-UNKNOWN"),
        "timestamp_evento": data.get("timestamp_evento", now_iso),
        "node_id": node_id,
        "zone_id": data.get("zone_id", "ZDL-001"),
        "county_id": data.get("county_id", "CNT-01"),
        "station_id": data.get("station_id", "OART-ST-01"),
        "asset_type": data.get("asset_type", "OART_RAIL_SWITCH"),
        "lat": float(data.get("lat", 37.7897)),
        "lon": float(data.get("lon", -122.4014)),
        "vibration_mms": vib,
        "temperature_c": temp,
        "sound_db": float(data.get("sound_db", 65.0)),
        "pm25_ugm3": float(data.get("pm25_ugm3", 12.0)),
        "passenger_load_pct": int(data.get("passenger_load_pct", 50)),
        "sensor_status": status,
        "requiere_mantenimiento_pdm": req_pdm,
        "ts_ingesta_bq": now_iso,
    }


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

    registros_validos = []
    registros_dlq = []
    ack_ids = []

    for received in response.received_messages:
        ack_ids.append(received.ack_id)
        raw_str = received.message.data.decode("utf-8", errors="replace")
        es_valido, record = validar_y_enriquecer_evento(raw_str)
        if es_valido:
            registros_validos.append(record)
        else:
            registros_dlq.append(record)

    # Insertar válidos en iot_aot_telemetry_raw
    if registros_validos:
        table_iot = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_IOT_AOT}"
        errs = bq_client.insert_rows_json(table_iot, registros_validos)
        if not errs:
            print(f"  ✅ Insertados {len(registros_validos)} eventos IoT válidos en '{table_iot}'")
        else:
            print(f"  ⚠️ Errores al insertar en '{table_iot}': {errs}")

    # Insertar corruptos en ingesta_deadletter_raw
    if registros_dlq:
        table_dlq = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_DEADLETTER}"
        errs_dlq = bq_client.insert_rows_json(table_dlq, registros_dlq)
        if not errs_dlq:
            print(f"  🛡️ Aislados {len(registros_dlq)} eventos de sensores fallidos en Dead-Letter '{table_dlq}'")
        else:
            print(f"  ⚠️ Errores al insertar en DLQ '{table_dlq}': {errs_dlq}")

    # Confirmar ACK en Pub/Sub
    subscriber.acknowledge(request={"subscription": config.PUBSUB_SUB_PATH, "ack_ids": ack_ids})
    print(f"  📬 ACK confirmado para {len(ack_ids)} mensajes en Cloud Pub/Sub.")
    print("👉 Siguiente paso: python3 05_verificar_cruce_multifuente_bq.py")


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
            import json
            from datetime import datetime, timezone
            from apache_beam import pvalue

            now_iso = datetime.now(timezone.utc).isoformat()
            raw_str = element.decode("utf-8", errors="replace") if isinstance(element, bytes) else str(element)
            try:
                data = json.loads(raw_str)
            except Exception as exc:
                yield pvalue.TaggedOutput(
                    self.OUTPUT_TAG_DLQ,
                    {
                        "payload_crudo": raw_str[:1000],
                        "origen_fuente": "IOT_AOT_BEAM",
                        "motivo_error": f"JSON_MALFORMADO: {exc}",
                        "ts_error": now_iso,
                    },
                )
                return

            node_id = str(data.get("node_id") or "").strip()
            vib = float(data.get("vibration_mms", -1.0))
            temp = float(data.get("temperature_c", -999.0))

            if not node_id or vib <= 0.0 or vib > 100.0 or temp < -40.0 or temp > 180.0:
                yield pvalue.TaggedOutput(
                    self.OUTPUT_TAG_DLQ,
                    {
                        "payload_crudo": raw_str[:1000],
                        "origen_fuente": "IOT_AOT_BEAM",
                        "motivo_error": f"LECTURA_FUERA_RANGO (node={node_id}, vib={vib}, temp={temp})",
                        "ts_error": now_iso,
                    },
                )
                return

            status = "CRITICAL_PDM" if (vib >= 7.5 or temp >= 80.0) else ("WARNING_PDM" if (vib >= 5.5 or temp >= 65.0) else "NORMAL")
            yield {
                "event_id": data.get("event_id", "EVT-UNKNOWN"),
                "timestamp_evento": data.get("timestamp_evento", now_iso),
                "node_id": node_id,
                "zone_id": data.get("zone_id", "ZDL-001"),
                "county_id": data.get("county_id", "CNT-01"),
                "station_id": data.get("station_id", "OART-ST-01"),
                "asset_type": data.get("asset_type", "OART_RAIL_SWITCH"),
                "lat": float(data.get("lat", 37.7897)),
                "lon": float(data.get("lon", -122.4014)),
                "vibration_mms": vib,
                "temperature_c": temp,
                "sound_db": float(data.get("sound_db", 65.0)),
                "pm25_ugm3": float(data.get("pm25_ugm3", 12.0)),
                "passenger_load_pct": int(data.get("passenger_load_pct", 50)),
                "sensor_status": status,
                "requiere_mantenimiento_pdm": status == "CRITICAL_PDM",
                "ts_ingesta_bq": now_iso,
            }

    options = PipelineOptions(extra_args)
    std_opts = options.view_as(StandardOptions)
    std_opts.runner = runner
    std_opts.streaming = True
    options.view_as(SetupOptions).save_main_session = False

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
