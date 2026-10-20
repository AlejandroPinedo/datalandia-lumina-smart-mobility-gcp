#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_pipeline_streaming_beam_bq.py
Pipeline de Ingesta y Validación para la Fuente F3 (Array of Things + Telemetría OART).
Lee los mensajes en tiempo real desde la suscripción de Cloud Pub/Sub, aplica las reglas
de integridad física y negocio de Lumina:
  - Si el sensor OART reporta lecturas corruptas (node_id vacío, vibración <= 0, o
    temperatura fuera de [-40, 180] °C), desvía el evento a la tabla Dead-Letter Queue
    (`ingesta_deadletter_raw`) sin interrumpir la operación.
  - Si el evento es válido, calcula `sensor_status` (NORMAL, WARNING_PDM, CRITICAL_PDM)
    y `requiere_mantenimiento_pdm` (BOOLEAN) y lo inserta en BigQuery (`iot_aot_telemetry_raw`).
"""

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


if __name__ == "__main__":
    consumir_pubsub_hacia_bigquery()
