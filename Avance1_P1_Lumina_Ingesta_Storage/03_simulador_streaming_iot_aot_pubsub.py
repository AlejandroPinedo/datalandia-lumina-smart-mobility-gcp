#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_simulador_streaming_iot_aot_pubsub.py
Simula en Tiempo Real la Fuente F3 (Sensores Urbanos Array of Things - AoT +
Telemetría de Trenes e Infraestructura Ferroviaria OART) publicando eventos JSON
hacia Google Cloud Pub/Sub (`lumina-iot-aot-telemetry-topic`).

Incluye:
  - Telemetría normal y de alerta preventiva (vibración en mm/s, temperatura en °C,
    ruido en dB, PM2.5 y porcentaje de carga de vagones OART).
  - Inyección controlada de fallas de sensores (criterio de evaluación de Fiabilidad:
    "¿Qué sucede si un sensor de la red OART falla?") para demostrar cómo el pipeline
    de ingesta en Apache Beam / Dataflow aísla el fallo en la Dead-Letter Queue (DLQ).
"""

import argparse
from datetime import datetime, timezone
import json
import random
import time
import uuid

from google.cloud import pubsub_v1
import config


ASSET_TYPES = [
    "OART_RAIL_SWITCH",
    "OART_TRAIN_BOGIE",
    "URBAN_AOT_ENV_NODE",
    "LUMINA_EV_MINIVAN_TELEMATICS",
]


def generar_evento_iot_aot(forzar_fallo_sensor: bool = False) -> dict:
    zona = random.choice(config.DATALANDIA_ZONES)
    asset = random.choice(ASSET_TYPES)
    node_num = random.randint(100, 999)
    now_iso = datetime.now(timezone.utc).isoformat()

    if forzar_fallo_sensor:
        tipo_fallo = random.choice(["VIBRACION_NEGATIVA_CORRUPTA", "TEMP_FUERA_ESCALA_999C", "NODE_ID_VACIO"])
        if tipo_fallo == "VIBRACION_NEGATIVA_CORRUPTA":
            return {
                "event_id": f"EVT-ERR-{uuid.uuid4().hex[:8].upper()}",
                "timestamp_evento": now_iso,
                "node_id": f"AOT-{zona['zone_id']}-{node_num}",
                "zone_id": zona["zone_id"],
                "county_id": zona["county_id"],
                "station_id": zona["nearest_oart_station_id"],
                "asset_type": "OART_RAIL_SWITCH",
                "lat": zona["lat"],
                "lon": zona["lon"],
                "vibration_mms": -45.0,  # Error físico de sensor roto
                "temperature_c": 42.1,
                "sound_db": 71.0,
                "pm25_ugm3": 14.2,
                "passenger_load_pct": 65,
            }
        elif tipo_fallo == "TEMP_FUERA_ESCALA_999C":
            return {
                "event_id": f"EVT-ERR-{uuid.uuid4().hex[:8].upper()}",
                "timestamp_evento": now_iso,
                "node_id": f"OART-SENS-{node_num}",
                "zone_id": zona["zone_id"],
                "county_id": zona["county_id"],
                "station_id": zona["nearest_oart_station_id"],
                "asset_type": "OART_TRAIN_BOGIE",
                "lat": zona["lat"],
                "lon": zona["lon"],
                "vibration_mms": 4.2,
                "temperature_c": 999.9,  # Cortocircuito en termopar OART
                "sound_db": 82.0,
                "pm25_ugm3": 18.0,
                "passenger_load_pct": 80,
            }
        else:
            return {
                "event_id": f"EVT-ERR-{uuid.uuid4().hex[:8].upper()}",
                "timestamp_evento": now_iso,
                "node_id": "",  # Sin identificador de nodo
                "zone_id": zona["zone_id"],
                "station_id": zona["nearest_oart_station_id"],
                "vibration_mms": 3.1,
                "temperature_c": 35.0,
            }

    # Simular 20% de probabilidad de requerir Mantenimiento Preventivo (PdM)
    es_alerta_pdm = random.random() < 0.22
    if es_alerta_pdm:
        vibration = round(random.uniform(7.8, 14.5), 2)   # Vibración alta en riel/bogie
        temp_c = round(random.uniform(81.0, 105.0), 2)    # Sobrecalentamiento
        sound_db = round(random.uniform(88.0, 102.0), 1)
    else:
        vibration = round(random.uniform(1.2, 6.4), 2)
        temp_c = round(random.uniform(22.0, 68.0), 2)
        sound_db = round(random.uniform(55.0, 79.0), 1)

    return {
        "event_id": f"AOT-{uuid.uuid4().hex[:10].upper()}",
        "timestamp_evento": now_iso,
        "node_id": f"NODE-{zona['zone_id']}-{node_num}",
        "zone_id": zona["zone_id"],
        "county_id": zona["county_id"],
        "station_id": zona["nearest_oart_station_id"],
        "asset_type": asset,
        "lat": round(zona["lat"] + random.uniform(-0.008, 0.008), 6),
        "lon": round(zona["lon"] + random.uniform(-0.008, 0.008), 6),
        "vibration_mms": vibration,
        "temperature_c": temp_c,
        "sound_db": sound_db,
        "pm25_ugm3": round(random.uniform(8.0, 38.0), 2),
        "passenger_load_pct": random.randint(25, 98),
    }


def publicar_stream_iot(max_messages: int, interval: float):
    config.mostrar_resumen_config()
    publisher = pubsub_v1.PublisherClient()
    topic_path = config.PUBSUB_TOPIC_PATH

    print(f"\n📡 Publicando {max_messages} eventos de telemetría AoT / OART en '{topic_path}'...\n")
    enviados_ok = 0
    enviados_falla = 0

    for i in range(1, max_messages + 1):
        es_fallo = random.random() < config.ERROR_RATE_SIMULATION
        evento = generar_evento_iot_aot(forzar_fallo_sensor=es_fallo)
        payload_bytes = json.dumps(evento, ensure_ascii=False).encode("utf-8")

        future = publisher.publish(
            topic_path,
            data=payload_bytes,
            source="ARRAY_OF_THINGS_OART",
            zone_id=str(evento.get("zone_id", "UNKNOWN")),
        )
        msg_id = future.result(timeout=15)

        if es_fallo:
            enviados_falla += 1
            print(
                f"  🔴 [{i:02d}/{max_messages}] [FALLA SENSOR SIMULADA -> DLQ] msg_id={msg_id} | "
                f"station={evento.get('station_id')} | vib={evento.get('vibration_mms')} | temp={evento.get('temperature_c')}"
            )
        else:
            enviados_ok += 1
            alerta_tag = "⚠️ ALERTA PdM" if evento["vibration_mms"] > 7.5 else "🟢 NORMAL"
            print(
                f"  {alerta_tag} [{i:02d}/{max_messages}] msg_id={msg_id} | "
                f"zona={evento['zone_id']} ({evento['station_id']}) | "
                f"vib={evento['vibration_mms']} mm/s | temp={evento['temperature_c']}°C | "
                f"carga_OART={evento['passenger_load_pct']}%"
            )
        time.sleep(interval)

    print(
        f"\n✅ Stream finalizado: {enviados_ok} eventos válidos + {enviados_falla} eventos de sensores fallidos "
        f"enviados a Cloud Pub/Sub."
    )
    print("👉 Siguiente paso: python3 04_pipeline_streaming_beam_bq.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador Streaming IoT Array of Things + OART")
    parser.add_argument("--max-messages", type=int, default=config.STREAM_MAX_MESSAGES)
    parser.add_argument("--interval", type=float, default=config.STREAM_INTERVAL_SECONDS)
    args = parser.parse_args()
    publicar_stream_iot(args.max_messages, args.interval)
