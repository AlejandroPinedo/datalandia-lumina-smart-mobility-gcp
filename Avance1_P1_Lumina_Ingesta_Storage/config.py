#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py - Configuración central del Proyecto DataLandia (Autoridad de Tránsito Lumina)
para el Avance 1 (P1): Diseño de Arquitectura End-to-End y Capa de Ingesta/Storage en GCP.

Integra el catálogo maestro de los 8 Condados de DataLandia, las 12 Agencias unificadas,
las estaciones de tren OART y los esquemas de BigQuery para las 4 fuentes oficiales:
  1. NYC TLC Trip Records       -> tlc_trips_raw
  2. SF Uber Movement Speeds    -> uber_movement_speeds_raw
  3. Array of Things (AoT) IoT  -> iot_aot_telemetry_raw
  4. Censo Demográfico INE      -> censo_demografia_raw
  + Dead-Letter Queue (DLQ)     -> ingesta_deadletter_raw
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH, override=True)
except ImportError:
    if ENV_PATH.exists():
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

# ==============================================================================
# PARÁMETROS GENERALES DE GOOGLE CLOUD PLATFORM
# ==============================================================================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gobiernodatos")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")
GCP_LOCATION = os.getenv("GCP_LOCATION", "US")

PUBSUB_TOPIC_IOT = os.getenv("PUBSUB_TOPIC_IOT", "lumina-iot-aot-telemetry-topic")
PUBSUB_SUB_IOT = os.getenv("PUBSUB_SUB_IOT", "lumina-iot-aot-telemetry-sub")

_raw_bucket = os.getenv("GCS_BUCKET_DATALAKE", "{GCP_PROJECT_ID}-lumina-datalake")
GCS_BUCKET_DATALAKE = _raw_bucket.replace("{GCP_PROJECT_ID}", GCP_PROJECT_ID)

BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "lumina_bronze_raw")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "lumina_silver_clean")
BQ_DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "lumina_gold_governed")
BQ_DATASET_AUDIT = os.getenv("BQ_DATASET_AUDIT", "lumina_audit_dq")

BQ_TABLE_TLC_TRIPS = os.getenv("BQ_TABLE_TLC_TRIPS", "tlc_trips_raw")
BQ_TABLE_UBER_SPEEDS = os.getenv("BQ_TABLE_UBER_SPEEDS", "uber_movement_speeds_raw")
BQ_TABLE_IOT_AOT = os.getenv("BQ_TABLE_IOT_AOT", "iot_aot_telemetry_raw")
BQ_TABLE_CENSO_INE = os.getenv("BQ_TABLE_CENSO_INE", "censo_demografia_raw")
BQ_TABLE_DEADLETTER = os.getenv("BQ_TABLE_DEADLETTER", "ingesta_deadletter_raw")

DATAFLOW_RUNNER = os.getenv("DATAFLOW_RUNNER", "DirectRunner")
DATAFLOW_JOB_NAME = os.getenv("DATAFLOW_JOB_NAME", "lumina-iot-aot-streaming-to-bq")
STREAM_INTERVAL_SECONDS = float(os.getenv("STREAM_INTERVAL_SECONDS", "0.3"))
STREAM_MAX_MESSAGES = int(os.getenv("STREAM_MAX_MESSAGES", "30"))
ERROR_RATE_SIMULATION = float(os.getenv("ERROR_RATE_SIMULATION", "0.15"))

# Rutas completas en GCP
PUBSUB_TOPIC_PATH = f"projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC_IOT}"
PUBSUB_SUB_PATH = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUB_IOT}"

BQ_FULL_TLC = f"{GCP_PROJECT_ID}:{BQ_DATASET_BRONZE}.{BQ_TABLE_TLC_TRIPS}"
BQ_FULL_UBER = f"{GCP_PROJECT_ID}:{BQ_DATASET_BRONZE}.{BQ_TABLE_UBER_SPEEDS}"
BQ_FULL_IOT = f"{GCP_PROJECT_ID}:{BQ_DATASET_BRONZE}.{BQ_TABLE_IOT_AOT}"
BQ_FULL_CENSO = f"{GCP_PROJECT_ID}:{BQ_DATASET_BRONZE}.{BQ_TABLE_CENSO_INE}"
BQ_FULL_DLQ = f"{GCP_PROJECT_ID}:{BQ_DATASET_BRONZE}.{BQ_TABLE_DEADLETTER}"

GCS_TEMP_LOCATION = f"gs://{GCS_BUCKET_DATALAKE}/dataflow/temp"
GCS_STAGING_LOCATION = f"gs://{GCS_BUCKET_DATALAKE}/dataflow/staging"

# ==============================================================================
# CATÁLOGO MAESTRO DE DATALANDIA (8 CONDADOS, ZONAS Y ESTACIONES OART)
# Llaves canónicas que permiten cruzar NYC TLC + Uber Movement + AoT IoT + Censo
# ==============================================================================
DATALANDIA_ZONES = [
    {
        "zone_id": "ZDL-001",
        "tlc_location_id": 161,
        "uber_movement_id": 101,
        "zone_name": "Centro Financiero Metropolitano",
        "county_id": "CNT-01",
        "county_name": "Condado Central (Core)",
        "nearest_oart_station_id": "OART-ST-01",
        "lat": 37.7897,
        "lon": -122.4014,
        "zone_type": "URBANO_DENSO",
    },
    {
        "zone_id": "ZDL-002",
        "tlc_location_id": 237,
        "uber_movement_id": 102,
        "zone_name": "Distrito Tecnológico Norte",
        "county_id": "CNT-02",
        "county_name": "Condado Silicon Norte",
        "nearest_oart_station_id": "OART-ST-04",
        "lat": 37.8044,
        "lon": -122.2712,
        "zone_type": "POLO_LABORAL",
    },
    {
        "zone_id": "ZDL-003",
        "tlc_location_id": 138,
        "uber_movement_id": 103,
        "zone_name": "Suburbio Residencial Colinas Verdes",
        "county_id": "CNT-03",
        "county_name": "Condado Altos del Este",
        "nearest_oart_station_id": "OART-ST-09",
        "lat": 37.7510,
        "lon": -122.1815,
        "zone_type": "SUBURBIO_ADULTO_MAYOR",
    },
    {
        "zone_id": "ZDL-004",
        "tlc_location_id": 132,
        "uber_movement_id": 104,
        "zone_name": "Corredor Universitario Sur",
        "county_id": "CNT-04",
        "county_name": "Condado Valle Joven",
        "nearest_oart_station_id": "OART-ST-15",
        "lat": 37.6879,
        "lon": -122.4702,
        "zone_type": "SUBURBIO_JOVEN_ESTUDIANTIL",
    },
    {
        "zone_id": "ZDL-005",
        "tlc_location_id": 79,
        "uber_movement_id": 105,
        "zone_name": "Puerto Multimodal y Ferry Bahía",
        "county_id": "CNT-05",
        "county_name": "Condado Bahía Oeste",
        "nearest_oart_station_id": "OART-ST-21",
        "lat": 37.7955,
        "lon": -122.3937,
        "zone_type": "NODO_PORTUARIO",
    },
    {
        "zone_id": "ZDL-006",
        "tlc_location_id": 48,
        "uber_movement_id": 106,
        "zone_name": "Parque Industrial y Logístico",
        "county_id": "CNT-06",
        "county_name": "Condado Ribera Industrial",
        "nearest_oart_station_id": "OART-ST-28",
        "lat": 37.6547,
        "lon": -122.4077,
        "zone_type": "POLO_LABORAL",
    },
    {
        "zone_id": "ZDL-007",
        "tlc_location_id": 230,
        "uber_movement_id": 107,
        "zone_name": "Residencial Lago Sereno (Retiro)",
        "county_id": "CNT-07",
        "county_name": "Condado Comarca del Lago",
        "nearest_oart_station_id": "OART-ST-33",
        "lat": 37.8715,
        "lon": -122.2730,
        "zone_type": "SUBURBIO_ADULTO_MAYOR",
    },
    {
        "zone_id": "ZDL-008",
        "tlc_location_id": 68,
        "uber_movement_id": 108,
        "zone_name": "Mirador Turístico y Globos Aerostáticos",
        "county_id": "CNT-08",
        "county_name": "Condado Mirador Sur",
        "nearest_oart_station_id": "OART-ST-40",
        "lat": 37.7219,
        "lon": -122.4782,
        "zone_type": "ECOTURISMO_Y_PERIFERIA",
    },
]

# Esquema BigQuery para Apache Beam (Streaming IoT Array of Things + OART)
BQ_SCHEMA_IOT_AOT = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp_evento", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "node_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "zone_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "county_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "station_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "asset_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "vibration_mms", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "temperature_c", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "sound_db", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "pm25_ugm3", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "passenger_load_pct", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sensor_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "requiere_mantenimiento_pdm", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "ts_ingesta_bq", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

BQ_SCHEMA_DEADLETTER = {
    "fields": [
        {"name": "payload_crudo", "type": "STRING", "mode": "NULLABLE"},
        {"name": "origen_fuente", "type": "STRING", "mode": "NULLABLE"},
        {"name": "motivo_error", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ts_error", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


def mostrar_resumen_config():
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    print(f"{BLUE}=========================================================================={RESET}")
    print(f"{GREEN}🏙️  PROYECTO DATALANDIA (LUMINA) - CONFIGURACIÓN GCP ({GCP_PROJECT_ID}){RESET}")
    print(f"{BLUE}=========================================================================={RESET}")
    print(f"  • Bucket Data Lake GCS   : {YELLOW}gs://{GCS_BUCKET_DATALAKE}{RESET}")
    print(f"  • Tópico Pub/Sub IoT AoT : {YELLOW}{PUBSUB_TOPIC_PATH}{RESET}")
    print(f"  • Dataset BQ Bronze      : {GREEN}{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}{RESET}")
    print(f"  • Tabla 1 (NYC TLC)      : {GREEN}{BQ_FULL_TLC}{RESET}")
    print(f"  • Tabla 2 (Uber Speeds)  : {GREEN}{BQ_FULL_UBER}{RESET}")
    print(f"  • Tabla 3 (AoT IoT/OART) : {GREEN}{BQ_FULL_IOT}{RESET}")
    print(f"  • Tabla 4 (Censo INE)    : {GREEN}{BQ_FULL_CENSO}{RESET}")
    print(f"  • Tabla 5 (Dead-Letter)  : {YELLOW}{BQ_FULL_DLQ}{RESET}")
    print(f"{BLUE}=========================================================================={RESET}")


if __name__ == "__main__":
    mostrar_resumen_config()
