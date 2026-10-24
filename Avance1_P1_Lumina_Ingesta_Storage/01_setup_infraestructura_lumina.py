#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_setup_infraestructura_lumina.py
Aprovisiona automáticamente en Google Cloud Platform (GCP) toda la Capa de Ingesta
y Almacenamiento (Data Lakehouse Medallion) para el proyecto DataLandia (Lumina):
  1. Habilita las APIs necesarias (Pub/Sub, Dataflow, BigQuery, Cloud Storage)
  2. Crea el Tópico y Suscripción en Cloud Pub/Sub para telemetría IoT (Array of Things + OART)
  3. Crea el Bucket de Data Lake en Cloud Storage (gs://{PROJECT_ID}-lumina-datalake)
  4. Crea los 4 Datasets Medallion en BigQuery (Bronze, Silver, Gold, Audit)
  5. Crea las 5 Tablas Físicas Particionadas y Clusterizadas en Capa Bronze:
     - tlc_trips_raw             (NYC TLC Trip Record Data adaptado a Lumina)
     - uber_movement_speeds_raw  (SF Uber Movement Speeds - baseline 24 km/h)
     - iot_aot_telemetry_raw     (Array of Things Urban Sensors + Trenes OART)
     - censo_demografia_raw      (Censo Demográfico INE - 8 Condados y Adulto Mayor/Jóvenes)
     - ingesta_deadletter_raw    (Dead-Letter Queue para tolerancia a fallos de sensores)
"""

import os
import subprocess
import config


def run_cmd(cmd: list[str], desc: str) -> bool:
    env = os.environ.copy()
    env["CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE"] = "false"
    env["CLOUDSDK_BILLING_QUOTA_PROJECT"] = config.GCP_PROJECT_ID
    env["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
    print(f"⚙️  {desc}...")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"   ✅ Completado: {desc}")
        return True
    stderr = (result.stderr or result.stdout).strip()
    if any(k in stderr.lower() for k in ("already exists", "alreadyexists", "you already own")):
        print(f"   ℹ️  El recurso ya existe ({desc}), continuando...")
        return True
    print(f"   ⚠️  Detalle en {desc}: {stderr[:280]}")
    return False


def crear_infraestructura_gcp():
    config.mostrar_resumen_config()

    # 1. Habilitar APIs clave en GCP
    run_cmd(
        [
            "gcloud", "services", "enable",
            "pubsub.googleapis.com",
            "dataflow.googleapis.com",
            "bigquery.googleapis.com",
            "storage.googleapis.com",
            "dataplex.googleapis.com",
            "dlp.googleapis.com",
            f"--project={config.GCP_PROJECT_ID}",
            "--quiet",
        ],
        f"Habilitando APIs de Ingesta, Storage y Gobierno en '{config.GCP_PROJECT_ID}'",
    )

    # 2. Crear Tópico y Suscripción en Cloud Pub/Sub (Streaming IoT AoT + OART)
    run_cmd(
        [
            "gcloud", "pubsub", "topics", "create",
            config.PUBSUB_TOPIC_IOT,
            f"--project={config.GCP_PROJECT_ID}",
        ],
        f"Creando tópico Pub/Sub '{config.PUBSUB_TOPIC_IOT}'",
    )

    run_cmd(
        [
            "gcloud", "pubsub", "subscriptions", "create",
            config.PUBSUB_SUB_IOT,
            f"--topic={config.PUBSUB_TOPIC_IOT}",
            f"--project={config.GCP_PROJECT_ID}",
            "--ack-deadline=60",
        ],
        f"Creando suscripción Pub/Sub '{config.PUBSUB_SUB_IOT}'",
    )

    # 3. Crear Bucket de Data Lake en Cloud Storage
    run_cmd(
        [
            "gcloud", "storage", "buckets", "create",
            f"gs://{config.GCS_BUCKET_DATALAKE}",
            f"--project={config.GCP_PROJECT_ID}",
            f"--location={config.GCP_REGION}",
            "--uniform-bucket-level-access",
        ],
        f"Creando Data Lake Bucket 'gs://{config.GCS_BUCKET_DATALAKE}'",
    )

    # 4. Crear Schemas Medallion y las 5 Tablas Particionadas/Clusterizadas en BigQuery
    sql_ddl = f"""
    -- =========================================================================
    -- 1. CREACIÓN DE SCHEMAS MEDALLION (BRONZE, SILVER, GOLD, AUDIT)
    -- =========================================================================
    CREATE SCHEMA IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}`
    OPTIONS (
      location = '{config.GCP_LOCATION}',
      description = 'Lumina Capa Bronze: Ingesta multifuente cruda (NYC TLC, Uber Movement, Array of Things IoT, Censo INE)'
    );

    CREATE SCHEMA IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_SILVER}`
    OPTIONS (
      location = '{config.GCP_LOCATION}',
      description = 'Lumina Capa Silver: Datos limpios, deduplicados y homologados por zone_id, county_id y station_id OART'
    );

    CREATE SCHEMA IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}`
    OPTIONS (
      location = '{config.GCP_LOCATION}',
      description = 'Lumina Capa Gold: Modelo Estrella para las 4 Verticales (Tiempo Real, Planes Diarios, Informes OLAP, PdM)'
    );

    CREATE SCHEMA IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_AUDIT}`
    OPTIONS (
      location = '{config.GCP_LOCATION}',
      description = 'Lumina Capa de Auditoria: Reglas Dataplex Data Quality y hallazgos Sensitive Data Protection (DLP)'
    );

    -- =========================================================================
    -- 2. TABLA BRONZE F1: VIAJES MULTIMODALES Y TAXIS (Ref: NYC TLC Trip Data)
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_TLC_TRIPS}` (
      trip_id STRING NOT NULL OPTIONS(description='Identificador unico del viaje'),
      pickup_datetime TIMESTAMP NOT NULL OPTIONS(description='Fecha y hora de inicio del viaje'),
      dropoff_datetime TIMESTAMP NOT NULL OPTIONS(description='Fecha y hora de fin del viaje'),
      hod INT64 OPTIONS(description='Hora del dia (0-23) para cruce con Uber Movement'),
      pulocation_id INT64 OPTIONS(description='LocationID original de NYC TLC'),
      dolocation_id INT64 OPTIONS(description='LocationID destino original de NYC TLC'),
      origin_zone_id STRING NOT NULL OPTIONS(description='Llave espacial canonica Origen DataLandia (ZDL-001..008)'),
      dest_zone_id STRING NOT NULL OPTIONS(description='Llave espacial canonica Destino DataLandia (ZDL-001..008)'),
      county_id STRING OPTIONS(description='Condado de origen (CNT-01..08)'),
      agency_id STRING OPTIONS(description='Agencia de transito (AG-01 a AG-12 unificadas en Lumina)'),
      transport_mode STRING OPTIONS(description='Modo: LUMINA_MULTIMODAL_OART, LUMINA_EV_MINIVAN, LUMINA_BIKE, PERSONAL_CAR_BENCHMARK'),
      connected_oart_station_id STRING OPTIONS(description='Estacion OART utilizada en primera/ultima milla'),
      passenger_count INT64 OPTIONS(description='Pasajeros a bordo para medir porcentaje de ocupacion'),
      trip_distance_km FLOAT64 OPTIONS(description='Distancia recorrida en kilometros'),
      duration_minutes FLOAT64 OPTIONS(description='Duracion total del viaje en minutos'),
      effective_speed_kmh FLOAT64 OPTIONS(description='Velocidad efectiva del viaje en km/h'),
      base_fare_usd FLOAT64 OPTIONS(description='Tarifa base en USD'),
      total_fare_usd FLOAT64 OPTIONS(description='Costo total del viaje en USD'),
      cost_per_km_usd FLOAT64 OPTIONS(description='KPI clave: Costo por kilometro recorrido (USD/km)'),
      payment_type STRING OPTIONS(description='LUMINA_UNIFIED_PAY, CARD, CASH'),
      rider_id_hash STRING OPTIONS(description='Identificador seudonimizado con SHA-256 (Cloud DLP / GDPR Privacy)'),
      payment_token_masked STRING OPTIONS(description='Token de pago enmascarado (Lumina Unified Pay DLP Masked)'),
      ts_ingesta_bq TIMESTAMP OPTIONS(description='Timestamp de carga en BigQuery Bronze')
    )
    PARTITION BY DATE(pickup_datetime)
    CLUSTER BY origin_zone_id, dest_zone_id, transport_mode, agency_id
    OPTIONS (
      description = 'Fuente F1 (Batch/Micro-batch): Registro de viajes multimodales basado en NYC TLC Trip Records',
      partition_expiration_days = 90
    );

    -- =========================================================================
    -- 3. TABLA BRONZE F2: TIEMPOS Y VELOCIDAD ENTRE ZONAS (Ref: Uber Movement)
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_UBER_SPEEDS}` (
      movement_id STRING NOT NULL OPTIONS(description='ID unico del registro Origen-Destino por hora'),
      travel_date DATE NOT NULL OPTIONS(description='Fecha de observacion vial'),
      hod INT64 NOT NULL OPTIONS(description='Hour of Day (0 a 23)'),
      time_period STRING OPTIONS(description='PEAK_MORNING, PEAK_EVENING, OFF_PEAK'),
      sourceid INT64 OPTIONS(description='ID poligono origen Uber Movement'),
      dstid INT64 OPTIONS(description='ID poligono destino Uber Movement'),
      origin_zone_id STRING NOT NULL OPTIONS(description='Zona canonica origen en DataLandia'),
      dest_zone_id STRING NOT NULL OPTIONS(description='Zona canonica destino en DataLandia'),
      county_id STRING OPTIONS(description='Condado asociado'),
      distance_corridor_km FLOAT64 OPTIONS(description='Distancia vial del corredor en km'),
      mean_travel_time_sec FLOAT64 OPTIONS(description='Tiempo promedio de viaje en segundos (Uber Movement)'),
      stddev_travel_time_sec FLOAT64 OPTIONS(description='Desviacion estandar del tiempo de viaje en segundos'),
      avg_speed_kmh FLOAT64 OPTIONS(description='Velocidad vial promedio en km/h (Hora punta DataLandia ~ 24 km/h)'),
      is_severe_congestion BOOL OPTIONS(description='TRUE cuando la velocidad cae por debajo de 24 km/h (15 mph)'),
      ts_ingesta_bq TIMESTAMP OPTIONS(description='Timestamp de carga en BigQuery Bronze')
    )
    PARTITION BY travel_date
    CLUSTER BY origin_zone_id, dest_zone_id, hod
    OPTIONS (
      description = 'Fuente F2 (Batch): Tiempos de viaje y velocidades urbanas basados en SF Caltrain / Uber Movement',
      partition_expiration_days = 90
    );

    -- =========================================================================
    -- 4. TABLA BRONZE F3: TELEMETRÍA URBANA E INFRAESTRUCTURA (Array of Things)
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_IOT_AOT}` (
      event_id STRING NOT NULL OPTIONS(description='UUID del evento de telemetria IoT'),
      timestamp_evento TIMESTAMP NOT NULL OPTIONS(description='Timestamp exacto de lectura del sensor'),
      node_id STRING NOT NULL OPTIONS(description='ID del nodo Array of Things o tren OART'),
      zone_id STRING OPTIONS(description='Zona de DataLandia asignada por geocodificacion / ST_CONTAINS'),
      county_id STRING OPTIONS(description='Condado donde se ubica el nodo'),
      station_id STRING OPTIONS(description='Estacion o tramo de la red ferroviaria OART monitoreado'),
      asset_type STRING OPTIONS(description='OART_RAIL_SWITCH, OART_TRAIN_BOGIE, URBAN_ENV_NODE, MINIVAN_TELEMATICS'),
      lat FLOAT64 OPTIONS(description='Latitud WGS84 del nodo sensor'),
      lon FLOAT64 OPTIONS(description='Longitud WGS84 del nodo sensor'),
      vibration_mms FLOAT64 OPTIONS(description='Vibracion mecanica en mm/s para mantenimiento predictivo'),
      temperature_c FLOAT64 OPTIONS(description='Temperatura del componente o ambiente en grados Celsius'),
      sound_db FLOAT64 OPTIONS(description='Nivel de presion sonora en decibeles (dB)'),
      pm25_ugm3 FLOAT64 OPTIONS(description='Particulas finas PM2.5 para medir impacto ecologico de Lumina'),
      passenger_load_pct INT64 OPTIONS(description='Porcentaje de ocupacion en tiempo real del vagon OART (0-100%)'),
      sensor_status STRING OPTIONS(description='NORMAL, WARNING_PDM, CRITICAL_PDM'),
      requiere_mantenimiento_pdm BOOL OPTIONS(description='TRUE si supera umbrales de vibracion (>7.5 mm/s) o temp (>80 C)'),
      ts_ingesta_bq TIMESTAMP OPTIONS(description='Timestamp de escritura via Dataflow / BigQuery Storage Write API')
    )
    PARTITION BY DATE(timestamp_evento)
    CLUSTER BY zone_id, station_id, node_id, sensor_status
    OPTIONS (
      description = 'Fuente F3 (Streaming): Telemetria de sensores urbanos Array of Things y red ferroviaria OART',
      partition_expiration_days = 90
    );

    -- =========================================================================
    -- 5. TABLA BRONZE F4: CENSO DEMOGRÁFICO Y VIVIENDA (Ref: Censo INE)
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_CENSO_INE}` (
      zone_id STRING NOT NULL OPTIONS(description='Llave primaria de zona en DataLandia (ZDL-001..008)'),
      zone_name STRING OPTIONS(description='Nombre del sector o distrito metropolitano'),
      county_id STRING NOT NULL OPTIONS(description='ID de los 8 condados de DataLandia (CNT-01..08)'),
      county_name STRING OPTIONS(description='Nombre oficial del condado'),
      nearest_oart_station_id STRING OPTIONS(description='Estacion OART de referencia en un radio de 8 km'),
      zone_type STRING OPTIONS(description='Tipologia urbana y demografica del sector'),
      poblacion_residente INT64 OPTIONS(description='Poblacion censada (Total DataLandia > 9 millones)'),
      viviendas_totales INT64 OPTIONS(description='Total de hogares censados en la zona'),
      pct_adulto_mayor_65 FLOAT64 OPTIONS(description='Porcentaje de residentes >= 65 anos (alta dependencia de primera milla)'),
      pct_jovenes_18_34 FLOAT64 OPTIONS(description='Porcentaje de jovenes 18-34 anos (prefieren transporte publico)'),
      ingreso_medio_hogar_usd FLOAT64 OPTIONS(description='Ingreso promedio mensual del hogar en USD'),
      pct_hogares_sin_auto FLOAT64 OPTIONS(description='Porcentaje de hogares sin vehiculo particular'),
      pct_menos_8km_estacion_oart FLOAT64 OPTIONS(description='Cobertura a < 8 km de estacion OART (89% promedio regional)'),
      indice_prioridad_primera_milla FLOAT64 OPTIONS(description='Score compuesto (0-100) para asignar flota diaria de Minivans/Bikes'),
      center_lat FLOAT64 OPTIONS(description='Latitud del centroide de la zona'),
      center_lon FLOAT64 OPTIONS(description='Longitud del centroide de la zona'),
      ts_ingesta_bq TIMESTAMP OPTIONS(description='Timestamp de carga en BigQuery Bronze')
    )
    CLUSTER BY county_id, zone_id, nearest_oart_station_id
    OPTIONS (
      description = 'Fuente F4 (Batch Estático): Censo de Poblacion y Vivienda (INE) mapeado a los 8 condados de DataLandia'
    );

    -- =========================================================================
    -- 6. TABLA TRANSVERSAL: DEAD-LETTER QUEUE (Tolerancia a Fallos de Ingesta)
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_DEADLETTER}` (
      payload_crudo STRING OPTIONS(description='Mensaje JSON/CSV original que fallo en la validacion de ingesta'),
      origen_fuente STRING OPTIONS(description='Fuente generadora (IOT_AOT_STREAM, TLC_BATCH, UBER_MOVEMENT)'),
      motivo_error STRING OPTIONS(description='Causa técnica del rechazo (ej. sensor OART fuera de rango o JSON corrupto)'),
      ts_error TIMESTAMP OPTIONS(description='Timestamp de captura en la cola Dead-Letter')
    )
    PARTITION BY DATE(ts_error)
    CLUSTER BY origen_fuente, motivo_error
    OPTIONS (
      description = 'Dead-Letter Queue (DLQ) para garantizar cero perdida de datos ante fallos de sensores OART',
      partition_expiration_days = 90
    );
    """

    run_cmd(
        [
            "bq", "query",
            "--use_legacy_sql=false",
            f"--project_id={config.GCP_PROJECT_ID}",
            sql_ddl,
        ],
        f"Creando Schemas Medallion y las 5 Tablas de Ingesta en '{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}'",
    )

    print("\n🎉 ¡Infraestructura de Ingesta y Storage de DataLandia (Lumina) lista en GCP!")
    print("👉 Siguiente paso: python3 02_ingesta_batch_multifuente.py")


if __name__ == "__main__":
    crear_infraestructura_gcp()
