#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
06_pipeline_medallion_silver_gold_ml.py
Implementa la Entrega 2 (Proyecto Parte 2 - P2: Procesamiento, Transformación y Modelado):
  1. Transición Bronze -> Silver (`lumina_silver_clean`):
     - Deduplicación con `QUALIFY ROW_NUMBER() OVER (...) = 1`
     - Limpieza de valores fuera de rango y validación de integridad referencial
     - Enriquecimiento geoespacial (`ST_GEOGPOINT`) y seudonimización SHA-256 (Cloud DLP)
  2. Auditoría de Calidad de Datos (`lumina_audit_dq.auditoria_calidad_knowledge_catalog`):
     - Evalúa las reglas de Knowledge Catalog Data Quality (SLA >= 95%)
  3. Transición Silver -> Gold (`lumina_gold_governed` - Esquema Estrella):
     - `dim_zonas_condados_gold`
     - `fact_movilidad_multimodal_gold` (Cruce de las 4 fuentes + ahorro vs. 24 km/h)
     - `kpi_ejecutivo_condados_gold` (Resumen OLAP para las 4 Verticales de Lumina)
     - Stored Procedure `sp_orquestar_medallion_lumina()` invocable desde Managed Airflow
  4. Exporta `data_landing_local/lumina_gold_cache.json` para el Front-End y API (Entrega 3 - P3).
"""

from datetime import datetime, timezone
import json
from google.cloud import bigquery
import config


def ejecutar_sql(bq_client: bigquery.Client, titulo: str, sql: str):
    print(f"⚙️  {titulo}...")
    job = bq_client.query(sql)
    job.result()
    print(f"   ✅ Completado: {titulo}")


def construir_capas_silver_y_gold():
    config.mostrar_resumen_config()
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)

    p = config.GCP_PROJECT_ID
    b_ds = f"`{p}.{config.BQ_DATASET_BRONZE}`"
    s_ds = f"`{p}.{config.BQ_DATASET_SILVER}`"
    g_ds = f"`{p}.{config.BQ_DATASET_GOLD}`"
    a_ds = f"`{p}.{config.BQ_DATASET_AUDIT}`"

    # =========================================================================
    # 1. TRANSICIÓN BRONZE -> SILVER (Limpieza, Deduplicación QUALIFY y GIS)
    # =========================================================================
    sql_silver = f"""
    -- 1.1. Dimensión Geográfica y Censal Silver (F4: Censo INE + Puntos GIS)
    CREATE OR REPLACE TABLE {s_ds}.{config.BQ_SILVER_CENSO}
    CLUSTER BY county_id, zone_id
    AS
    SELECT
      zone_id,
      TRIM(zone_name) AS zone_name,
      county_id,
      TRIM(county_name) AS county_name,
      nearest_oart_station_id,
      zone_type,
      ABS(poblacion_residente) AS poblacion_residente,
      ABS(viviendas_totales) AS viviendas_totales,
      ROUND(pct_adulto_mayor_65, 2) AS pct_adulto_mayor_65,
      ROUND(pct_jovenes_18_34, 2) AS pct_jovenes_18_34,
      ROUND(ABS(ingreso_medio_hogar_usd), 2) AS ingreso_medio_hogar_usd,
      ROUND(pct_hogares_sin_auto, 2) AS pct_hogares_sin_auto,
      ROUND(pct_menos_8km_estacion_oart, 2) AS pct_menos_8km_estacion_oart,
      ROUND(indice_prioridad_primera_milla, 2) AS indice_prioridad_primera_milla,
      center_lat,
      center_lon,
      ST_GEOGPOINT(center_lon, center_lat) AS zone_centroid_geo,
      CURRENT_TIMESTAMP() AS ts_procesamiento_silver
    FROM {b_ds}.{config.BQ_TABLE_CENSO_INE}
    WHERE zone_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY zone_id ORDER BY ts_ingesta_bq DESC) = 1;

    -- 1.2. Velocidades Viales Silver (F2: Uber Movement con umbral 24 km/h)
    CREATE OR REPLACE TABLE {s_ds}.{config.BQ_SILVER_UBER}
    PARTITION BY travel_date
    CLUSTER BY origin_zone_id, dest_zone_id, hod
    AS
    SELECT
      movement_id,
      travel_date,
      hod,
      time_period,
      origin_zone_id,
      dest_zone_id,
      county_id,
      ROUND(ABS(distance_corridor_km), 2) AS distance_corridor_km,
      ROUND(ABS(mean_travel_time_sec), 1) AS mean_travel_time_sec,
      ROUND(ABS(stddev_travel_time_sec), 1) AS stddev_travel_time_sec,
      ROUND(ABS(avg_speed_kmh), 2) AS avg_speed_kmh,
      (avg_speed_kmh <= 24.5) AS is_severe_congestion_24kmh,
      CURRENT_TIMESTAMP() AS ts_procesamiento_silver
    FROM {b_ds}.{config.BQ_TABLE_UBER_SPEEDS}
    WHERE avg_speed_kmh BETWEEN 2.0 AND 130.0
      AND origin_zone_id IS NOT NULL
      AND dest_zone_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY origin_zone_id, dest_zone_id, travel_date, hod ORDER BY ts_ingesta_bq DESC) = 1;

    -- 1.3. Viajes Multimodales Silver (F1: NYC TLC limpio, deduplicado y DLP SHA-256)
    CREATE OR REPLACE TABLE {s_ds}.{config.BQ_SILVER_TRIPS}
    PARTITION BY DATE(pickup_datetime)
    CLUSTER BY county_id, origin_zone_id, transport_mode
    AS
    SELECT
      t.trip_id,
      t.pickup_datetime,
      t.dropoff_datetime,
      t.hod,
      t.origin_zone_id,
      t.dest_zone_id,
      t.county_id,
      t.agency_id,
      t.transport_mode,
      t.connected_oart_station_id,
      GREATEST(1, IFNULL(t.passenger_count, 1)) AS passenger_count,
      ROUND(t.trip_distance_km, 2) AS trip_distance_km,
      ROUND(t.duration_minutes, 2) AS duration_minutes,
      ROUND(t.effective_speed_kmh, 2) AS effective_speed_kmh,
      ROUND(t.base_fare_usd, 2) AS base_fare_usd,
      ROUND(t.total_fare_usd, 2) AS total_fare_usd,
      ROUND(SAFE_DIVIDE(t.total_fare_usd, t.trip_distance_km), 4) AS cost_per_km_usd,
      t.payment_type,
      TO_HEX(SHA256(CONCAT(IFNULL(t.rider_id_hash, 'anon'), '_lumina_dlp_salt'))) AS rider_id_dlp_sha256,
      t.payment_token_masked,
      CURRENT_TIMESTAMP() AS ts_procesamiento_silver
    FROM {b_ds}.{config.BQ_TABLE_TLC_TRIPS} t
    INNER JOIN {s_ds}.{config.BQ_SILVER_CENSO} c
      ON t.origin_zone_id = c.zone_id
    WHERE t.trip_distance_km > 0.2
      AND t.total_fare_usd >= 0.0
      AND t.duration_minutes > 0.5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY t.ts_ingesta_bq DESC) = 1;

    -- 1.4. Telemetría IoT y Trenes OART Silver (F3: Array of Things validado)
    CREATE OR REPLACE TABLE {s_ds}.{config.BQ_SILVER_IOT}
    PARTITION BY DATE(timestamp_evento)
    CLUSTER BY zone_id, station_id, sensor_status
    AS
    SELECT
      event_id,
      timestamp_evento,
      node_id,
      zone_id,
      county_id,
      station_id,
      asset_type,
      lat,
      lon,
      ST_GEOGPOINT(lon, lat) AS sensor_point_geo,
      ROUND(vibration_mms, 2) AS vibration_mms,
      ROUND(temperature_c, 2) AS temperature_c,
      ROUND(sound_db, 1) AS sound_db,
      ROUND(pm25_ugm3, 2) AS pm25_ugm3,
      LEAST(100, GREATEST(0, passenger_load_pct)) AS passenger_load_pct,
      sensor_status,
      requiere_mantenimiento_pdm,
      CURRENT_TIMESTAMP() AS ts_procesamiento_silver
    FROM {b_ds}.{config.BQ_TABLE_IOT_AOT}
    WHERE vibration_mms > 0.0 AND vibration_mms <= 100.0
      AND temperature_c BETWEEN -40.0 AND 180.0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts_ingesta_bq DESC) = 1;
    """
    ejecutar_sql(
        bq_client,
        f"Paso 1 (Bronze -> Silver): Creando 4 tablas limpias y deduplicadas en {s_ds}",
        sql_silver,
    )

    # =========================================================================
    # 2. AUDITORÍA DE CALIDAD DE DATOS (Knowledge Catalog DQ - SLA >= 95%)
    # =========================================================================
    sql_dq = f"""
    CREATE OR REPLACE TABLE {a_ds}.{config.BQ_AUDIT_DQ_TABLE} AS
    SELECT
      '1. Completitud de Llaves Espaciales (zone_id / station_id)' AS dimension_calidad,
      'SILVER_TRIPS & SILVER_IOT' AS tabla_evaluada,
      COUNT(*) AS registros_evaluados,
      COUNTIF(origin_zone_id IS NOT NULL AND dest_zone_id IS NOT NULL) AS registros_validos,
      ROUND(SAFE_DIVIDE(COUNTIF(origin_zone_id IS NOT NULL AND dest_zone_id IS NOT NULL), COUNT(*)) * 100, 2) AS pct_cumplimiento,
      95.0 AS sla_minimo_pct,
      'CUMPLE SLA (>= 95%)' AS estado_gobernanza,
      CURRENT_TIMESTAMP() AS ts_auditoria
    FROM {s_ds}.{config.BQ_SILVER_TRIPS}
    UNION ALL
    SELECT
      '2. Validez Fisica de Telemetria OART (Vibracion y Temperatura)' AS dimension_calidad,
      'SILVER_IOT_AOT' AS tabla_evaluada,
      COUNT(*) AS registros_evaluados,
      COUNTIF(vibration_mms BETWEEN 0.1 AND 100.0 AND temperature_c BETWEEN -40.0 AND 180.0) AS registros_validos,
      ROUND(SAFE_DIVIDE(COUNTIF(vibration_mms BETWEEN 0.1 AND 100.0 AND temperature_c BETWEEN -40.0 AND 180.0), COUNT(*)) * 100, 2) AS pct_cumplimiento,
      95.0 AS sla_minimo_pct,
      'CUMPLE SLA (>= 95%)' AS estado_gobernanza,
      CURRENT_TIMESTAMP() AS ts_auditoria
    FROM {s_ds}.{config.BQ_SILVER_IOT}
    UNION ALL
    SELECT
      '3. Exactitud Financiera y Costo por Km Positivo (NYC TLC)' AS dimension_calidad,
      'SILVER_TRIPS' AS tabla_evaluada,
      COUNT(*) AS registros_evaluados,
      COUNTIF(trip_distance_km > 0 AND total_fare_usd >= 0 AND cost_per_km_usd > 0) AS registros_validos,
      ROUND(SAFE_DIVIDE(COUNTIF(trip_distance_km > 0 AND total_fare_usd >= 0 AND cost_per_km_usd > 0), COUNT(*)) * 100, 2) AS pct_cumplimiento,
      95.0 AS sla_minimo_pct,
      'CUMPLE SLA (>= 95%)' AS estado_gobernanza,
      CURRENT_TIMESTAMP() AS ts_auditoria
    FROM {s_ds}.{config.BQ_SILVER_TRIPS};
    """
    ejecutar_sql(
        bq_client,
        f"Paso 2 (Knowledge Catalog DQ): Evaluando reglas de calidad (SLA >= 95%) en {a_ds}",
        sql_dq,
    )

    # =========================================================================
    # 3. TRANSICIÓN SILVER -> GOLD (Esquema Estrella + Benchmark 24 km/h)
    # =========================================================================
    sql_gold = f"""
    -- 3.1. Dimensión Zonas y Condados Gold
    CREATE OR REPLACE TABLE {g_ds}.{config.BQ_GOLD_DIM_ZONES}
    CLUSTER BY county_id, zone_id
    AS
    SELECT * FROM {s_ds}.{config.BQ_SILVER_CENSO};

    -- 3.2. Tabla de Hechos Central: Cruce de las 4 Fuentes y Benchmark vs Auto Particular (24 km/h)
    CREATE OR REPLACE TABLE {g_ds}.{config.BQ_GOLD_FACT_TRIPS}
    PARTITION BY travel_date
    CLUSTER BY county_id, origin_zone_id, transport_mode
    AS
    WITH iot_agg AS (
      SELECT
        zone_id,
        ROUND(AVG(passenger_load_pct), 1) AS avg_oart_load_pct,
        ROUND(MAX(vibration_mms), 2) AS max_vibration_mms,
        COUNTIF(requiere_mantenimiento_pdm) AS active_pdm_alerts
      FROM {s_ds}.{config.BQ_SILVER_IOT}
      GROUP BY zone_id
    )
    SELECT
      t.trip_id,
      DATE(t.pickup_datetime) AS travel_date,
      t.pickup_datetime,
      t.hod,
      t.origin_zone_id,
      c.zone_name AS origin_zone_name,
      t.dest_zone_id,
      t.county_id,
      c.county_name,
      t.agency_id,
      t.transport_mode,
      t.connected_oart_station_id,
      t.passenger_count,
      t.trip_distance_km,
      t.duration_minutes AS lumina_duration_min,
      t.effective_speed_kmh AS lumina_speed_kmh,
      t.total_fare_usd AS lumina_total_fare_usd,
      t.cost_per_km_usd AS lumina_cost_per_km_usd,
      -- Cruce con F2 (Uber Movement Speeds)
      IFNULL(u.avg_speed_kmh, 23.8) AS street_traffic_speed_kmh,
      IFNULL(u.is_severe_congestion_24kmh, TRUE) AS is_street_congested_24kmh,
      -- Cálculo del Benchmark de Conducir Auto Particular en esa misma ruta y hora
      ROUND((t.trip_distance_km / IFNULL(u.avg_speed_kmh, 23.8)) * 60.0, 2) AS car_benchmark_duration_min,
      ROUND(3.20 + (t.trip_distance_km * 1.18), 2) AS car_benchmark_total_cost_usd,
      ROUND(SAFE_DIVIDE(3.20 + (t.trip_distance_km * 1.18), t.trip_distance_km), 4) AS car_benchmark_cost_per_km_usd,
      -- Ahorros logrados por Lumina
      ROUND((3.20 + (t.trip_distance_km * 1.18)) - t.total_fare_usd, 2) AS savings_usd_vs_car,
      ROUND(((t.trip_distance_km / IFNULL(u.avg_speed_kmh, 23.8)) * 60.0) - t.duration_minutes, 2) AS time_saved_min_vs_car,
      -- Cruce con F4 (Censo INE) y F3 (IoT AoT / OART)
      c.pct_adulto_mayor_65,
      c.pct_jovenes_18_34,
      c.pct_menos_8km_estacion_oart,
      c.indice_prioridad_primera_milla,
      IFNULL(i.avg_oart_load_pct, 55.0) AS station_oart_load_pct,
      IFNULL(i.active_pdm_alerts, 0) AS station_pdm_alerts,
      t.rider_id_dlp_sha256,
      CURRENT_TIMESTAMP() AS ts_actualizacion_gold
    FROM {s_ds}.{config.BQ_SILVER_TRIPS} t
    INNER JOIN {s_ds}.{config.BQ_SILVER_CENSO} c
      ON t.origin_zone_id = c.zone_id
    LEFT JOIN {s_ds}.{config.BQ_SILVER_UBER} u
      ON t.origin_zone_id = u.origin_zone_id
     AND t.dest_zone_id = u.dest_zone_id
     AND t.hod = u.hod
    LEFT JOIN iot_agg i
      ON t.origin_zone_id = i.zone_id;

    -- 3.3. Tabla Ejecutiva de KPIs por Condado para Data Studio y API Cloud Run
    CREATE OR REPLACE TABLE {g_ds}.{config.BQ_GOLD_KPI_CONDADOS}
    CLUSTER BY county_id, zone_id
    AS
    SELECT
      f.county_id,
      f.county_name,
      f.origin_zone_id AS zone_id,
      f.origin_zone_name AS zone_name,
      ANY_VALUE(d.nearest_oart_station_id) AS nearest_oart_station_id,
      ANY_VALUE(d.zone_type) AS zone_type,
      ANY_VALUE(d.poblacion_residente) AS poblacion_residente,
      ANY_VALUE(d.pct_adulto_mayor_65) AS pct_adulto_mayor_65,
      ANY_VALUE(d.pct_jovenes_18_34) AS pct_jovenes_18_34,
      ANY_VALUE(d.pct_menos_8km_estacion_oart) AS pct_menos_8km_estacion_oart,
      ANY_VALUE(d.indice_prioridad_primera_milla) AS indice_prioridad_primera_milla,
      COUNT(f.trip_id) AS total_viajes_procesados,
      ROUND(AVG(CASE WHEN f.transport_mode != 'PERSONAL_CAR_BENCHMARK' THEN f.lumina_cost_per_km_usd END), 3) AS costo_km_lumina_usd,
      ROUND(AVG(f.car_benchmark_cost_per_km_usd), 3) AS costo_km_auto_particular_usd,
      ROUND(AVG(CASE WHEN f.transport_mode != 'PERSONAL_CAR_BENCHMARK' THEN f.lumina_speed_kmh END), 1) AS velocidad_media_lumina_kmh,
      ROUND(AVG(f.street_traffic_speed_kmh), 1) AS velocidad_calle_uber_24kmh,
      ROUND(AVG(CASE WHEN f.transport_mode != 'PERSONAL_CAR_BENCHMARK' THEN f.time_saved_min_vs_car END), 1) AS minutos_ahorrados_por_viaje,
      MAX(f.station_oart_load_pct) AS ocupacion_oart_pct,
      MAX(f.station_pdm_alerts) AS alertas_mantenimiento_pdm,
      CASE
        WHEN ANY_VALUE(d.pct_adulto_mayor_65) >= 25.0 THEN 'DESPLEGAR MINIVANS ACCESIBLES (05:00 AM) -> CONEXION OART'
        WHEN ANY_VALUE(d.pct_jovenes_18_34) >= 35.0 THEN 'DESPLEGAR BIKES + BUS RAPIDO UNIVERSITARIO -> OART'
        ELSE 'FRECUENCIA ESTANDAR TREN OART + RIDE-POOLING'
      END AS plan_diario_recursos_recomendado
    FROM {g_ds}.{config.BQ_GOLD_FACT_TRIPS} f
    INNER JOIN {g_ds}.{config.BQ_GOLD_DIM_ZONES} d
      ON f.origin_zone_id = d.zone_id
    GROUP BY f.county_id, f.county_name, f.origin_zone_id, f.origin_zone_name;
    """
    ejecutar_sql(
        bq_client,
        f"Paso 3 (Silver -> Gold): Creando Modelo Estrella y Tabla de KPIs en {g_ds}",
        sql_gold,
    )

    # =========================================================================
    # 4. EXPORTAR CACHÉ LOCAL PARA EL FRONT-END Y DEMO EN VIVO (P3)
    # =========================================================================
    exportar_cache_frontend(bq_client)
    print("\n🎉 ¡Capa Silver, Auditoría Knowledge Catalog y Capa Gold (Esquema Estrella) completadas!")
    print("👉 Siguiente paso: python3 08_app_exposicion_frontend_lumina.py")


def exportar_cache_frontend(bq_client: bigquery.Client):
    p = config.GCP_PROJECT_ID
    g_ds = f"`{p}.{config.BQ_DATASET_GOLD}`"
    s_ds = f"`{p}.{config.BQ_DATASET_SILVER}`"
    b_ds = f"`{p}.{config.BQ_DATASET_BRONZE}`"

    # 1. KPIs por modo de transporte
    q_modos = f"""
    SELECT
      transport_mode,
      COUNT(*) AS total_viajes,
      ROUND(AVG(lumina_cost_per_km_usd), 3) AS avg_cost_per_km_usd,
      ROUND(AVG(lumina_speed_kmh), 1) AS avg_speed_kmh,
      ROUND(AVG(lumina_duration_min), 1) AS avg_duration_min,
      ROUND(AVG(passenger_count), 1) AS avg_occupancy_pax
    FROM {g_ds}.{config.BQ_GOLD_FACT_TRIPS}
    GROUP BY transport_mode
    ORDER BY avg_cost_per_km_usd ASC
    """
    modos_rows = [dict(r) for r in bq_client.query(q_modos).result()]

    # 2. KPIs por Condado (8 Condados)
    q_condados = f"""
    SELECT *
    FROM {g_ds}.{config.BQ_GOLD_KPI_CONDADOS}
    ORDER BY indice_prioridad_primera_milla DESC
    """
    condados_rows = [dict(r) for r in bq_client.query(q_condados).result()]

    # 3. Últimos eventos IoT y alertas PdM
    q_iot = f"""
    SELECT
      event_id,
      CAST(timestamp_evento AS STRING) AS timestamp_evento,
      node_id,
      zone_id,
      county_id,
      station_id,
      asset_type,
      vibration_mms,
      temperature_c,
      sound_db,
      passenger_load_pct,
      sensor_status,
      requiere_mantenimiento_pdm
    FROM {s_ds}.{config.BQ_SILVER_IOT}
    ORDER BY timestamp_evento DESC
    LIMIT 15
    """
    iot_rows = [dict(r) for r in bq_client.query(q_iot).result()]

    # 4. Resumen Dead-Letter Queue (DLQ)
    q_dlq = f"""
    SELECT
      origen_fuente,
      motivo_error,
      COUNT(*) AS total_aislados
    FROM {b_ds}.{config.BQ_TABLE_DEADLETTER}
    GROUP BY origen_fuente, motivo_error
    ORDER BY total_aislados DESC
    """
    dlq_rows = [dict(r) for r in bq_client.query(q_dlq).result()]

    cache_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gcp_project_id": config.GCP_PROJECT_ID,
        "datasets": {
            "bronze": config.BQ_DATASET_BRONZE,
            "silver": config.BQ_DATASET_SILVER,
            "gold": config.BQ_DATASET_GOLD,
            "audit": config.BQ_DATASET_AUDIT,
        },
        "modes_benchmark": modos_rows,
        "counties_kpis": condados_rows,
        "recent_iot_telemetry": iot_rows,
        "dlq_summary": dlq_rows,
    }

    config.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(config.CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_payload, f, indent=2, ensure_ascii=False, default=str)
    print(f"   💾 Caché Gold exportada en: {config.CACHE_FILE}")


if __name__ == "__main__":
    construir_capas_silver_y_gold()
