"""SQL compartido por CLI y Airflow. Fechas y horas armonizadas en UTC."""
import config
from operational_sql import build_operational_sql


def table(dataset, name):
    return f"`{config.GCP_PROJECT_ID}.{dataset}.{name}`"


def build_dq_sql():
    """Auditoría SQL sobre Bronze y Silver; porcentaje sin redondear para decidir el SLA.

    La validez de origen evita ocultar descartes por haber filtrado Silver primero.
    Las tablas vacías son SIN_DATOS y bloquean la publicación.
    """
    trips = table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_TLC_TRIPS)
    silver = table(config.BQ_DATASET_SILVER, config.BQ_SILVER_TRIPS)
    census = table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)
    iot = table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_IOT_AOT)
    audit = table(config.BQ_DATASET_AUDIT, config.BQ_AUDIT_DQ_TABLE)
    return f"""
    CREATE OR REPLACE TABLE {audit} AS
    WITH reglas AS (
      SELECT '1. Integridad espacial' AS dimension_calidad, 'BRONZE_TRIPS' AS tabla_evaluada,
        COUNT(*) AS registros_evaluados,
        COUNTIF(origin_zone_id IN (SELECT zone_id FROM {census})
            AND dest_zone_id IN (SELECT zone_id FROM {census})) AS registros_validos
      FROM {trips}
      UNION ALL
      SELECT '2. Validez temporal y financiera', 'BRONZE_TRIPS', COUNT(*),
        COUNTIF(trip_distance_km > 0.2 AND total_fare_usd > 0 AND duration_minutes > 0.5
                AND dropoff_datetime > pickup_datetime)
      FROM {trips}
      UNION ALL
      SELECT '3. Validez fisica de telemetria', 'BRONZE_IOT', COUNT(*),
        COUNTIF(vibration_mms > 0 AND vibration_mms <= 100
                AND temperature_c BETWEEN -40 AND 180
                AND passenger_load_pct BETWEEN 0 AND 100
                AND station_id IS NOT NULL AND zone_id IS NOT NULL)
      FROM {iot}
      UNION ALL
      SELECT '4. Unicidad de viajes Silver', 'SILVER_TRIPS', COUNT(*), COUNT(DISTINCT trip_id)
      FROM {silver}
    ), porcentajes AS (
      SELECT *, SAFE_DIVIDE(registros_validos, registros_evaluados) * 100 AS pct_cumplimiento
      FROM reglas
    )
    SELECT *, 95.0 AS sla_minimo_pct,
      CASE WHEN registros_evaluados = 0 THEN 'SIN_DATOS'
           WHEN pct_cumplimiento >= 95 THEN 'CUMPLE SLA (>= 95%)'
           ELSE 'NO CUMPLE SLA' END AS estado_gobernanza,
      CURRENT_TIMESTAMP() AS ts_auditoria
    FROM porcentajes;
    """


def build_quality_gate_sql():
    audit = table(config.BQ_DATASET_AUDIT, config.BQ_AUDIT_DQ_TABLE)
    return f"""
    ASSERT (SELECT COUNT(*) = 4 AND COUNTIF(
      registros_evaluados > 0 AND pct_cumplimiento >= sla_minimo_pct) = 4
      FROM {audit}) AS 'Calidad insuficiente o sin datos: Gold no se publica';
    """


def build_silver_sql():
    return f"""
    -- 1.1. Dimensión Geográfica y Censal Silver (F4: Censo INE + Puntos GIS)
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)}
    CLUSTER BY county_id, zone_id
    AS
    SELECT
      zone_id,
      TRIM(zone_name) AS zone_name,
      county_id,
      TRIM(county_name) AS county_name,
      nearest_oart_station_id,
      zone_type,
      poblacion_residente AS poblacion_residente,
      viviendas_totales AS viviendas_totales,
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
    FROM {table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_CENSO_INE)}
    WHERE zone_id IS NOT NULL AND poblacion_residente >= 0 AND viviendas_totales >= 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY zone_id ORDER BY ts_ingesta_bq DESC) = 1;

    -- 1.2. Velocidades Viales Silver (F2: Uber Movement con umbral 24 km/h)
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_UBER)}
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
    FROM {table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_UBER_SPEEDS)}
    WHERE avg_speed_kmh BETWEEN 2.0 AND 130.0
      AND origin_zone_id IS NOT NULL
      AND dest_zone_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY origin_zone_id, dest_zone_id, travel_date, hod ORDER BY ts_ingesta_bq DESC) = 1;

    -- 1.3. Viajes Multimodales Silver (F1: NYC TLC limpio, deduplicado y hash de demo SHA-256)
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_TRIPS)}
    PARTITION BY DATE(pickup_datetime)
    CLUSTER BY county_id, origin_zone_id, transport_mode
    AS
    SELECT
      t.trip_id,
      t.pickup_datetime,
      t.dropoff_datetime,
      EXTRACT(HOUR FROM t.pickup_datetime) AS hod,
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
    FROM {table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_TLC_TRIPS)} t
    INNER JOIN {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)} c
      ON t.origin_zone_id = c.zone_id
    WHERE t.trip_distance_km > 0.2
      AND t.total_fare_usd >= 0.0
      AND t.duration_minutes > 0.5
      AND t.dropoff_datetime > t.pickup_datetime
      AND t.dest_zone_id IN (SELECT zone_id FROM {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY t.ts_ingesta_bq DESC) = 1;

    -- 1.4. Telemetría IoT y Trenes OART Silver (F3: Array of Things validado)
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_IOT)}
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
    FROM {table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_IOT_AOT)}
    WHERE vibration_mms > 0.0 AND vibration_mms <= 100.0
      AND temperature_c BETWEEN -40.0 AND 180.0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts_ingesta_bq DESC) = 1;
    """


def build_gold_sql():
    return f"""
    -- 3.1. Dimensión Zonas y Condados Gold
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_DIM_ZONES)}
    CLUSTER BY county_id, zone_id
    AS
    SELECT * FROM {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)};

    -- 3.2. Tabla de Hechos Central: Cruce de las 4 Fuentes y Benchmark vs Auto Particular (24 km/h)
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_FACT_TRIPS)}
    PARTITION BY travel_date
    CLUSTER BY county_id, origin_zone_id, transport_mode
    AS
    WITH iot_agg AS (
      SELECT
        zone_id, station_id,
        DATE(timestamp_evento) AS event_date,
        EXTRACT(HOUR FROM timestamp_evento) AS event_hour,
        ROUND(AVG(passenger_load_pct), 1) AS avg_oart_load_pct,
        ROUND(MAX(vibration_mms), 2) AS max_vibration_mms,
        COUNTIF(requiere_mantenimiento_pdm) AS active_pdm_alerts
      FROM {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_IOT)}
      GROUP BY zone_id, station_id, event_date, event_hour
    )
    SELECT
      t.trip_id,
      DATE(t.pickup_datetime) AS travel_date,
      t.pickup_datetime,
      EXTRACT(HOUR FROM t.pickup_datetime) AS hod,
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
      u.avg_speed_kmh AS street_traffic_speed_kmh,
      u.is_severe_congestion_24kmh AS is_street_congested_24kmh,
      -- Cálculo del Benchmark de Conducir Auto Particular en esa misma ruta y hora
      ROUND((t.trip_distance_km / u.avg_speed_kmh) * 60.0, 2) AS car_benchmark_duration_min,
      ROUND({config.CAR_FIXED_COST_USD} + (t.trip_distance_km * {config.CAR_VARIABLE_COST_USD_KM}), 2) AS car_benchmark_total_cost_usd,
      ROUND(SAFE_DIVIDE({config.CAR_FIXED_COST_USD} + (t.trip_distance_km * {config.CAR_VARIABLE_COST_USD_KM}), t.trip_distance_km), 4) AS car_benchmark_cost_per_km_usd,
      -- Ahorros logrados por Lumina
      ROUND(({config.CAR_FIXED_COST_USD} + (t.trip_distance_km * {config.CAR_VARIABLE_COST_USD_KM})) - t.total_fare_usd, 2) AS savings_usd_vs_car,
      ROUND(((t.trip_distance_km / u.avg_speed_kmh) * 60.0) - t.duration_minutes, 2) AS time_saved_min_vs_car,
      -- Cruce con F4 (Censo INE) y F3 (IoT AoT / OART)
      c.pct_adulto_mayor_65,
      c.pct_jovenes_18_34,
      c.pct_menos_8km_estacion_oart,
      c.indice_prioridad_primera_milla,
      i.avg_oart_load_pct AS station_oart_load_pct,
      i.active_pdm_alerts AS station_pdm_alerts,
      u.movement_id IS NOT NULL AS traffic_match_found,
      i.zone_id IS NOT NULL AS iot_match_found,
      'SUPUESTO_TARIFARIO_NO_OBSERVADO' AS benchmark_source,
      t.rider_id_dlp_sha256,
      CURRENT_TIMESTAMP() AS ts_actualizacion_gold
    FROM {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_TRIPS)} t
    INNER JOIN {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_CENSO)} c
      ON t.origin_zone_id = c.zone_id
    LEFT JOIN {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_UBER)} u
      ON t.origin_zone_id = u.origin_zone_id
     AND t.dest_zone_id = u.dest_zone_id
     AND EXTRACT(HOUR FROM t.pickup_datetime) = u.hod
     AND DATE(t.pickup_datetime) = u.travel_date
    LEFT JOIN iot_agg i
      ON t.origin_zone_id = i.zone_id
     AND t.connected_oart_station_id = i.station_id
     AND DATE(t.pickup_datetime) = i.event_date
     AND EXTRACT(HOUR FROM t.pickup_datetime) = i.event_hour;

    -- 3.3. Tabla Ejecutiva de KPIs por Condado para Data Studio y API Cloud Run
    CREATE OR REPLACE TABLE {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_KPI_CONDADOS)}
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
      COUNTIF(f.traffic_match_found) AS viajes_con_trafico_observado,
      COUNTIF(f.iot_match_found) AS viajes_con_iot_observado,
      ROUND(SAFE_DIVIDE(SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.lumina_total_fare_usd, NULL)),
           SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.trip_distance_km, NULL))), 3) AS ingreso_km_lumina_usd,
      CAST(NULL AS FLOAT64) AS espera_media_min_no_disponible,
      CAST(NULL AS FLOAT64) AS utilizacion_flota_pct_no_disponible,
      ROUND(SAFE_DIVIDE(SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.lumina_total_fare_usd, NULL)),
          SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.trip_distance_km, NULL))), 3) AS costo_km_lumina_usd,
      ROUND(SAFE_DIVIDE(SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.car_benchmark_total_cost_usd, NULL)),
          SUM(IF(f.transport_mode != 'PERSONAL_CAR_BENCHMARK', f.trip_distance_km, NULL))), 3) AS costo_km_auto_particular_usd,
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
    FROM {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_FACT_TRIPS)} f
    INNER JOIN {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_DIM_ZONES)} d
      ON f.origin_zone_id = d.zone_id
    GROUP BY f.county_id, f.county_name, f.origin_zone_id, f.origin_zone_name;
    """ + build_operational_sql()
