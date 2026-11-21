"""Gold sin tarifas supuestas, con cobertura temporal y referencia observada del auto."""
from Avance1_P1_Lumina_Ingesta_Storage.existing_project_data import validate_table_id


def build_observed_gold(project, dataset):
    prefix = validate_table_id(f'{project}.{dataset}.fact_movilidad_multimodal_gold').rsplit('.', 1)[0]
    return f'''CREATE OR REPLACE TABLE `{prefix}.fact_movilidad_multimodal_gold`
    PARTITION BY travel_date CLUSTER BY origin_zone_id,transport_mode AS
    WITH iot AS (
      SELECT zone_id,station_id,DATE(timestamp_evento) AS event_date,
        EXTRACT(HOUR FROM timestamp_evento) AS event_hour,
        AVG(passenger_load_pct) AS station_oart_load_pct,
        COUNTIF(requiere_mantenimiento_pdm) AS station_pdm_alerts
      FROM `{prefix}.iot_aot_telemetry_silver` GROUP BY 1,2,3,4
    ), cars AS (
      SELECT origin_zone_id,dest_zone_id,DATE(pickup_datetime) AS travel_date,
        EXTRACT(HOUR FROM pickup_datetime) AS hod,COUNT(*) AS reference_rows,
        AVG(total_fare_usd) AS reference_fare_usd,AVG(duration_minutes) AS reference_duration_min
      FROM `{prefix}.tlc_trips_silver` WHERE transport_mode='PERSONAL_CAR_BENCHMARK'
      GROUP BY 1,2,3,4
    )
    SELECT t.trip_id,DATE(t.pickup_datetime) AS travel_date,t.pickup_datetime,
      EXTRACT(HOUR FROM t.pickup_datetime) AS hod,t.origin_zone_id,c.zone_name AS origin_zone_name,
      t.dest_zone_id,t.county_id,c.county_name,t.agency_id,t.transport_mode,
      t.connected_oart_station_id,t.passenger_count,t.trip_distance_km,
      t.duration_minutes AS lumina_duration_min,t.effective_speed_kmh AS lumina_speed_kmh,
      t.total_fare_usd AS lumina_total_fare_usd,
      SAFE_DIVIDE(t.total_fare_usd,t.trip_distance_km) AS revenue_per_km_usd,
      u.avg_speed_kmh AS street_traffic_speed_kmh,
      u.is_severe_congestion_24kmh AS is_street_congested_24kmh,
      c.pct_adulto_mayor_65,c.pct_jovenes_18_34,c.pct_menos_8km_estacion_oart,
      c.indice_prioridad_primera_milla,i.station_oart_load_pct,i.station_pdm_alerts,
      u.movement_id IS NOT NULL AS traffic_match_found,i.zone_id IS NOT NULL AS iot_match_found,
      r.reference_rows,r.reference_fare_usd,r.reference_duration_min,
      r.reference_fare_usd-t.total_fare_usd AS observed_fare_difference_usd,
      r.reference_duration_min-t.duration_minutes AS observed_duration_difference_min,
      'PERSONAL_CAR_ROWS_SAME_OD_DATE_HOUR_NOT_CAUSAL' AS benchmark_source,
      CURRENT_TIMESTAMP() AS ts_actualizacion_gold
    FROM `{prefix}.tlc_trips_silver` t
    LEFT JOIN `{prefix}.dim_geografia_censo_silver` c ON t.origin_zone_id=c.zone_id
    LEFT JOIN `{prefix}.uber_movement_silver` u ON t.origin_zone_id=u.origin_zone_id
      AND t.dest_zone_id=u.dest_zone_id AND DATE(t.pickup_datetime)=u.travel_date
      AND EXTRACT(HOUR FROM t.pickup_datetime)=u.hod
    LEFT JOIN iot i ON t.origin_zone_id=i.zone_id AND t.connected_oart_station_id=i.station_id
      AND DATE(t.pickup_datetime)=i.event_date AND EXTRACT(HOUR FROM t.pickup_datetime)=i.event_hour
    LEFT JOIN cars r ON t.origin_zone_id=r.origin_zone_id AND t.dest_zone_id=r.dest_zone_id
      AND DATE(t.pickup_datetime)=r.travel_date AND EXTRACT(HOUR FROM t.pickup_datetime)=r.hod'''


def assert_isolated_writes(sql, project, dataset):
    import re
    if not re.fullmatch(r'lumina_validacion_[a-z0-9_]+', dataset):
        raise ValueError('Solo se permite escribir en un dataset nuevo de validación')
    destinations = re.findall(r'CREATE\s+OR\s+REPLACE\s+TABLE\s+`([^`]+)`', sql, re.I)
    if not destinations or any(not table.startswith(f'{project}.{dataset}.') for table in destinations):
        raise ValueError('El SQL intenta escribir fuera del dataset aislado')
    if re.search(r'\b(?:INSERT|DELETE|MERGE|DROP|TRUNCATE)\b', sql, re.I):
        raise ValueError('Operación de mutación no permitida en la validación')
