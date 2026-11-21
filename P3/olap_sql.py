"""SQL portable del proceso OLAP P3; las fechas de la demo son UTC ISO 8601."""


def statements(prefix="", bigquery=False):
    def name(value):
        return f"`{prefix}.{value}`" if prefix else f'"{value}"'
    date_trip = "DATE(pickup_datetime)" if bigquery else "substr(pickup_datetime,1,10)"
    hour_trip = "EXTRACT(HOUR FROM pickup_datetime)" if bigquery else "CAST(substr(pickup_datetime,12,2) AS INTEGER)"
    date_iot = "DATE(timestamp_evento)" if bigquery else "substr(timestamp_evento,1,10)"
    hour_iot = "EXTRACT(HOUR FROM timestamp_evento)" if bigquery else "CAST(substr(timestamp_evento,12,2) AS INTEGER)"
    return [
        f'''CREATE TABLE {name('p3_silver_zones')} AS
        SELECT * FROM (
          SELECT c.*, ROW_NUMBER() OVER(PARTITION BY zone_id ORDER BY ts_ingesta_bq DESC) AS rn
          FROM {name('p3_bronze_census')} c WHERE poblacion_residente >= 0
        ) ranked WHERE rn = 1''',
        f'''CREATE TABLE {name('p3_silver_trips')} AS
        SELECT * FROM (
          SELECT t.*, {date_trip} AS travel_date, {hour_trip} AS event_hour,
            ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY ts_ingesta_bq DESC) AS rn
          FROM {name('p3_bronze_trips')} t
          WHERE trip_distance_km > 0.2 AND duration_minutes > 0.5 AND total_fare_usd >= 0
            AND dropoff_datetime > pickup_datetime
            AND origin_zone_id IN (SELECT zone_id FROM {name('p3_silver_zones')})
            AND dest_zone_id IN (SELECT zone_id FROM {name('p3_silver_zones')})
        ) ranked WHERE rn = 1''',
        f'''CREATE TABLE {name('p3_silver_speeds')} AS
        SELECT * FROM (
          SELECT u.*, ROW_NUMBER() OVER(PARTITION BY origin_zone_id,dest_zone_id,travel_date,hod
                     ORDER BY ts_ingesta_bq DESC) AS rn
          FROM {name('p3_bronze_speeds')} u WHERE avg_speed_kmh > 0
        ) ranked WHERE rn = 1''',
        f'''CREATE TABLE {name('p3_silver_iot')} AS
        SELECT * FROM (
          SELECT i.*, ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY ts_ingesta_bq DESC) AS rn
          FROM {name('p3_bronze_iot')} i
        ) ranked WHERE rn = 1''',
        f'''CREATE TABLE {name('p3_gold_fact')} AS
        WITH iot AS (
          SELECT zone_id,station_id,{date_iot} AS travel_date,{hour_iot} AS event_hour,
            AVG(passenger_load_pct) AS passenger_load_pct, COUNT(*) AS matched_events
          FROM {name('p3_silver_iot')}
          GROUP BY zone_id,station_id,travel_date,event_hour
        ) SELECT t.trip_id,t.transport_mode,t.county_id,t.origin_zone_id,t.dest_zone_id,t.travel_date,
          t.trip_distance_km,t.total_fare_usd,t.duration_minutes,t.passenger_count,
          u.avg_speed_kmh AS street_speed_kmh,i.passenger_load_pct AS oart_load_pct,
          CASE WHEN u.movement_id IS NOT NULL THEN 1 ELSE 0 END AS traffic_match_found,
          CASE WHEN i.matched_events IS NOT NULL THEN 1 ELSE 0 END AS iot_match_found
        FROM {name('p3_silver_trips')} t
        JOIN {name('p3_silver_zones')} c ON t.origin_zone_id=c.zone_id
        LEFT JOIN {name('p3_silver_speeds')} u ON t.origin_zone_id=u.origin_zone_id
          AND t.dest_zone_id=u.dest_zone_id AND t.travel_date=u.travel_date AND t.event_hour=u.hod
        LEFT JOIN iot i ON t.origin_zone_id=i.zone_id AND t.connected_oart_station_id=i.station_id
          AND t.travel_date=i.travel_date AND t.event_hour=i.event_hour''',
        f'''CREATE TABLE {name('p3_gold_kpi')} AS
        SELECT transport_mode,COUNT(*) AS total_trips,ROUND(SUM(trip_distance_km),2) AS total_km,
          ROUND(SUM(total_fare_usd),2) AS total_revenue_usd,
          ROUND(SUM(total_fare_usd)/NULLIF(SUM(trip_distance_km),0),4) AS revenue_per_km_usd,
          ROUND(AVG(duration_minutes),2) AS avg_duration_min,
          ROUND(AVG(passenger_count),2) AS avg_passengers,
          SUM(traffic_match_found) AS traffic_matched_trips,SUM(iot_match_found) AS iot_matched_trips
        FROM {name('p3_gold_fact')} GROUP BY transport_mode''',
    ]
