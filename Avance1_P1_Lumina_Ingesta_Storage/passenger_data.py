"""Alternativas y ejemplos de viajes observados en Gold. Solo lectura."""
import hashlib
import json
from datetime import datetime, timezone
from existing_project_data import validate_table_id


def build_passenger_query(table_id, columns):
    validate_table_id(table_id)
    required = {'trip_id', 'travel_date', 'pickup_datetime', 'origin_zone_id',
                'origin_zone_name', 'dest_zone_id', 'county_name', 'transport_mode',
                'trip_distance_km', 'lumina_duration_min', 'lumina_total_fare_usd',
                'connected_oart_station_id'}
    missing = required - set(columns)
    if missing:
        raise ValueError('Faltan columnas para consultar trayectos: ' + ', '.join(sorted(missing)))
    return f'''SELECT origin_zone_id, origin_zone_name, dest_zone_id, county_name, transport_mode,
      COUNT(*) AS total_trips,
      SUM(trip_distance_km) AS distance_sum, COUNT(trip_distance_km) AS distance_count,
      SUM(lumina_duration_min) AS duration_sum, COUNT(lumina_duration_min) AS duration_count,
      SUM(lumina_total_fare_usd) AS fare_sum, COUNT(lumina_total_fare_usd) AS fare_count,
      MIN(travel_date) AS first_date, MAX(travel_date) AS last_date,
      ARRAY_AGG(DISTINCT connected_oart_station_id IGNORE NULLS) AS station_ids,
      ARRAY_AGG(STRUCT(trip_id, travel_date, pickup_datetime, trip_distance_km,
        lumina_duration_min, lumina_total_fare_usd)
        ORDER BY pickup_datetime DESC, trip_id DESC LIMIT 3) AS recent_trips
    FROM `{table_id}`
    GROUP BY origin_zone_id, origin_zone_name, dest_zone_id, county_name, transport_mode
    ORDER BY origin_zone_id, dest_zone_id, transport_mode, county_name'''


def read_passenger_data(client, table_id, job_config=None):
    table_id = validate_table_id(table_id)
    table = client.get_table(table_id, timeout=20)
    sql = build_passenger_query(table_id, [field.name for field in table.schema])
    job = client.query(sql, job_config=job_config, location=table.location)
    def normalize(value):
        if hasattr(value, 'items'):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        return value
    rows = [normalize(row) for row in job.result(timeout=240)]
    if not rows:
        raise ValueError('Gold no contiene trayectos; no se generan alternativas de respaldo')
    total = sum(int(row['total_trips']) for row in rows)
    if table.num_rows is not None and total != table.num_rows:
        raise ValueError('El total por trayecto no coincide con los metadatos de Gold; vuelve a consultar')
    return {'status': 'ok', 'data_origin': 'existing_project_data',
            'source': 'bigquery:' + table_id, 'source_modified_by_this_process': False,
            'provenance_status': 'not_independently_verified', 'live_query': False,
            'queried_at': datetime.now(timezone.utc).isoformat(), 'job_id': job.job_id,
            'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(), 'query_sql': sql,
            'total_source_records': total, 'routes': rows,
            'trip_examples_scope': 'Hasta los 3 últimos registros por origen, destino, condado y modo; no es un historial personal ni el listado completo de Gold',
            'limitation': 'Alternativas históricas observadas. No son rutas en tiempo real, reservas, horarios ni cotizaciones. Las conexiones OART son atributos de Gold, no secuencias de paradas verificadas.'}
