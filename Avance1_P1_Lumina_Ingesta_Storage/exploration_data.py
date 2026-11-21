"""Desglose para el explorador; exclusivamente SELECT sobre la fuente existente."""
import hashlib
import json
from datetime import datetime, timezone
from existing_project_data import validate_table_id

DIMENSIONS = ['travel_date', 'county_name', 'transport_mode', 'hod']
METRICS = ['total_trips', 'total_km', 'total_revenue_usd', 'duration_sum',
           'duration_count', 'passengers_sum', 'passengers_count',
           'missing_distance_rows', 'missing_amount_rows', 'negative_distance_rows']


def build_exploration_query(table_id, columns):
    validate_table_id(table_id)
    required = set(DIMENSIONS + ['trip_distance_km', 'lumina_total_fare_usd',
                                'lumina_duration_min', 'passenger_count'])
    missing = required - set(columns)
    if missing:
        raise ValueError('Faltan columnas de exploración: ' + ', '.join(sorted(missing)))
    return f'''SELECT travel_date, county_name, transport_mode, hod,
      COUNT(*) AS total_trips, SUM(trip_distance_km) AS total_km,
      SUM(lumina_total_fare_usd) AS total_revenue_usd,
      SUM(lumina_duration_min) AS duration_sum, COUNT(lumina_duration_min) AS duration_count,
      SUM(passenger_count) AS passengers_sum, COUNT(passenger_count) AS passengers_count,
      COUNTIF(trip_distance_km IS NULL) AS missing_distance_rows,
      COUNTIF(lumina_total_fare_usd IS NULL) AS missing_amount_rows,
      COUNTIF(trip_distance_km < 0) AS negative_distance_rows
    FROM `{table_id}` GROUP BY travel_date, county_name, transport_mode, hod
    ORDER BY travel_date, county_name, transport_mode, hod'''


def pack_rows(rows):
    dictionaries = {key: [] for key in DIMENSIONS}
    indices = {key: {} for key in DIMENSIONS}
    packed = []
    for row in rows:
        item = []
        for key in DIMENSIONS:
            value = row[key]
            if value is not None and key == 'travel_date':
                value = str(value)
            if value not in indices[key]:
                indices[key][value] = len(dictionaries[key])
                dictionaries[key].append(value)
            item.append(indices[key][value])
        item.extend(row[key] for key in METRICS)
        packed.append(item)
    return {'columns': DIMENSIONS + METRICS, 'dictionaries': dictionaries, 'rows': packed}


def read_exploration(client, table, expected_count, job_config=None):
    sql = build_exploration_query(table.full_table_id.replace(':', '.'),
                                  [field.name for field in table.schema])
    job = client.query(sql, job_config=job_config, location=table.location)
    rows = [dict(row) for row in job.result(timeout=180)]
    if not rows or sum(int(row['total_trips']) for row in rows) != expected_count:
        raise ValueError('El desglose no coincide con el total consultado; volver a consultar la fuente')
    result = pack_rows(rows)
    result.update(status='ok', job_id=job.job_id,
                  queried_at=datetime.now(timezone.utc).isoformat(),
                  sql_sha256=hashlib.sha256(sql.encode()).hexdigest(), query_sql=sql,
                  source_modified_by_this_process=False)
    return json.loads(json.dumps(result, default=str))
