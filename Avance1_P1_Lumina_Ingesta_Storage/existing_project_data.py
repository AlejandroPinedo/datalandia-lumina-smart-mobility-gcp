"""Consultas de solo lectura sobre tablas existentes; sin generadores ni valores suplentes."""
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import time


def validate_table_id(value):
    if not re.fullmatch(r'[a-z][a-z0-9-]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',value):
        raise ValueError('Se requiere project.dataset.table sin SQL adicional')
    return value


def build_query(table_id, columns):
    validate_table_id(table_id)
    names=set(columns)
    def pick(*candidates, required=True):
        found=next((n for n in candidates if n in names),None)
        if not found and required:
            raise ValueError('La fuente no contiene una columna requerida: '+', '.join(candidates))
        return f'`{found}`' if found else None
    mode=pick('transport_mode','modo_transporte',required=False)
    distance=pick('trip_distance_km','distancia_km')
    amount=pick('lumina_total_fare_usd','total_fare_usd','costo_total_usd','total_amount')
    duration=pick('lumina_duration_min','duration_minutes','duracion_min',required=False)
    passengers=pick('passenger_count','pasajeros',required=False)
    traffic=pick('traffic_match_found',required=False)
    iot=pick('iot_match_found',required=False)
    # Una etiqueta de ausencia de desglose no se presenta como un modo observado.
    mode_expr=mode or "'SIN_DESGLOSE_EN_FUENTE'"
    avg_duration=f'AVG({duration})' if duration else 'CAST(NULL AS FLOAT64)'
    avg_passengers=f'AVG({passengers})' if passengers else 'CAST(NULL AS FLOAT64)'
    traffic_count=f'COUNTIF({traffic})' if traffic else 'CAST(NULL AS INT64)'
    iot_count=f'COUNTIF({iot})' if iot else 'CAST(NULL AS INT64)'
    trip_id=pick('trip_id',required=False)
    unique=f'COUNT(DISTINCT {trip_id})' if trip_id else 'CAST(NULL AS INT64)'
    sql=f'''SELECT {mode_expr} AS transport_mode,COUNT(*) AS total_trips,
      {unique} AS unique_trip_ids,
      SUM({distance}) AS total_km,SUM({amount}) AS total_revenue_usd,
      SAFE_DIVIDE(SUM({amount}),SUM({distance})) AS revenue_per_km_usd,
      {avg_duration} AS avg_duration_min,{avg_passengers} AS avg_passengers,
      {traffic_count} AS traffic_matched_trips,{iot_count} AS iot_matched_trips,
      COUNTIF({distance} IS NULL) AS missing_distance_rows,
      COUNTIF({amount} IS NULL) AS missing_amount_rows,
      COUNTIF({distance} < 0) AS negative_distance_rows
    FROM `{table_id}` GROUP BY transport_mode ORDER BY transport_mode'''
    mapping={'mode':mode,'distance_km':distance,'amount_usd':amount,'duration_min':duration,
             'passengers':passengers,'traffic_match':traffic,'iot_match':iot,'trip_id':trip_id}
    return sql,mapping


def summarize(rows):
    # SUM conserva ausencia total; no convierte mediciones ausentes en ceros.
    def total(key, selected):
        values=[float(row[key]) for row in selected if row.get(key) is not None]
        if not values: return None
        if any(not math.isfinite(value) for value in values):
            raise ValueError('La fuente contiene totales no finitos')
        return round(sum(values),2)
    lumina=[row for row in rows if row.get('transport_mode')!='PERSONAL_CAR_BENCHMARK']
    return {'total_trips':sum(int(row['total_trips']) for row in rows),
            'total_km':total('total_km',rows),
            'total_revenue_usd':total('total_revenue_usd',lumina)}


def read_existing(client, table_id, job_config=None):
    start=time.monotonic()
    table_id=validate_table_id(table_id)
    table=client.get_table(table_id,timeout=20)
    columns=[field.name for field in table.schema]
    sql,mapping=build_query(table_id,columns)
    job=client.query(sql,job_config=job_config,location=table.location)
    rows=json.loads(json.dumps([dict(row) for row in job.result(timeout=120)],default=str))
    if not rows: raise ValueError('La tabla existente no devolvió filas; no se generan datos de respaldo')
    now=datetime.now(timezone.utc).isoformat()
    meta={'table_id':table_id,'table_type':table.table_type,'num_rows_at_inspection':table.num_rows,
          'num_bytes_at_inspection':table.num_bytes,'location':table.location,
          'modified':table.modified.isoformat() if table.modified else None,
          'description':table.description,'schema':[{'name':f.name,'type':f.field_type} for f in table.schema]}
    return {'status':'ok','backend':'bigquery_existing','live_query':True,
            'source':'bigquery:'+table_id,'data_origin':'existing_project_data',
            'provenance_status':'not_independently_verified',
            'run_id':'existing_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S'),
            'processed_at':meta['modified'],'queried_at':now,
            'summary':summarize(rows),'by_mode':rows,'source_table':meta,
            'column_mapping':mapping,'job_id':job.job_id,
            'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),
            'query_sql':sql,'latency_ms':round((time.monotonic()-start)*1000,2),
            'source_modified_by_this_process':False,
            'limitation':'Se usan los registros ya existentes; no se certifica su procedencia externa por el solo hecho de estar en BigQuery.'}
