"""Demo reproducible CSV/IoT -> Bronze -> Silver -> Gold -> API HTTP -> consumidor.

Por defecto ejecuta SQLite sin credenciales. Cloud se activa solo con --backend=bigquery.
Usa datos sintéticos y un dataset/archivo aislado por ejecución; no altera Gold histórico.
"""
import argparse
import contextlib
import csv
from datetime import datetime, timezone
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[1]
CODE = REPO / "Avance1_P1_Lumina_Ingesta_Storage"
sys.path.insert(0,str(CODE))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import config
from telemetry import validar_y_enriquecer_evento
from olap_sql import statements

NUMERIC = {'hod','pulocation_id','dolocation_id','passenger_count','poblacion_residente','viviendas_totales','passenger_load_pct'}
FLOATS = {'trip_distance_km','duration_minutes','effective_speed_kmh','base_fare_usd','total_fare_usd','cost_per_km_usd',
          'distance_corridor_km','mean_travel_time_sec','stddev_travel_time_sec','avg_speed_kmh',
          'pct_adulto_mayor_65','pct_jovenes_18_34','ingreso_medio_hogar_usd','pct_hogares_sin_auto',
          'pct_menos_8km_estacion_oart','indice_prioridad_primera_milla','center_lat','center_lon',
          'lat','lon','vibration_mms','temperature_c','sound_db','pm25_ugm3'}


def source_records(directory):
    generator = importlib.import_module('02_ingesta_batch_multifuente')
    original = generator.DATA_DIR
    generator.DATA_DIR = directory
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            files = {'p3_bronze_census': generator.generar_censo_demografia_ine(),
                     'p3_bronze_speeds': generator.generar_uber_movement_speeds(),
                     'p3_bronze_trips': generator.generar_nyc_tlc_trips_multimodal(600)}
    finally:
        generator.DATA_DIR = original
    result, hashes = {}, {}
    for name,path in files.items():
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        with path.open(encoding='utf-8',newline='') as source:
            rows = list(csv.DictReader(source))
        for row in rows:
            for field,value in list(row.items()):
                if field in NUMERIC: row[field] = int(value)
                elif field in FLOATS: row[field] = float(value)
        result[name] = rows
    # Casos visibles de limpieza y deduplicación.
    result['p3_bronze_trips'].append(dict(result['p3_bronze_trips'][0]))
    invalid = dict(result['p3_bronze_trips'][1])
    invalid.update(trip_id='DEMO_INVALID_NEGATIVE_DISTANCE',trip_distance_km=-1)
    result['p3_bronze_trips'].append(invalid)
    day = str(result['p3_bronze_trips'][0]['pickup_datetime'])[:10]
    valid, rejected = [], []
    for z in config.DATALANDIA_ZONES:
        for hour in [7,8,9,12,13,17,18,19,21,22]:
            raw = dict(event_id=f"P3-{z['zone_id']}-{hour}",node_id=f"P3-NODE-{z['zone_id']}",
                       zone_id=z['zone_id'],county_id=z['county_id'],station_id=z['nearest_oart_station_id'],
                       asset_type='OART_RAIL_SWITCH',timestamp_evento=f'{day}T{hour:02d}:00:00+00:00',
                       lat=z['lat'],lon=z['lon'],vibration_mms=3.2,temperature_c=35.0,sound_db=60,
                       pm25_ugm3=12,passenger_load_pct=60)
            ok,row = validar_y_enriquecer_evento(json.dumps(raw))
            (valid if ok else rejected).append(row)
    for payload in ['{"node_id":"broken","vibration_mms":"abc"}', 'null']:
        ok,row = validar_y_enriquecer_evento(payload)
        if ok: raise AssertionError('La anomalía de demo no fue aislada')
        rejected.append(row)
    result['p3_bronze_iot'],result['p3_deadletter'] = valid,rejected
    return result, hashes


def save_local(records,database,run_id):
    with contextlib.closing(sqlite3.connect(database)) as db, db:
        for table,rows in records.items():
            columns = list(rows[0])
            types = ['INTEGER' if k in NUMERIC or isinstance(rows[0][k],bool) else 'REAL' if k in FLOATS else 'TEXT' for k in columns]
            definition = ','.join(f'"{k}" {t}' for k,t in zip(columns,types))
            db.execute(f'CREATE TABLE "{table}" ({definition})')
            db.executemany(f'INSERT INTO "{table}" VALUES ({",".join("?" for _ in columns)})',
                           [tuple(row[k] for k in columns) for row in rows])
        sql = statements()
        for command in sql: db.execute(command)
        meta = [('run_id',run_id),('data_origin','synthetic_generated_for_p3'),
                ('processed_at',datetime.now(timezone.utc).isoformat())]
        db.execute('CREATE TABLE p3_run_metadata(key TEXT,value TEXT)')
        db.executemany('INSERT INTO p3_run_metadata VALUES (?,?)',meta)
        count,unique = db.execute('SELECT COUNT(*),COUNT(DISTINCT trip_id) FROM p3_gold_fact').fetchone()
        if count != 600 or unique != 600: raise AssertionError('Se perdió el grano de viaje')
        rows = db.execute('SELECT SUM(traffic_match_found),SUM(iot_match_found) FROM p3_gold_fact').fetchone()
        return {'gold_rows':count,'unique_trip_ids':unique,'traffic_matched_trips':rows[0],
                'iot_matched_trips':rows[1],'sql_sha256':hashlib.sha256('\n'.join(sql).encode()).hexdigest()}


def save_cloud(records,run_id):
    from google.cloud import bigquery
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    dataset_name = run_id
    dataset = bigquery.Dataset(f'{config.GCP_PROJECT_ID}.{dataset_name}')
    dataset.location = config.GCP_LOCATION
    dataset.default_table_expiration_ms = 7 * 86400 * 1000
    dataset.description = 'P3 aislada: datos sintéticos. Tablas expiran en 7 días.'
    client.create_dataset(dataset)
    jobs = []
    query_config = bigquery.QueryJobConfig(maximum_bytes_billed=1073741824)
    for name,rows in records.items():
        job = client.load_table_from_json(rows,f'{dataset.project}.{dataset_name}.{name}',
                job_config=bigquery.LoadJobConfig(autodetect=True,write_disposition='WRITE_EMPTY'))
        job.result(); jobs.append(job.job_id)
    # Autodetect de JSON preserva TIMESTAMP solo si se le suministra el esquema.
    # Las columnas de fechas se convierten explícitamente en la carga de tablas.
    for name in ['p3_bronze_trips','p3_bronze_iot','p3_bronze_speeds']:
        fields = {'p3_bronze_trips':['pickup_datetime','dropoff_datetime'],
                  'p3_bronze_iot':['timestamp_evento'], 'p3_bronze_speeds':['travel_date']}[name]
        # JSON autodetect puede elegir STRING o TIMESTAMP; CAST funciona para ambos.
        conversions = ','.join(f'CAST({field} AS {"DATE" if field=="travel_date" else "TIMESTAMP"}) AS {field}' for field in fields)
        query = f'CREATE OR REPLACE TABLE `{dataset.project}.{dataset_name}.{name}` AS SELECT * REPLACE({conversions}) FROM `{dataset.project}.{dataset_name}.{name}`'
        job=client.query(query, job_config=query_config);job.result();jobs.append(job.job_id)
    sql=statements(f'{dataset.project}.{dataset_name}',bigquery=True)
    for command in sql:
        job=client.query(command, job_config=query_config);job.result();jobs.append(job.job_id)
    meta=[{'key':'run_id','value':run_id},{'key':'data_origin','value':'synthetic_generated_for_p3'},
          {'key':'processed_at','value':datetime.now(timezone.utc).isoformat()}]
    job=client.load_table_from_json(meta,f'{dataset.project}.{dataset_name}.p3_run_metadata',
            job_config=bigquery.LoadJobConfig(autodetect=True,write_disposition='WRITE_EMPTY'))
    job.result();jobs.append(job.job_id)
    query=f'SELECT COUNT(*) AS n,COUNT(DISTINCT trip_id) AS u,SUM(traffic_match_found) AS traffic,SUM(iot_match_found) AS iot FROM `{dataset.project}.{dataset_name}.p3_gold_fact`'
    job=client.query(query, job_config=query_config);row=next(iter(job.result()));jobs.append(job.job_id)
    if row['n']!=600 or row['u']!=600: raise AssertionError('Grano de viaje inválido en Gold cloud')
    os.environ['LUMINA_P3_DATASET']=dataset_name
    return {'gold_rows':row['n'],'unique_trip_ids':row['u'],'traffic_matched_trips':row['traffic'],
            'iot_matched_trips':row['iot'],'dataset':f'{dataset.project}.{dataset_name}', 'job_ids':jobs,
            'sql_sha256':hashlib.sha256('\n'.join(sql).encode()).hexdigest()}


def verify_http():
    api=importlib.import_module('08_app_exposicion_frontend_lumina')
    class Handler(api.StdlibLuminaHandler):
        def log_message(self,*args): pass
    server=api.HTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    prefix=f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with urllib.request.urlopen(prefix+'/api/v1/olap-kpis',timeout=30) as response:
            report=json.load(response)
            if report['status']!='ok' or report['summary']['total_trips']!=600:
                raise AssertionError('La API no expuso el Gold de esta ejecución')
        with urllib.request.urlopen(prefix+'/p3',timeout=5) as response:
            body=response.read().decode()
            if '/api/v1/olap-kpis' not in body: raise AssertionError('Consumidor no conectado a API')
        with urllib.request.urlopen(prefix+'/',timeout=5) as response:
            body=response.read().decode()
            if re.search(r'\{[{%]',body): raise AssertionError('Plantillas sin resolver')
        return report
    finally:
        server.shutdown();server.server_close();worker.join(timeout=3)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend',choices=['sqlite','bigquery'],default='sqlite')
    parser.add_argument('--serve',action='store_true',help='Dejar el tablero accesible tras verificar la demo')
    args=parser.parse_args()
    started=time.monotonic()
    run_id='p3_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]
    runtime=REPO/'.runtime'/'p3'/run_id;runtime.mkdir(parents=True)
    records,hashes=source_records(runtime)
    os.environ['LUMINA_DATA_BACKEND']=args.backend
    database=runtime/'olap.sqlite'
    metrics=save_local(records,database,run_id) if args.backend=='sqlite' else save_cloud(records,run_id)
    os.environ['LUMINA_SQLITE_PATH']=str(database)
    report=verify_http()
    evidence={'run_id':run_id,'backend':args.backend,'data_origin':'synthetic_generated_for_p3',
              'completed_at':datetime.now(timezone.utc).isoformat(),'elapsed_seconds':round(time.monotonic()-started,3),
              'input_sha256':hashes,'bronze_counts':{name:len(rows) for name,rows in records.items()},
              **metrics,'api_report':report,'verification':{'trip_grain':'passed','api_reads_gold':'passed',
              'consumer_references_api':'passed','legacy_template_resolved':'passed'},
              'browser_visual_review':'not_performed'}
    output=REPO/'P3'/'evidencias';output.mkdir(exist_ok=True)
    (output/f'{args.backend}_latest.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
    (runtime/'environment.json').write_text(json.dumps({'LUMINA_DATA_BACKEND':args.backend,
        'LUMINA_SQLITE_PATH':str(database),'LUMINA_P3_DATASET':os.getenv('LUMINA_P3_DATASET','')},indent=2),encoding='utf-8')
    print(json.dumps({'run_id':run_id,'backend':args.backend,'gold_rows':metrics['gold_rows'],
          'verification':evidence['verification'],'evidence':str(output/f'{args.backend}_latest.json')},indent=2))
    if args.serve:
        api=importlib.import_module('08_app_exposicion_frontend_lumina')
        print(f'Abre http://127.0.0.1:{config.FLASK_PORT}/p3')
        api.HTTPServer((config.FLASK_HOST,config.FLASK_PORT),api.StdlibLuminaHandler).serve_forever()


if __name__=='__main__': main()
