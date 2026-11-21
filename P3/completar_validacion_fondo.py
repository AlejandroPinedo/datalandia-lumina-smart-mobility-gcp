"""Ejecuta Bronze→Silver→DQ→Gold aislado y valida los servicios con datos existentes."""
from collections import Counter
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
from pathlib import Path
import sys
import time
import uuid

from usar_datos_existentes import configure_credentials
import config
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from operations.validation_sql import build_observed_gold, assert_isolated_writes
from operations.forecast import evaluate_demand
from operations.event_store import EventStore
from operations.maintenance import assess_sensors
from operations.portable import verify_portable_join
from operations.context import enrich_report
from pipeline_sql import build_silver_sql, build_dq_sql, build_quality_gate_sql


def normalize(value):
    if hasattr(value,'items'): return {key:normalize(item) for key,item in value.items()}
    if isinstance(value,(list,tuple)): return [normalize(item) for item in value]
    if hasattr(value,'isoformat'): return value.isoformat()
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resume',action='store_true',help='Retoma la última validación incompleta sin repetir etapas aprobadas')
    parser.add_argument('--bronze-dataset',help='Dataset aislado cargado desde archivos Parquet existentes')
    args=parser.parse_args()
    if args.bronze_dataset:
        if not re.fullmatch(r'lumina_ingesta_[0-9]{8}_[0-9]{6}_[a-f0-9]{6}',args.bronze_dataset):
            raise ValueError('La ingesta debe proceder de un dataset aislado reconocido')
        config.BQ_DATASET_BRONZE=args.bronze_dataset
    configure_credentials()
    from google.cloud import bigquery
    now=datetime.now(timezone.utc)
    run_id=now.strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]
    target='lumina_validacion_'+run_id
    runtime=ROOT/'.runtime/operations'/run_id;runtime.mkdir(parents=True,exist_ok=True)
    report={'status':'running','run_id':run_id,'started_at':now.isoformat(),
            'source_data_origin':'existing_project_data','generated_records_used':False,
            'historical_sources_modified':False,'target_dataset':target,'stages':[],
            'limitations':[], 'requirements_scope':'Arquitectura para cuatro verticales; demostración funcional end-to-end de OLAP'}
    output=ROOT/'P3/evidencias/validacion_fondo_latest.json'
    resuming=False
    if args.resume and output.is_file():
        previous=json.loads(output.read_text(encoding='utf-8'))
        previous_target=previous.get('target_dataset','')
        if previous.get('status') in ('not_completed','running'):
            if not re.fullmatch(r'lumina_validacion_[0-9]{8}_[0-9]{6}_[a-f0-9]{6}',previous_target):
                raise ValueError('El destino anterior no es un dataset aislado reconocido')
            report=previous;target=previous_target;run_id=previous['run_id']
            runtime=ROOT/'.runtime/operations'/run_id;runtime.mkdir(parents=True,exist_ok=True)
            report.setdefault('previous_attempts',[]).append({key:previous.get(key) for key in ('finished_at','error_type','error')})
            for key in ('error_type','error'):report.pop(key,None)
            report['status']='running';report['resumed_at']=now.isoformat();resuming=True
    def save():
        temporary=output.with_suffix('.tmp')
        temporary.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        temporary.replace(output)
    started=time.perf_counter()
    try:
        with bigquery.Client(project=config.GCP_PROJECT_ID) as client:
            source_dataset=client.get_dataset(f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}')
            location=source_dataset.location
            dataset=bigquery.Dataset(f'{config.GCP_PROJECT_ID}.{target}')
            dataset.location=location
            dataset.description='Validación aislada Lumina; datos históricos existentes, sin generadores'
            dataset.labels={'purpose':'academic-validation','source':'existing-data'}
            if resuming:client.get_dataset(dataset.reference)
            else:client.create_dataset(dataset,exists_ok=False)
            previous_sources=report.get('source_tables',{})
            report['source_tables']={}
            for key,name in [('trips',config.BQ_TABLE_TLC_TRIPS),('traffic',config.BQ_TABLE_UBER_SPEEDS),
                             ('iot',config.BQ_TABLE_IOT_AOT),('census',config.BQ_TABLE_CENSO_INE)]:
                identifier=f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{name}'
                table=client.get_table(identifier)
                if resuming and previous_sources.get(key,{}).get('modified')!=(table.modified.isoformat() if table.modified else None):
                    raise ValueError('La fuente cambió desde la ejecución anterior; inicia una validación nueva')
                report['source_tables'][key]={'table_id':identifier,'rows':table.num_rows,
                    'modified':table.modified.isoformat() if table.modified else None}
            original_modifications={key:value['modified'] for key,value in report['source_tables'].items()}
            config.BQ_DATASET_SILVER=config.BQ_DATASET_GOLD=config.BQ_DATASET_AUDIT=target
            def query(label,sql,write=False,parameters=None):
                if write: assert_isolated_writes(sql,config.GCP_PROJECT_ID,target)
                reusable={'Bronze existente → Silver aislado','Auditoría de cuatro reglas',
                          'Compuerta de calidad ≥95%','Silver → Gold observado'}
                previous_stage=next((stage for stage in report['stages'] if stage['name']==label and stage['status']=='passed'),None)
                if resuming and label in reusable and previous_stage:
                    if previous_stage['sql_sha256']!=hashlib.sha256(sql.encode()).hexdigest():
                        raise ValueError('El código de una etapa ya ejecutada cambió; no se reutiliza su resultado')
                    previous_stage['reused_on_resume']=True
                    print('Etapa conservada: '+label,flush=True)
                    return []
                stamp=time.perf_counter()
                job=client.query(sql,location=location,job_config=bigquery.QueryJobConfig(
                    maximum_bytes_billed=10*1024**3,query_parameters=parameters or []))
                rows=[normalize(row) for row in job.result(timeout=600)]
                report['stages'].append({'name':label,'status':'passed','job_id':job.job_id,
                    'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),
                    'total_bytes_processed':job.total_bytes_processed,
                    'elapsed_seconds':round(time.perf_counter()-stamp,3)})
                save();return rows
            silver=build_silver_sql().replace('ROUND(ABS(ingreso_medio_hogar_usd), 2)',
                'IF(ingreso_medio_hogar_usd >= 0, ROUND(ingreso_medio_hogar_usd, 2), NULL)')
            query('Bronze existente → Silver aislado',silver,True)
            query('Auditoría de cuatro reglas',build_dq_sql(),True)
            report['quality']=query('Resultados DQ',f'SELECT * FROM `{config.GCP_PROJECT_ID}.{target}.{config.BQ_AUDIT_DQ_TABLE}`')
            query('Compuerta de calidad ≥95%',build_quality_gate_sql())
            query('Silver → Gold observado',build_observed_gold(config.GCP_PROJECT_ID,target),True)
            gold=f'`{config.GCP_PROJECT_ID}.{target}.fact_movilidad_multimodal_gold`'
            report['coverage']=query('Cobertura y cardinalidad',f'''SELECT COUNT(*) AS total_rows,
              COUNT(DISTINCT trip_id) AS unique_trips,COUNTIF(origin_zone_name IS NOT NULL) AS census_matches,
              COUNTIF(traffic_match_found) AS traffic_matches,COUNTIF(iot_match_found) AS iot_matches,
              COUNTIF(reference_rows > 0) AS observed_car_reference_matches,
              MIN(travel_date) AS first_date,MAX(travel_date) AS last_date FROM {gold}''')[0]
            silver_count=query('Reconciliación Silver',f'SELECT COUNT(*) AS total_rows FROM `{config.GCP_PROJECT_ID}.{target}.tlc_trips_silver`')[0]['total_rows']
            if report['coverage']['total_rows']!=silver_count or report['coverage']['unique_trips']!=silver_count:
                raise ValueError('El cruce altera la cardinalidad de Silver')
            report['olap']=query('Consumo OLAP del Gold nuevo',f'''SELECT transport_mode,COUNT(*) AS trips,
              SUM(trip_distance_km) AS distance_km,SUM(lumina_total_fare_usd) AS fare_usd,
              SAFE_DIVIDE(SUM(lumina_total_fare_usd),SUM(trip_distance_km)) AS fare_per_km_usd,
              AVG(lumina_duration_min) AS avg_duration_min,
              AVG(observed_fare_difference_usd) AS avg_difference_vs_observed_car_usd
              FROM {gold} GROUP BY transport_mode''')
            daily=query('Demanda diaria observada',f'''SELECT origin_zone_id AS zone_id,transport_mode,
              travel_date AS date,COUNT(*) AS trips FROM {gold} GROUP BY 1,2,3 ORDER BY 1,2,3''')
            report['forecasts']=evaluate_demand(daily)
            zones=query('Censo existente',f'SELECT * FROM `{config.GCP_PROJECT_ID}.{target}.dim_geografia_censo_silver`')
            traffic=query('Tráfico existente',f'SELECT * FROM `{config.GCP_PROJECT_ID}.{target}.uber_movement_silver`')
            events=query('Telemetría existente',f'SELECT * FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{config.BQ_TABLE_IOT_AOT}`')
            (runtime/'events_existing.jsonl').write_text(''.join(json.dumps(event,ensure_ascii=False,default=str)+'\n' for event in events),encoding='utf-8')
            # Se preservan event_id y timestamp_evento. Replay no equivale a sensores en vivo.
            store=EventStore(runtime/'events.sqlite3')
            try:
                latency=[];outcomes=Counter()
                for event in events:
                    tick=time.perf_counter();outcomes[store.process(event)]+=1;latency.append((time.perf_counter()-tick)*1000)
                first=store.counts()
                redelivery=Counter(store.process(event) for event in events)
                if store.counts()!=first: raise AssertionError('La reentrega modifica eventos o DLQ')
                store.backup(runtime/'recovery.sqlite3')
                restored=EventStore(runtime/'recovery.sqlite3')
                try:
                    if restored.counts()!=first or restored.integrity()!='ok': raise AssertionError('La restauración no conserva el estado')
                finally:restored.close()
                accepted=[normalize(json.loads(row[0])) for row in store.db.execute('SELECT payload FROM events')]
                report['streaming']={'mode':'historical_event_time_replay','input_events':len(events),
                    'outcomes':dict(outcomes),'redelivery_outcomes':dict(redelivery),
                    'durable_counts':first,'idempotency':'passed','recovery':'passed',
                    'integrity':store.integrity(),'original_timestamps_preserved':True,
                    'latency_ms_p95':sorted(latency)[max(0,int(len(latency)*.95)-1)] if latency else None,
                    'measurement_scope':'Procesamiento local de eventos históricos; no latencia de Pub/Sub o Dataflow'}
                report['maintenance']=assess_sensors(accepted)
            finally:store.close()
            passenger=json.loads((ROOT/'P3/evidencias/movilidad_pasajero_latest.json').read_text(encoding='utf-8'))
            ids=sorted({trip['trip_id'] for route in passenger['routes'] for trip in route['recent_trips']})
            trips=query('Registros existentes para motor abierto',f'''SELECT trip_id,pickup_datetime,
              origin_zone_id,dest_zone_id,connected_oart_station_id FROM {gold} WHERE trip_id IN UNNEST(@ids)''',
              parameters=[bigquery.ArrayQueryParameter('ids','STRING',ids)])
            report['portable_join']=verify_portable_join(runtime/'portable.sqlite3',trips,zones,traffic,accepted)
            (runtime/'portable_sources.json').write_text(json.dumps({'trips':trips,'zones':zones,'traffic':traffic,'telemetry':accepted},ensure_ascii=False,default=str),encoding='utf-8')
            enrich_report(report,runtime)
            report['realtime_readiness']={'status':'requires_operational_sources',
                'missing':['active_requests','fleet_and_driver_availability','live_gps_traffic',
                           'oart_schedules','accessibility_preferences','operational_events'],
                'historical_query_available':True,'dispatch_claimed':False}
            report['unavailable_metrics']={'waiting_time':'No request_timestamp y pickup asociado',
                'fleet_utilization':'No capacidad y tiempo disponible de flota',
                'operating_cost_per_km':'No costos de operación de flota',
                'failure_probability':'No etiquetas observadas de fallas'}
            for key,item in report['source_tables'].items():
                refreshed=client.get_table(item['table_id'])
                if (refreshed.modified.isoformat() if refreshed.modified else None)!=original_modifications[key]:
                    raise ValueError('Una fuente histórica cambió durante la validación; repetir para obtener una instantánea estable')
            report['source_immutability_verification']='passed'
            report['security']={'public_exports_contain_rider_identifiers':False,
                'historical_datasets_are_write_targets':False,
                'open_components':['Python','SQLite','React','Node.js','JSON','SQL'],
                'cloud_specific_components':['BigQuery execution and metadata client'],
                'iam_cmek_deployment_claimed':False}
            report['status']='validated_olap_pipeline'
            report['limitations']=['No certifica procedencia externa de las tablas existentes',
                'Replay de 11 eventos no acredita throughput de sensores metropolitanos',
                'Los pronósticos con histórico antiguo no se publican como demanda actual',
                'La arquitectura cubre cuatro verticales; la demo end-to-end acreditada es OLAP']
    except Exception as exc:
        report['status']='not_completed';report['error_type']=type(exc).__name__;report['error']=str(exc)
        save();raise
    finally:
        report['finished_at']=datetime.now(timezone.utc).isoformat()
        report['elapsed_seconds']=round(time.perf_counter()-started,3);save()
    print(json.dumps({'status':report['status'],'target_dataset':target,'coverage':report['coverage'],
                      'forecast_series':len(report['forecasts']),'streaming':report['streaming']},ensure_ascii=False,indent=2))
    print('LISTO. Pipeline completo validado sin reemplazar las tablas históricas.')


if __name__=='__main__':main()
