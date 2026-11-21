"""Demuestra ingesta batch Parquet→Bronze y ejecuta el pipeline sin datos generados."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid
from usar_datos_existentes import configure_credentials
import config

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args();configure_credentials()
    from google.cloud import bigquery
    now=datetime.now(timezone.utc)
    output=ROOT/'P3/evidencias/ingesta_demostrada_latest.json'
    report={'status':'running','run_id':now.strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6],
        'started_at':now.isoformat(),'origin':'existing_project_data','generated_records_used':False,
        'historical_sources_modified':False,'scope':'Reingesta verificable de los datos existentes; no descarga externa nueva',
        'format':'PARQUET','tables':[]}
    if args.resume and output.exists():
        previous=json.loads(output.read_text(encoding='utf-8'))
        if previous['status'] in ['running','not_completed']:
            if not re.fullmatch(r'[0-9]{8}_[0-9]{6}_[a-f0-9]{6}',previous['run_id']):raise ValueError('Run ID inválido')
            report=previous;report['status']='running';report.pop('error',None)
    target='lumina_ingesta_'+report['run_id']
    report['target_dataset']=target
    bucket=config.GCS_BUCKET_DATALAKE
    if not re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]',bucket):raise ValueError('Bucket inválido')
    def save():
        temporary=output.with_suffix('.tmp');temporary.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8');temporary.replace(output)
    try:
        with bigquery.Client(project=config.GCP_PROJECT_ID) as client:
            source_dataset=client.get_dataset(f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}')
            location=source_dataset.location
            dataset=bigquery.Dataset(f'{config.GCP_PROJECT_ID}.{target}');dataset.location=location
            dataset.description='Ingesta demostrada desde Parquet de fuentes existentes; sin generadores'
            client.create_dataset(dataset,exists_ok=True)
            names=[config.BQ_TABLE_TLC_TRIPS,config.BQ_TABLE_UBER_SPEEDS,config.BQ_TABLE_IOT_AOT,config.BQ_TABLE_CENSO_INE]
            for name in names:
                source=f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{name}'
                destination=f'{config.GCP_PROJECT_ID}.{target}.{name}'
                table=client.get_table(source);modified=table.modified.isoformat()
                prior=next((item for item in report['tables'] if item['source']==source),None)
                if prior and prior.get('status')=='passed':
                    if prior['source_modified_at']!=modified:raise ValueError('La fuente cambió; no se reutiliza el resultado')
                    print('Ingesta conservada: '+name,flush=True);continue
                uri=f'gs://{bucket}/lumina-cierre/{report["run_id"]}/{name}/part-*.parquet'
                item=prior or {'source':source,'destination':destination,'source_rows':table.num_rows,'source_modified_at':modified,'uri':uri,'status':'running'}
                if prior and prior['source_modified_at']!=modified:raise ValueError('La fuente cambió durante la reanudación')
                if not prior:report['tables'].append(item)
                save()
                print('Exportando los registros existentes: '+name,flush=True)
                if item.get('extract_job_id'):export=client.get_job(item['extract_job_id'],location=location)
                else:
                    export=client.extract_table(source,uri,location=location,job_config=bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET,compression=bigquery.Compression.SNAPPY))
                    item['extract_job_id']=export.job_id;save()
                export.result(timeout=600)
                item['file_counts']=getattr(export,'destination_uri_file_counts',None);save()
                print('Cargando Parquet en Bronze aislado: '+name,flush=True)
                if item.get('load_job_id'):load=client.get_job(item['load_job_id'],location=location)
                else:
                    load=client.load_table_from_uri(uri,destination,location=location,job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,write_disposition=bigquery.WriteDisposition.WRITE_EMPTY))
                    item['load_job_id']=load.job_id;save()
                load.result(timeout=600)
                loaded=client.get_table(destination);item['loaded_rows']=loaded.num_rows
                fields=','.join('t.`'+field.name+'` AS `'+field.name+'`' for field in sorted(table.schema,key=lambda f:f.name))
                checksum=f'''SELECT 'source' AS origin,COUNT(*) AS total_rows,
                  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({fields})))) AS fingerprint_xor FROM `{source}` t
                  UNION ALL SELECT 'loaded',COUNT(*),BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({fields})))) FROM `{destination}` t'''
                check=client.query(checksum,location=location,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=10*1024**3))
                counts=[dict(row) for row in check.result(timeout=600)];item['checksum_job_id']=check.job_id
                item['checksums']=counts
                by_source={row['origin']:row for row in counts}
                if by_source['source']['total_rows']!=by_source['loaded']['total_rows'] or by_source['source']['fingerprint_xor']!=by_source['loaded']['fingerprint_xor']:
                    raise ValueError('La ingesta no conserva recuentos/huella de los registros de '+name)
                if client.get_table(source).modified.isoformat()!=modified:raise ValueError('La fuente histórica cambió durante la lectura')
                item['status']='passed';save()
        report['status']='ingestion_validated';report['finished_at']=datetime.now(timezone.utc).isoformat();save()
        print('Ingesta validada. Ejecutando procesamiento sobre el Bronze recién cargado...',flush=True)
        subprocess.run([sys.executable,'-X','utf8',str(ROOT/'P3/completar_validacion_fondo.py'),'--bronze-dataset',target],cwd=ROOT,check=True)
        validation=json.loads((ROOT/'P3/evidencias/validacion_fondo_latest.json').read_text(encoding='utf-8'))
        if validation['status']!='validated_olap_pipeline':raise ValueError('Pipeline sin validar')
        report['pipeline_run_id']=validation['run_id'];report['pipeline_target_dataset']=validation['target_dataset']
        report['status']='passed';save()
        validation['ingestion']={key:report[key] for key in ['status','run_id','target_dataset','tables','format','scope']}
        (ROOT/'P3/evidencias/validacion_fondo_latest.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    except Exception as exc:
        report['status']='not_completed';report['error_type']=type(exc).__name__;report['error']=str(exc);save();raise
    print('LISTO. Ingesta por archivos y procesamiento completo verificados.',flush=True)


if __name__=='__main__':main()
