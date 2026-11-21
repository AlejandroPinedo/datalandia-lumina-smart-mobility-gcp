"""Lee el Gold existente y verifica su exposición por API. No genera ni carga viajes."""
import argparse
from datetime import datetime,timezone
import importlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
CODE=ROOT/'Avance1_P1_Lumina_Ingesta_Storage'
sys.path.insert(0,str(CODE))
from existing_project_data import read_existing,validate_table_id
import config


def configure_credentials():
    packages=ROOT/'.runtime/python-gcp'
    if packages.is_dir(): sys.path.insert(0,str(packages))
    credentials=ROOT/'.runtime/gcloud/application_default_credentials.json'
    if credentials.is_file(): os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS',str(credentials))
    os.environ.setdefault('CLOUDSDK_CONFIG',str(ROOT/'.runtime/gcloud'))


def verify_http(expected):
    api=importlib.import_module('08_app_exposicion_frontend_lumina')
    class Handler(api.StdlibLuminaHandler):
        def log_message(self,*args): pass
    server=api.HTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    prefix=f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with urllib.request.urlopen(prefix+'/api/v1/olap-kpis',timeout=10) as response:
            report=json.load(response)
            if report.get('status')!='ok' or report.get('summary')!=expected['summary']:
                raise AssertionError('La API no coincide con la consulta de las tablas existentes')
            if report.get('job_id')!=expected['job_id'] or report.get('data_origin')!='existing_project_data':
                raise AssertionError('La API no identifica la fuente existente consultada')
        for route in ('/p3','/'):
            with urllib.request.urlopen(prefix+route,timeout=5) as response:
                text=response.read().decode('utf-8')
                if '/api/v1/olap-kpis' not in text or re.search(r'\{[{%]',text):
                    raise AssertionError('El consumidor no está conectado o tiene una plantilla sin resolver')
        return {'api_matches_existing_query':'passed','consumer_references_api':'passed'}
    finally:
        server.shutdown();server.server_close();worker.join(timeout=3)


def export_existing(source):
    configure_credentials()
    from google.cloud import bigquery
    source=validate_table_id(source)
    project=source.split('.')[0]
    evidence_dir=ROOT/'P3/evidencias';evidence_dir.mkdir(exist_ok=True)
    source_info=None
    try:
        with bigquery.Client(project=project) as client:
            table=client.get_table(source,timeout=20)
            source_info={'table_id':source,'num_rows_at_inspection':table.num_rows,
                         'description':table.description,
                         'schema':[{'name':field.name,'type':field.field_type} for field in table.schema]}
            (evidence_dir/'esquema_fuente_existente.json').write_text(
                json.dumps(source_info,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
            report=read_existing(client,source,bigquery.QueryJobConfig(maximum_bytes_billed=5*1024**3))
    except Exception as exc:
        diagnostic={'status':'not_completed','source':source,'error_type':type(exc).__name__,
                    'message':str(exc),'source_info':source_info,
                    'checked_at':datetime.now(timezone.utc).isoformat()}
        # Diagnóstico local: no se publica en el repositorio ni incluye archivos de credenciales.
        runtime=ROOT/'.runtime';runtime.mkdir(exist_ok=True)
        (runtime/'consulta_existente_error.json').write_text(
            json.dumps(diagnostic,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        raise
    output=evidence_dir/'datos_existentes_latest.json'
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    os.environ['LUMINA_DATA_BACKEND']='project_export'
    os.environ['LUMINA_PROJECT_REPORT']=str(output)
    report['verification']=verify_http(json.loads(output.read_text(encoding='utf-8')))
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-table',default=os.getenv('LUMINA_SOURCE_TABLE',
        f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}.{config.BQ_GOLD_FACT_TRIPS}'))
    parser.add_argument('--serve',action='store_true')
    args=parser.parse_args()
    report=export_existing(args.source_table)
    print(json.dumps({'status':report['status'],'source':report['source'],'summary':report['summary'],
                      'job_id':report['job_id'],'verification':report['verification'],
                      'source_modified_by_this_process':False},ensure_ascii=False,indent=2))
    if args.serve:
        api=importlib.import_module('08_app_exposicion_frontend_lumina')
        print(f'Abrir http://127.0.0.1:{config.FLASK_PORT}/p3')
        api.HTTPServer((config.FLASK_HOST,config.FLASK_PORT),api.StdlibLuminaHandler).serve_forever()


if __name__=='__main__': main()
