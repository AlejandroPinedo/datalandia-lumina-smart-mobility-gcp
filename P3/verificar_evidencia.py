"""Verifica únicamente la evidencia de lectura de tablas existentes."""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from existing_project_data import build_query,summarize


def verify():
    path=ROOT/'P3/evidencias/datos_existentes_latest.json'
    data=json.loads(path.read_text(encoding='utf-8'))
    if data.get('status')!='ok' or data.get('data_origin')!='existing_project_data':
        raise ValueError('No hay evidencia válida de las tablas existentes')
    if data.get('source_modified_by_this_process') is not False:
        raise ValueError('Falta verificar que no se modificó la fuente')
    table=data['source_table']
    sql,_=build_query(table['table_id'],[field['name'] for field in table['schema']])
    if hashlib.sha256(sql.encode()).hexdigest()!=data['sql_sha256']:
        raise ValueError('El SQL no coincide con la evidencia')
    if summarize(data['by_mode'])!=data['summary']:
        raise ValueError('Los totales no coinciden con las filas consultadas')
    verification=data.get('verification',{})
    if not data.get('job_id') or any(verification.get(key)!='passed' for key in
            ('api_matches_existing_query','consumer_references_api')):
        raise ValueError('Falta la consulta o la verificación HTTP')
    metadata_rows=table.get('num_rows_at_inspection')
    if metadata_rows is not None and metadata_rows!=data['summary']['total_trips']:
        raise ValueError('El conteo consultado no coincide con los metadatos registrados')
    return {'status':'passed','source':data['source'],'source_rows':data['summary']['total_trips'],
            'job_id':data['job_id'],'generated_data_used':False,
            'provenance_status':data['provenance_status']}


if __name__=='__main__': print(json.dumps(verify(),ensure_ascii=False,indent=2))
