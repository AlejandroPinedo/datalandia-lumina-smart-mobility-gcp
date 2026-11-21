"""Contrasta el manifiesto cloud con SQL, lotes locales y resultado SQLite.

No consulta de nuevo GCP ni accede al contenido de credenciales.
"""
import hashlib
import json
from pathlib import Path
import re
from olap_sql import statements

ROOT=Path(__file__).resolve().parents[1]


def verify():
    cloud=json.loads((ROOT/'P3/evidencias/bigquery_latest.json').read_text(encoding='utf-8'))
    local=json.loads((ROOT/'P3/evidencias/sqlite_latest.json').read_text(encoding='utf-8'))
    run_id=cloud['run_id']
    if not re.fullmatch(r'p3_\d{8}_\d{6}_[a-f0-9]{6}',run_id):
        raise ValueError('Identificador de ejecución inválido')
    assert cloud['backend']=='bigquery' and cloud['data_origin']=='synthetic_generated_for_p3'
    assert cloud['api_report']['status']=='ok'
    assert cloud['api_report']['backend']=='bigquery' and cloud['api_report']['run_id']==run_id
    assert cloud['gold_rows']==cloud['unique_trip_ids']==cloud['api_report']['summary']['total_trips']==600
    assert all(value=='passed' for value in cloud['verification'].values())
    assert cloud['api_report']['summary']==local['api_report']['summary']
    assert cloud['api_report']['by_mode']==local['api_report']['by_mode']
    sql='\n'.join(statements(cloud['dataset'],bigquery=True))
    assert hashlib.sha256(sql.encode()).hexdigest()==cloud['sql_sha256']
    inputs=ROOT/'.runtime/p3'/run_id
    checked=0
    for filename,digest in cloud['input_sha256'].items():
        if Path(filename).name!=filename or '/' in filename or '\\' in filename:
            raise ValueError('Nombre de lote inválido')
        path=inputs/filename
        if path.is_file():
            assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
            checked+=1
    return {'status':'passed','run_id':run_id,'gold_rows':cloud['gold_rows'],
            'job_ids_recorded':len(cloud['job_ids']),'input_files_rechecked':checked,
            'sql_hash_matches':True,'cloud_local_kpis_match':True,
            'limitation':'Verificación de artefactos de ejecución; no es una nueva consulta remota ni un benchmark.'}


if __name__=='__main__':
    print(json.dumps(verify(),ensure_ascii=False,indent=2))
