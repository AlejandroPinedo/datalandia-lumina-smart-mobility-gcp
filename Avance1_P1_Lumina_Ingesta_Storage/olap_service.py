"""P3 expone únicamente datos existentes o una exportación identificada de esa consulta."""
from datetime import datetime,timezone
import json
import logging
import os
from pathlib import Path
import time
from existing_project_data import read_existing

ROOT=Path(__file__).resolve().parents[1]


def load_olap_report():
    started=time.monotonic()
    backend=os.getenv('LUMINA_DATA_BACKEND','project_export')
    try:
        if backend=='project_export':
            path=Path(os.getenv('LUMINA_PROJECT_REPORT',str(ROOT/'P3/evidencias/datos_existentes_latest.json')))
            data=json.loads(path.read_text(encoding='utf-8'))
            if data.get('data_origin')!='existing_project_data' or data.get('status')!='ok':
                raise ValueError('La API solo admite una consulta verificada a las tablas existentes')
            if not data.get('source','').startswith('bigquery:') or not data.get('job_id'):
                raise ValueError('Faltan la tabla fuente o el job de consulta')
            data['backend']='project_export'
            data['live_query']=False
            data['served_at']=datetime.now(timezone.utc).isoformat()
            data['latency_ms']=round((time.monotonic()-started)*1000,2)
            return data
        if backend in ('bigquery_existing','bigquery'):
            from google.cloud import bigquery
            import config
            source=os.getenv('LUMINA_SOURCE_TABLE',
                f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}.{config.BQ_GOLD_FACT_TRIPS}')
            with bigquery.Client(project=config.GCP_PROJECT_ID) as client:
                return read_existing(client,source,
                    bigquery.QueryJobConfig(maximum_bytes_billed=5*1024**3))
        raise ValueError('P3 no admite generadores, bases de demo ni datos suplentes')
    except Exception as exc:
        logging.getLogger('lumina.olap').warning('Datos existentes no disponibles (%s)',type(exc).__name__)
        return {'status':'unavailable','backend':backend,'data_origin':None,
                'reason':'No hay una consulta a las tablas existentes disponible. Ejecuta P3/usar_datos_existentes.ps1.',
                'by_mode':[],'summary':None}
