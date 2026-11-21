"""Acceso de lectura a los trayectos exportados del Gold existente."""
import json
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parents[1]/'P3/evidencias/movilidad_pasajero_latest.json'


def load_passenger_report(path=DEFAULT_PATH):
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8'))
        if (data.get('status') != 'ok' or data.get('data_origin') != 'existing_project_data'
                or not data.get('source', '').startswith('bigquery:') or not data.get('job_id')
                or data.get('source_modified_by_this_process') is not False
                or not isinstance(data.get('routes'), list)):
            raise ValueError('La exportación no identifica trayectos existentes')
        if sum(int(row['total_trips']) for row in data['routes']) != data.get('total_source_records'):
            raise ValueError('El conteo de trayectos no coincide con el total consultado')
        data['live_query'] = False
        return data
    except (OSError, ValueError, TypeError, KeyError):
        return {'status':'unavailable', 'routes':[], 'data_origin':None,
                'reason':'Actualiza los trayectos existentes ejecutando P3/recuperar_web_pasajero.ps1.'}
