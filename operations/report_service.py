"""Resultados verificables de la ejecución aislada, sin fabricar estados exitosos."""
import json
from pathlib import Path

REPORT=Path(__file__).resolve().parents[1]/'P3/evidencias/validacion_fondo_latest.json'


def load_operations_report(path=REPORT):
    try:
        value=json.loads(Path(path).read_text(encoding='utf-8'))
        if (value.get('source_data_origin')!='existing_project_data' or
            value.get('generated_records_used') is not False or
            value.get('historical_sources_modified') is not False):
            raise ValueError('No hay evidencia válida')
        return value
    except (OSError,ValueError,TypeError):
        return {'status':'unavailable','source_data_origin':None,
                'reason':'Ejecuta P3/completar_validacion_fondo.ps1 para generar la evidencia.'}
