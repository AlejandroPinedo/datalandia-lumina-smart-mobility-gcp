"""Exposición de la evidencia aislada del pipeline completo."""
import json
from pathlib import Path


def load_validation_report():
    try:
        data=json.loads((Path(__file__).resolve().parents[1]/'P3/evidencias/validacion_fondo_latest.json').read_text(encoding='utf-8'))
        if data.get('source_data_origin')!='existing_project_data' or data.get('generated_records_used') is not False or data.get('historical_sources_modified') is not False:
            raise ValueError('Evidencia no válida')
        return data
    except (OSError,ValueError,TypeError):
        return {'status':'unavailable','reason':'Ejecuta P3/completar_validacion_fondo.ps1.'}
