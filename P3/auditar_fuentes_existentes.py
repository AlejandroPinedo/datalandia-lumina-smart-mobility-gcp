"""Inventario de fuentes existentes. Solo metadatos; no crea ni reemplaza recursos."""
from datetime import datetime, timezone
import json
from pathlib import Path
from usar_datos_existentes import configure_credentials
import config

ROOT = Path(__file__).resolve().parents[1]


def main():
    configure_credentials()
    from google.cloud import bigquery
    result = {'project': config.GCP_PROJECT_ID, 'checked_at': datetime.now(timezone.utc).isoformat(),
              'operation': 'metadata_read_only', 'datasets': [], 'errors': []}
    with bigquery.Client(project=config.GCP_PROJECT_ID) as client:
        for item in client.list_datasets():
            if not item.dataset_id.startswith('lumina_'): continue
            # Solo fuentes del proyecto Lumina; excluye otros trabajos y pruebas sintéticas.
            dataset = client.get_dataset(item.reference)
            record = {'dataset_id': dataset.dataset_id, 'location': dataset.location,
                      'description': dataset.description, 'tables': []}
            try:
                for reference in client.list_tables(dataset.reference):
                    table = client.get_table(reference.reference)
                    record['tables'].append({'table_id': table.full_table_id.replace(':', '.'),
                        'table_type': table.table_type, 'num_rows': table.num_rows,
                        'num_bytes': table.num_bytes, 'description': table.description,
                        'modified': table.modified.isoformat() if table.modified else None,
                        'schema': [{'name': field.name, 'type': field.field_type,
                                    'mode': field.mode} for field in table.schema]})
            except Exception as exc:
                result['errors'].append({'dataset_id': dataset.dataset_id, 'error_type': type(exc).__name__})
            result['datasets'].append(record)
    target = ROOT/'P3/evidencias/inventario_fuentes_existentes.json'
    temporary = target.with_suffix('.tmp')
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    temporary.replace(target)
    print(json.dumps({'status': 'ok', 'project': result['project'],
        'datasets': len(result['datasets']), 'tables': sum(len(d['tables']) for d in result['datasets']),
        'errors': result['errors'], 'source_modified_by_this_process': False}, ensure_ascii=False, indent=2))
    print('LISTO. Inventario de fuentes existentes guardado.')


if __name__ == '__main__': main()
