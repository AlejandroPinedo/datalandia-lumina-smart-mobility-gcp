"""Exporta los trayectos existentes para la web del pasajero, sin escribir en GCP."""
import argparse
import json
import os
from pathlib import Path
from usar_datos_existentes import configure_credentials
from passenger_data import read_passenger_data
import config

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-table', default=os.getenv('LUMINA_SOURCE_TABLE',
        f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}.{config.BQ_GOLD_FACT_TRIPS}'))
    args = parser.parse_args()
    configure_credentials()
    from google.cloud import bigquery
    with bigquery.Client(project=args.source_table.split('.')[0]) as client:
        report = read_passenger_data(client, args.source_table,
                                    bigquery.QueryJobConfig(maximum_bytes_billed=5*1024**3))
    target = ROOT/'P3/evidencias/movilidad_pasajero_latest.json'
    temporary = target.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, ensure_ascii=False, default=str, separators=(',', ':')), encoding='utf-8')
    temporary.replace(target)
    print(json.dumps({'status':report['status'], 'source':report['source'],
                      'registros_consultados':report['total_source_records'],
                      'grupos_de_trayectos':len(report['routes']), 'job_id':report['job_id'],
                      'source_modified_by_this_process':False}, ensure_ascii=False, indent=2))
    print('LISTO. Los trayectos existentes están disponibles para la web del pasajero.')


if __name__ == '__main__': main()
