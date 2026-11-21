"""P3 usa exclusivamente las tablas existentes. No genera ni carga datos simulados."""
import argparse
import importlib
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from olap_service import load_olap_report
from usar_datos_existentes import export_existing,verify_http
import config


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend',choices=['project_export','bigquery_existing','bigquery'],default='project_export')
    parser.add_argument('--serve',action='store_true')
    args=parser.parse_args()
    if args.backend in ('bigquery_existing','bigquery'):
        source=os.getenv('LUMINA_SOURCE_TABLE',f'{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}.{config.BQ_GOLD_FACT_TRIPS}')
        report=export_existing(source)
    else:
        os.environ['LUMINA_DATA_BACKEND']='project_export'
        report=load_olap_report()
        if report['status']!='ok':
            raise RuntimeError(report['reason'])
        verify_http(report)
    print('Fuente:',report['source'])
    print('Registros consultados:',report['summary']['total_trips'])
    print('Procedencia externa: pendiente de verificacion; no se genero ningun viaje.')
    if args.serve:
        api=importlib.import_module('08_app_exposicion_frontend_lumina')
        print(f'Abrir http://127.0.0.1:{config.FLASK_PORT}/p3')
        api.HTTPServer((config.FLASK_HOST,config.FLASK_PORT),api.StdlibLuminaHandler).serve_forever()


if __name__=='__main__': main()
