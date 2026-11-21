"""Comprueba la evidencia ejecutada; no confunde fuentes ausentes con resultados exitosos."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

ROOT=Path(__file__).resolve().parents[1]


def verify():
    report=json.loads((ROOT/'P3/evidencias/validacion_fondo_latest.json').read_text(encoding='utf-8'))
    assert report['status']=='validated_olap_pipeline', 'El pipeline no terminó'
    assert report['generated_records_used'] is False
    assert report['historical_sources_modified'] is False
    assert report['source_immutability_verification']=='passed'
    assert len(report['quality'])==4 and all(r['registros_evaluados']>0 and r['pct_cumplimiento']>=95 for r in report['quality'])
    coverage=report['coverage']
    assert coverage['total_rows']==coverage['unique_trips']==sum(r['trips'] for r in report['olap'])
    assert coverage['total_rows']<=report['source_tables']['trips']['rows']
    stages={stage['name']:stage for stage in report['stages']}
    for name in ['Bronze existente → Silver aislado','Auditoría de cuatro reglas','Compuerta de calidad ≥95%',
                 'Silver → Gold observado','Consumo OLAP del Gold nuevo']:
        assert stages[name]['status']=='passed' and stages[name]['job_id'] and stages[name]['sql_sha256']
    assert report['streaming']['idempotency']==report['streaming']['recovery']=='passed'
    assert report['streaming']['original_timestamps_preserved'] is True
    assert report['portable_join']['status']=='passed'
    assert report['portable_join']['input_records']==report['portable_join']['output_records']
    for model in report['forecasts']:
        assert model['validation_end']<model['test_start']
        assert model['test_metrics']['n']==7
        if model['data_age_days']>2: assert model['operational_forecast_for_tomorrow'] is None
    runtime=ROOT/'.runtime/operations'/report['run_id']
    for filename in ['events.sqlite3','recovery.sqlite3']:
        db=sqlite3.connect((runtime/filename).resolve().as_uri()+'?mode=ro',uri=True)
        try:
            assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
            assert db.execute('SELECT COUNT(*) FROM events').fetchone()[0]==report['streaming']['durable_counts']['events']
        finally:db.close()
    assert report['privacy_verification']['rider_identifiers_in_portable_export'] is False
    result={'status':'passed','checked_at':datetime.now(timezone.utc).isoformat(),
        'run_id':report['run_id'],'gold_validated_rows':coverage['total_rows'],
        'raw_trip_rows':report['source_tables']['trips']['rows'],
        'excluded_during_cleaning':report['source_tables']['trips']['rows']-coverage['total_rows'],
        'temporal_coverage':{key:coverage[key] for key in ['traffic_matches','iot_matches','census_matches']},
        'forecast_series':len(report['forecasts']),'generated_business_records':0,
        'scope':'Pipeline OLAP end-to-end, streaming histórico, backtesting y recuperación locales; no operación metropolitana'}
    (ROOT/'P3/evidencias/verificacion_fondo_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result


if __name__=='__main__':print(json.dumps(verify(),ensure_ascii=False,indent=2))
