"""P3 no debe generar datos ni aceptar resultados sintéticos como datos del proyecto.

Las entradas mínimas de protocolo son fixtures de prueba y no se publican como métricas.
"""
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from existing_project_data import build_query,read_existing
from olap_service import load_olap_report

COLUMNS=['trip_id','transport_mode','trip_distance_km','lumina_total_fare_usd','lumina_duration_min','passenger_count']
SOURCE='bigdata-herles-pinedo.lumina_gold_governed.fact_movilidad_multimodal_gold'


class P3ExistingDataTests(unittest.TestCase):
    def test_api_exposes_the_saved_existing_table_query(self):
        path=ROOT/'P3/evidencias/datos_existentes_latest.json'
        original=json.loads(path.read_text(encoding='utf-8'))
        with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'project_export','LUMINA_PROJECT_REPORT':str(path)}):
            result=load_olap_report()
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['summary'],original['summary'])
        self.assertEqual(result['job_id'],original['job_id'])
        self.assertEqual(result['source'],'bigquery:'+SOURCE)
        self.assertFalse(result['live_query'])

    def test_query_reads_full_existing_source_without_writes_or_sampling(self):
        sql,_=build_query(SOURCE,COLUMNS)
        self.assertIn('FROM `'+SOURCE+'`',sql)
        self.assertTrue(sql.startswith('SELECT '))
        self.assertNotRegex(sql.upper(),r'\b(CREATE|REPLACE|INSERT|MERGE|DELETE|DROP|LIMIT|RAND)\b')
        self.assertNotIn('600',sql)

    def test_missing_measurement_columns_fail_instead_of_using_constants(self):
        with self.assertRaises(ValueError): build_query(SOURCE,['transport_mode'])

    def test_absent_coverage_flags_remain_null(self):
        sql,mapping=build_query(SOURCE,COLUMNS)
        self.assertIsNone(mapping['traffic_match'])
        self.assertIn('CAST(NULL AS INT64) AS traffic_matched_trips',sql)
        self.assertNotIn('COALESCE',sql)

    def test_table_identifier_rejects_sql_injection(self):
        with self.assertRaises(ValueError): build_query(SOURCE+'`; DROP TABLE test',COLUMNS)

    def test_empty_remote_result_does_not_fabricate_rows(self):
        client=Mock()
        table=Mock()
        table.schema=[Mock(name='field') for _ in COLUMNS]
        for field,name in zip(table.schema,COLUMNS): field.name=name
        client.get_table.return_value=table
        client.query.return_value.result.return_value=[]
        with self.assertRaises(ValueError): read_existing(client,SOURCE)

    def test_missing_export_returns_unavailable_and_creates_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'absent.json'
            with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'project_export','LUMINA_PROJECT_REPORT':str(path)}):
                result=load_olap_report()
            self.assertEqual(result['status'],'unavailable')
            self.assertIsNone(result['summary'])
            self.assertFalse(path.exists())

    def test_synthetic_manifest_is_rejected_even_if_marked_successful(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'old.json'
            path.write_text(json.dumps({'status':'ok','data_origin':'synthetic_generated_for_p3'}),encoding='utf-8')
            with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'project_export','LUMINA_PROJECT_REPORT':str(path)}):
                result=load_olap_report()
            self.assertEqual(result['status'],'unavailable')
            self.assertEqual(result['by_mode'],[])

    def test_old_sqlite_backend_cannot_reactivate_generated_demo(self):
        with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'sqlite'}):
            result=load_olap_report()
        self.assertEqual(result['status'],'unavailable')

    def test_p3_runner_has_no_generator_imports(self):
        source=(ROOT/'P3/demo_e2e.py').read_text(encoding='utf-8')
        self.assertNotIn('source_records',source)
        self.assertNotIn('generar_nyc',source)
        self.assertNotIn('save_cloud',source)


if __name__=='__main__': unittest.main()
