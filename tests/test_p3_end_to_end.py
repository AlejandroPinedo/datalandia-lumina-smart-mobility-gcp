"""Ensayo P3 completo con datos aislados y consumidor HTTP, sin credenciales."""
import contextlib
import importlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(REPO/'P3'))
sys.path.insert(0,str(REPO/'Avance1_P1_Lumina_Ingesta_Storage'))
from demo_e2e import source_records,save_local,verify_http
from olap_service import load_olap_report
from calculadora_consumo import calculate


class P3Tests(unittest.TestCase):
    def test_pipeline_api_and_consumer_use_new_gold(self):
        with tempfile.TemporaryDirectory() as temp:
            directory=Path(temp)
            records,_=source_records(directory)
            database=directory/'demo.sqlite'
            metrics=save_local(records,database,'test_p3')
            self.assertEqual(len(records['p3_bronze_trips']),602)
            self.assertEqual(len(records['p3_deadletter']),2)
            self.assertEqual(metrics['gold_rows'],600)
            self.assertEqual(metrics['traffic_matched_trips'],540)
            self.assertEqual(metrics['iot_matched_trips'],600)
            with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'sqlite','LUMINA_SQLITE_PATH':str(database)}):
                report=verify_http()
            self.assertEqual(report['run_id'],'test_p3')
            self.assertEqual(report['summary']['total_trips'],600)

    def test_api_reads_changes_in_gold_instead_of_fixed_results(self):
        with tempfile.TemporaryDirectory() as temp:
            directory=Path(temp)
            records,_=source_records(directory)
            database=directory/'demo.sqlite'
            save_local(records,database,'changed')
            with contextlib.closing(sqlite3.connect(database)) as connection, connection:
                connection.execute('UPDATE p3_gold_kpi SET total_trips=1')
            with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'sqlite','LUMINA_SQLITE_PATH':str(database)}):
                report=load_olap_report()
            self.assertEqual(report['summary']['total_trips'],5)

    def test_missing_database_does_not_create_one_or_fabricate_data(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'missing.sqlite'
            with patch.dict(os.environ,{'LUMINA_DATA_BACKEND':'sqlite','LUMINA_SQLITE_PATH':str(path)}):
                report=load_olap_report()
            self.assertEqual(report['status'],'unavailable')
            self.assertIsNone(report['summary'])
            self.assertFalse(path.exists())

    def test_standalone_dashboard_contains_no_template_expressions(self):
        api=importlib.import_module('08_app_exposicion_frontend_lumina')
        text=api.render_standalone_dashboard_html()
        self.assertNotIn('{{',text)
        self.assertNotIn('{%',text)

    def test_year_estimate_applies_contingency_once(self):
        config=json.loads((REPO/'P3'/'costos.json').read_text(encoding='utf-8'))
        result=calculate(config)
        self.assertAlmostEqual(result['monthly_subtotal'],15.0565)
        self.assertEqual(result['first_year_with_contingency'],216.81)
        config['usage_monthly']['bigquery_scanned_tib']=-1
        with self.assertRaises(ValueError): calculate(config)


if __name__=='__main__': unittest.main()
