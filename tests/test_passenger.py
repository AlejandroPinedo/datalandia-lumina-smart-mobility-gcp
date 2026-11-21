import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from passenger_data import build_passenger_query
from passenger_service import load_passenger_report


class PassengerSourceTests(unittest.TestCase):
    def test_query_reads_existing_columns_without_identity_or_writes(self):
        evidence = json.loads((ROOT/'P3/evidencias/datos_existentes_latest.json').read_text(encoding='utf-8'))
        sql = build_passenger_query(evidence['source_table']['table_id'], [field['name'] for field in evidence['source_table']['schema']])
        self.assertTrue(sql.startswith('SELECT'))
        for token in ['INSERT ', 'CREATE ', 'MERGE ', 'DELETE ', 'RAND(', 'rider_id', 'SHA256']:
            self.assertNotIn(token, sql)
        self.assertIn('COUNT(*) AS total_trips', sql)
        self.assertIn('ORDER BY pickup_datetime DESC, trip_id DESC LIMIT 3', sql)

    def test_missing_columns_and_sql_injection_fail(self):
        with self.assertRaises(ValueError): build_passenger_query('a.b.c', [])
        with self.assertRaises(ValueError): build_passenger_query('a.b.c;DROP TABLE c', [])

    def test_absent_or_corrupt_export_has_no_fallback_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'report.json'
            self.assertEqual(load_passenger_report(path)['routes'], [])
            path.write_text('{bad json', encoding='utf-8')
            result = load_passenger_report(path)
            self.assertEqual(result['status'], 'unavailable')
            self.assertEqual(result['routes'], [])


class PassengerActualExportTests(unittest.TestCase):
    def test_existing_export_matches_executed_sql_and_source_count(self):
        path = ROOT/'P3/evidencias/movilidad_pasajero_latest.json'
        if not path.exists(): self.skipTest('Pendiente de la consulta real de Gold')
        report = load_passenger_report(path)
        self.assertEqual(report['status'], 'ok')
        schema = json.loads((ROOT/'P3/evidencias/esquema_fuente_existente.json').read_text(encoding='utf-8'))
        sql = build_passenger_query(report['source'].removeprefix('bigquery:'), [field['name'] for field in schema['schema']])
        self.assertEqual(hashlib.sha256(sql.encode()).hexdigest(), report['sql_sha256'])
        self.assertEqual(sum(row['total_trips'] for row in report['routes']), report['total_source_records'])
        self.assertFalse(report['source_modified_by_this_process'])
        for route in report['routes']:
            self.assertLessEqual(len(route['recent_trips']), 3)
            for trip in route['recent_trips']:
                self.assertIsInstance(trip, dict)
                self.assertTrue(trip['trip_id'])


if __name__ == '__main__': unittest.main()
