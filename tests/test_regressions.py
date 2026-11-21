"""Regresiones locales, sin credenciales ni escrituras cloud.

SQLite ejecuta las relaciones y auditorías generadas, adaptando solo nombres y
funciones de dialecto; no sustituye la validación del script completo en BigQuery.
"""
from pathlib import Path
import importlib
import contextlib
import csv
import io
import json
import re
import sqlite3
import sys
import tempfile
import threading
import urllib.request
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

CODE = Path(__file__).resolve().parents[1] / "Avance1_P1_Lumina_Ingesta_Storage"
sys.path.insert(0, str(CODE))
import config
import pipeline_sql
import telemetry


def event(**changes):
    value = dict(event_id="e1", node_id="n1", zone_id="z1", county_id="c1", station_id="s1",
                 asset_type="rail", timestamp_evento="2026-11-20T10:00:00+00:00", lat=-12,
                 lon=-77, vibration_mms=3, temperature_c=30, sound_db=50, pm25_ugm3=10,
                 passenger_load_pct=50)
    value.update(changes)
    return json.dumps(value)


def message(raw, ack):
    return SimpleNamespace(message=SimpleNamespace(data=raw.encode()), ack_id=ack)


class TelemetryTests(unittest.TestCase):
    def test_invalid_payloads_go_to_dlq(self):
        payloads = ['{', 'null', '[]', '123', event(vibration_mms="abc"),
                    event(vibration_mms=None), event(vibration_mms=float('nan')),
                    event(temperature_c=float('inf')), event(lat=91), event(lon=-181),
                    event(station_id=''), event(node_id=True), event(passenger_load_pct=1.5),
                    event(passenger_load_pct=101), event(timestamp_evento='bad'),
                    event(timestamp_evento=None), event(timestamp_evento=123),
                    event(timestamp_evento='2026-11-20T10:00:00'), event(pm25_ugm3=-1)]
        for payload in payloads:
            with self.subTest(payload=payload):
                valid, row = telemetry.validar_y_enriquecer_evento(payload)
                self.assertFalse(valid)
                self.assertIn('motivo_error', row)

    def test_thresholds_and_timezone(self):
        valid, row = telemetry.validar_y_enriquecer_evento(event(vibration_mms=7.5,
                timestamp_evento='2026-11-20T05:00:00-05:00'))
        self.assertTrue(valid)
        self.assertTrue(row['requiere_mantenimiento_pdm'])
        self.assertEqual(row['timestamp_evento'], '2026-11-20T10:00:00+00:00')

    def test_partial_failure_only_acknowledges_persisted_rows(self):
        bq, sub = Mock(), Mock()
        bq.insert_rows_json.return_value = [{'index': 1, 'errors': [{'reason': 'backendError'}]}]
        with self.assertRaises(RuntimeError):
            telemetry.persistir_y_confirmar([message(event(), 'a'), message(event(event_id='e2'), 'b')],
                                            bq, sub, 'subscription', 'iot', 'dlq')
        self.assertEqual(sub.acknowledge.call_args.kwargs['request']['ack_ids'], ['a'])
        self.assertEqual(bq.insert_rows_json.call_args.kwargs['row_ids'], ['e1', 'e2'])

    def test_transport_failure_acknowledges_nothing(self):
        bq, sub = Mock(), Mock()
        bq.insert_rows_json.side_effect = TimeoutError('network')
        with self.assertRaises(TimeoutError):
            telemetry.persistir_y_confirmar([message(event(), 'a')], bq, sub, 's', 'iot', 'dlq')
        sub.acknowledge.assert_not_called()

    def test_dlq_failure_does_not_acknowledge_invalid_event(self):
        bq, sub = Mock(), Mock()
        bq.insert_rows_json.side_effect = [[], [{'index': 0, 'errors': []}]]
        with self.assertRaises(RuntimeError):
            telemetry.persistir_y_confirmar([message(event(), 'a'), message('null', 'b')],
                                            bq, sub, 's', 'iot', 'dlq')
        self.assertEqual(sub.acknowledge.call_args.kwargs['request']['ack_ids'], ['a'])


class CountIf:
    def __init__(self):
        self.count = 0
    def step(self, value):
        self.count += bool(value)
    def finalize(self):
        return self.count


def connection():
    db = sqlite3.connect(':memory:')
    db.create_aggregate('COUNTIF', 1, CountIf)
    db.create_function('SAFE_DIVIDE', 2, lambda a, b: a / b if a is not None and b else None)
    return db


def local_sql(sql):
    sql = re.sub(r'`[^`]+\.([^`.]+)`', lambda m: m[1], sql)
    sql = sql.replace('CREATE OR REPLACE TABLE', 'CREATE TABLE')
    sql = sql.replace('CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP')
    sql = re.sub(r'EXTRACT\(HOUR FROM ([^)]+)\)', r"CAST(strftime('%H', \1) AS INT)", sql)
    return sql


class SQLTests(unittest.TestCase):
    def test_join_preserves_trip_grain_across_dates_hours_and_stations(self):
        sql = pipeline_sql.build_gold_sql()
        cte = sql[sql.index('WITH iot_agg AS ('):sql.index('\n    SELECT\n      t.trip_id')]
        a = sql.index('    FROM ', sql.index('      t.rider_id_dlp_sha256,'))
        b = sql.index(';', a)
        query = cte + '\nSELECT t.trip_id, u.avg_speed_kmh, i.avg_oart_load_pct\n' + sql[a:b]
        db = connection()
        db.executescript(f'''
            CREATE TABLE {config.BQ_SILVER_TRIPS}(trip_id, origin_zone_id, dest_zone_id, pickup_datetime, connected_oart_station_id);
            CREATE TABLE {config.BQ_SILVER_CENSO}(zone_id);
            CREATE TABLE {config.BQ_SILVER_UBER}(origin_zone_id, dest_zone_id, travel_date, hod, avg_speed_kmh);
            CREATE TABLE {config.BQ_SILVER_IOT}(zone_id, station_id, timestamp_evento, passenger_load_pct, vibration_mms, requiere_mantenimiento_pdm);
            INSERT INTO {config.BQ_SILVER_CENSO} VALUES ('z1');
            INSERT INTO {config.BQ_SILVER_TRIPS} VALUES ('a','z1','z2','2026-11-19 10:00:00','s1'), ('b','z1','z2','2026-11-20 10:00:00','s1'), ('c','z1','z2','2026-11-21 10:00:00','s1');
            INSERT INTO {config.BQ_SILVER_UBER} VALUES ('z1','z2','2026-11-19',10,20), ('z1','z2','2026-11-20',10,40);
            INSERT INTO {config.BQ_SILVER_IOT} VALUES ('z1','s1','2026-11-19 10:01:00',30,3,0), ('z1','s1','2026-11-20 10:01:00',60,3,0), ('z1','s2','2026-11-20 10:01:00',99,3,0), ('z1','s1','2026-11-20 11:01:00',88,3,0);
        ''')
        self.assertEqual(db.execute(local_sql(query)).fetchall(), [('a',20,30), ('b',40,60), ('c',None,None)])

    def audit_db(self):
        db = connection()
        db.executescript(f'''
            CREATE TABLE {config.BQ_TABLE_TLC_TRIPS}(origin_zone_id, dest_zone_id, trip_distance_km, total_fare_usd, duration_minutes, dropoff_datetime, pickup_datetime);
            CREATE TABLE {config.BQ_SILVER_TRIPS}(trip_id);
            CREATE TABLE {config.BQ_SILVER_CENSO}(zone_id);
            CREATE TABLE {config.BQ_TABLE_IOT_AOT}(vibration_mms, temperature_c, passenger_load_pct, station_id, zone_id);
            INSERT INTO {config.BQ_SILVER_CENSO} VALUES ('z1'), ('z2');
        ''')
        return db

    def test_dq_rejects_bad_raw_data_even_if_silver_is_clean(self):
        db = self.audit_db()
        for i in range(20):
            fare = -1 if i < 2 else 5
            db.execute(f'INSERT INTO {config.BQ_TABLE_TLC_TRIPS} VALUES (?,?,?,?,?,?,?)',
                       ('z1','z2',3,fare,10,'2026-11-20 11:00:00','2026-11-20 10:00:00'))
            db.execute(f'INSERT INTO {config.BQ_SILVER_TRIPS} VALUES (?)', (str(i),))
        db.execute(f'INSERT INTO {config.BQ_TABLE_IOT_AOT} VALUES (3,30,50,\'s1\',\'z1\')')
        db.executescript(local_sql(pipeline_sql.build_dq_sql()))
        rows = db.execute(f'SELECT pct_cumplimiento, estado_gobernanza FROM {config.BQ_AUDIT_DQ_TABLE}').fetchall()
        self.assertIn((90, 'NO CUMPLE SLA'), rows)
        self.assertIn((100, 'CUMPLE SLA (>= 95%)'), rows)

    def test_empty_sources_are_not_reported_as_passing(self):
        db = self.audit_db()
        db.executescript(local_sql(pipeline_sql.build_dq_sql()))
        states = db.execute(f'SELECT estado_gobernanza FROM {config.BQ_AUDIT_DQ_TABLE}').fetchall()
        self.assertEqual(states, [('SIN_DATOS',)] * 4)

    def test_cli_stops_before_gold_if_gate_fails(self):
        module = importlib.import_module('06_pipeline_medallion_silver_gold_ml')
        client = Mock()
        client.query.return_value.result.side_effect = [None, None, RuntimeError('SLA')]
        cloud = SimpleNamespace(bigquery=SimpleNamespace(Client=Mock(return_value=client)))
        with patch.dict(sys.modules, {'google': SimpleNamespace(cloud=cloud), 'google.cloud': cloud}), \
             patch.object(module, 'exportar_cache_frontend') as export:
            with self.assertRaises(RuntimeError):
                module.construir_capas_silver_y_gold()
            self.assertEqual(client.query.call_count, 3)
            export.assert_not_called()


class IngestionTests(unittest.TestCase):
    def test_generators_cover_catalog_and_preserve_total_population(self):
        module = importlib.import_module('02_ingesta_batch_multifuente')
        with tempfile.TemporaryDirectory() as temp, patch.object(module, 'DATA_DIR', Path(temp)), \
             contextlib.redirect_stdout(io.StringIO()):
            censo = module.generar_censo_demografia_ine()
            with censo.open(encoding='utf-8', newline='') as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(len(rows), len(config.DATALANDIA_ZONES))
            self.assertEqual(sum(int(r['poblacion_residente']) for r in rows), 9250000)
            self.assertEqual(len({r['county_id'] for r in rows}), 8)
            viajes = module.generar_nyc_tlc_trips_multimodal(10)
            with viajes.open(encoding='utf-8', newline='') as file:
                ids = [r['trip_id'] for r in csv.DictReader(file)]
            module.generar_nyc_tlc_trips_multimodal(10)
            with viajes.open(encoding='utf-8', newline='') as file:
                self.assertEqual(ids, [r['trip_id'] for r in csv.DictReader(file)])

    def test_upload_uses_merge_and_no_target_replace(self):
        module = importlib.import_module('02_ingesta_batch_multifuente')
        with tempfile.TemporaryDirectory() as temp, patch.object(module, 'run_cmd', return_value=True) as run:
            path = Path(temp) / 'trips.csv'
            path.write_text('trip_id,ts_ingesta_bq\ne1,2026-11-20\n', encoding='utf-8')
            module.cargar_csv_sin_reemplazar(path, 'trips', ('trip_id',))
            commands = [c.args[0] for c in run.call_args_list]
            self.assertFalse(any('--replace' in c for c in commands))
            self.assertTrue(any(c[-1].startswith('MERGE ') for c in commands))
            self.assertTrue(commands[-1][-1].startswith('DROP TABLE IF EXISTS '))

    def test_load_failure_never_merges(self):
        module = importlib.import_module('02_ingesta_batch_multifuente')
        with tempfile.TemporaryDirectory() as temp, patch.object(module, 'run_cmd') as run:
            path = Path(temp) / 'trips.csv'
            path.write_text('trip_id,ts_ingesta_bq\ne1,2026-11-20\n', encoding='utf-8')
            run.side_effect = [True, True, RuntimeError('load failed'), True]
            with self.assertRaises(RuntimeError):
                module.cargar_csv_sin_reemplazar(path, 'trips', ('trip_id',))
            self.assertFalse(any(c.args[0][-1].startswith('MERGE ') for c in run.call_args_list))


class APITests(unittest.TestCase):
    def test_http_endpoints_expose_scenarios_and_unavailable_plans(self):
        api = importlib.import_module('08_app_exposicion_frontend_lumina')
        class Handler(api.StdlibLuminaHandler):
            def log_message(self, *args):
                pass
        server = api.HTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        prefix = f'http://127.0.0.1:{server.server_address[1]}'
        try:
            with urllib.request.urlopen(prefix + '/api/v1/health', timeout=3) as response:
                self.assertEqual(json.load(response)['status'], 'healthy')
            with patch.object(api, 'HAS_BIGQUERY', False):
                for endpoint in ('daily-plan', 'maintenance-plan'):
                    with urllib.request.urlopen(prefix + '/api/v1/' + endpoint, timeout=3) as response:
                        self.assertEqual(json.load(response)['status'], 'unavailable')
            request = urllib.request.Request(prefix + '/api/v1/route-simulator', data=b'{}',
                                             headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertTrue(json.load(response)['is_simulation'])
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=3)

    def test_query_failure_never_fabricates_metrics(self):
        api = importlib.import_module('08_app_exposicion_frontend_lumina')
        with patch.object(api, 'execute_bq_sql_safe', return_value=None):
            result = api.process_agent_query('ahorro')
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['metrics'], {})
        self.assertIsNone(result['sql_executed'])

    def test_route_is_labeled_as_scenario_and_uses_configured_costs(self):
        api = importlib.import_module('08_app_exposicion_frontend_lumina')
        with patch.object(config, 'CAR_FIXED_COST_USD', 0), patch.object(config, 'CAR_VARIABLE_COST_USD_KM', 0):
            result = api.compute_route_simulation({})
        self.assertTrue(result['is_simulation'])
        self.assertEqual(result['comparison']['personal_car_benchmark']['total_cost_usd'], 0)

    def test_empty_cache_does_not_reintroduce_sample_alerts(self):
        api = importlib.import_module('08_app_exposicion_frontend_lumina')
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'cache.json'
            path.write_text(json.dumps({'recent_iot_telemetry': [], 'dlq_summary': []}), encoding='utf-8')
            with patch.object(config, 'CACHE_FILE', path):
                data = api.load_gold_dashboard_data()
        self.assertEqual(data['recent_iot_telemetry'], [])
        self.assertEqual(data['dlq_summary'], [])
        self.assertEqual(data['data_origin'], 'exported_cache')


if __name__ == '__main__':
    unittest.main()
