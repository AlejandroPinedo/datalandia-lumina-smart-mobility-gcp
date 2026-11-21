"""Pruebas de contratos: fixtures aisladas nunca se publican como datos del proyecto."""
from datetime import date, datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from operations.forecast import predict,evaluate_series
from operations.dispatch import schedule_requests,candidates_for_request
from operations.event_store import EventStore
from operations.maintenance import assess_sensors
from operations.stream_worker import consume_pubsub
from operations.validation_sql import build_observed_gold,assert_isolated_writes
import config
from pipeline_sql import build_silver_sql


class ActualHistoryTests(unittest.TestCase):
    def test_model_uses_separate_validation_test_and_rejects_stale_operational_forecast(self):
        report=json.loads((ROOT/'P3/evidencias/datos_existentes_latest.json').read_text(encoding='utf-8'))
        cube=report['exploration'];columns=cube['columns'];values={}
        day_index=columns.index('travel_date');trips_index=columns.index('total_trips')
        for row in cube['rows']:
            day=cube['dictionaries']['travel_date'][row[day_index]]
            values[day]=values.get(day,0)+row[trips_index]
        observations=[{'date':day,'trips':count} for day,count in sorted(values.items())]
        result=evaluate_series(observations,today=date(2026,11,21))
        self.assertEqual(result['status'],'historical_evaluation_only')
        self.assertIsNone(result['operational_forecast_for_tomorrow'])
        self.assertLess(result['validation_end'],result['test_start'])
        self.assertEqual(result['test_metrics']['n'],7)
        history={date.fromisoformat(k):v for k,v in values.items()}
        target=sorted(history)[-7]
        original=predict(history,target,result['selected_model'])
        future_changed={day:(n+999999 if day>=target else n) for day,n in history.items()}
        self.assertEqual(original,predict(future_changed,target,result['selected_model']))

    def test_new_gold_has_observed_reference_and_isolated_write_guard(self):
        sql=build_observed_gold('bigdata-herles-pinedo','lumina_validacion_test')
        assert_isolated_writes(sql,'bigdata-herles-pinedo','lumina_validacion_test')
        self.assertIn('reference_rows',sql)
        self.assertNotIn('CAR_FIXED',sql)
        self.assertNotIn('rider_id',sql)
        with self.assertRaises(ValueError):assert_isolated_writes(sql,'bigdata-herles-pinedo','lumina_gold_governed')
        with self.assertRaises(ValueError):assert_isolated_writes(sql.replace('lumina_validacion_test','lumina_gold_governed'),'bigdata-herles-pinedo','lumina_validacion_test')


class DispatchContractTests(unittest.TestCase):
    def fixture(self):
        now=datetime(2026,11,21,12,tzinfo=timezone.utc)
        request=lambda rid,origin,dest:{'request_id':rid,'origin':origin,'destination':dest,'requested_at':now.isoformat(),'passengers':1}
        drivers=[{'driver_id':d,'status':'available','observed_at':now.isoformat(),
                  'shift_start':(now-timedelta(hours=1)).isoformat(),'shift_end':(now+timedelta(hours=3)).isoformat()} for d in ['d1','d2']]
        fleet=[{'vehicle_id':v,'driver_id':d,'zone_id':z,'status':'available','observed_at':now.isoformat(),
                'capacity':4,'occupied_seats':0,'wheelchair_accessible':True,'transport_mode':'M'}
               for v,d,z in [('v1','d1','A'),('v2','d2','B')]]
        edges=[{'origin':a,'destination':b,'duration_min':n,'fare_usd':n,'observed_at':now.isoformat(),
                'transport_mode':'M','wheelchair_accessible':True} for a,b,n in [('A','X',1),('A','Z',2),('B','X',2),('B','Z',100),('X','Y',1),('Z','W',1)]]
        return now,[request('r1','X','Y'),request('r2','Z','W')],fleet,drivers,edges

    def test_global_assignment_beats_greedy_and_preserves_exclusivity(self):
        now,requests,fleet,drivers,edges=self.fixture()
        result=schedule_requests(requests,fleet,drivers,edges,now)
        self.assertEqual(result['status'],'planned')
        self.assertEqual({a['request_id']:a['vehicle_id'] for a in result['assignments']},{'r1':'v2','r2':'v1'})
        self.assertEqual(sum(a['score'] for a in result['assignments']),6)
        self.assertEqual(len({a['driver_id'] for a in result['assignments']}),2)

    def test_missing_stale_capacity_accessibility_and_closure_do_not_dispatch(self):
        now,requests,fleet,drivers,edges=self.fixture()
        self.assertEqual(candidates_for_request(requests[0],[],drivers,edges,now)['status'],'requires_sources')
        for v in fleet:v['observed_at']=(now-timedelta(hours=1)).isoformat()
        self.assertFalse(candidates_for_request(requests[0],fleet,drivers,edges,now)['candidates'])
        for v in fleet:v['observed_at']=now.isoformat();v['occupied_seats']=v['capacity']
        self.assertFalse(candidates_for_request(requests[0],fleet,drivers,edges,now)['candidates'])
        for v in fleet:v['occupied_seats']=0;v['wheelchair_accessible']=False
        requests[0]['wheelchair_required']=True
        self.assertFalse(candidates_for_request(requests[0],fleet,drivers,edges,now)['candidates'])
        requests[0]['wheelchair_required']=False
        events=[{'kind':'closure','origin':'X','destination':'Y','starts_at':(now-timedelta(minutes=1)).isoformat(),'ends_at':(now+timedelta(minutes=1)).isoformat()}]
        self.assertFalse(candidates_for_request(requests[0],fleet,drivers,edges,now,events=events)['candidates'])


class EventContractTests(unittest.TestCase):
    def test_pubsub_ack_follows_persistence_and_storage_failure_keeps_message(self):
        from types import SimpleNamespace
        message=SimpleNamespace(message=SimpleNamespace(data=b'{}'),ack_id='ack-contract')
        class FailingStore:
            def process(self,_):raise OSError('storage unavailable')
        acks=[]
        with self.assertRaises(OSError):consume_pubsub([message],FailingStore(),acks.append)
        self.assertEqual(acks,[])
        with tempfile.TemporaryDirectory() as temp:
            store=EventStore(Path(temp)/'events.sqlite3')
            try:
                outcomes=consume_pubsub([message],store,acks.append)
                self.assertEqual(outcomes,['deadletter']);self.assertEqual(acks,['ack-contract'])
                self.assertEqual(store.counts()['deadletter'],1)
            finally:store.close()

    def test_duplicate_conflict_late_event_and_restore(self):
        # Contrato mínimo aislado. No se usa como medición ni evidencia de sensores reales.
        event={'event_id':'test-event','node_id':'test-node','zone_id':'test-zone','station_id':'test-station',
               'timestamp_evento':'2026-11-21T12:00:00+00:00','vibration_mms':1,'temperature_c':20,'passenger_load_pct':50}
        with tempfile.TemporaryDirectory() as temp:
            store=EventStore(Path(temp)/'events.sqlite3')
            try:
                self.assertEqual(store.process(event),'accepted')
                self.assertEqual(store.process(event),'duplicate')
                self.assertEqual(store.process({**event,'temperature_c':21}),'deadletter')
                self.assertEqual(store.process({**event,'event_id':'older','timestamp_evento':'2026-11-20T12:00:00+00:00'}),'accepted')
                self.assertEqual(store.latest()[0]['event_id'],'test-event')
                store.backup(Path(temp)/'restore.sqlite3')
                recovered=EventStore(Path(temp)/'restore.sqlite3')
                try:self.assertEqual(recovered.counts(),store.counts());self.assertEqual(recovered.integrity(),'ok')
                finally:recovered.close()
                diagnosis=assess_sensors([event],now=datetime(2026,11,22,tzinfo=timezone.utc))[0]
                self.assertEqual(diagnosis['status'],'stale_sensor');self.assertIsNone(diagnosis['failure_probability'])
            finally:store.close()


if __name__=='__main__':unittest.main()
