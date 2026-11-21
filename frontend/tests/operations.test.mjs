import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createApp,readValidationReport} from '../server.mjs';

test('API y aplicación consumen el Gold del pipeline nuevo, reconciliado con su evidencia',async()=>{
  const report=await readValidationReport();
  assert.equal(report.status,'validated_olap_pipeline');
  assert.equal(report.olap.reduce((n,row)=>n+row.trips,0),report.coverage.total_rows);
  assert.equal(report.coverage.unique_trips,report.coverage.total_rows);
  assert.equal(report.generated_records_used,false);
  const server=createApp();await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try {
    const base=`http://127.0.0.1:${server.address().port}`;
    const response=await fetch(base+'/api/v1/operations'),value=await response.json();
    assert.equal(response.status,200);assert.equal(value.run_id,report.run_id);
    assert.equal(value.source_immutability_verification,'passed');
    const daily=await(await fetch(base+'/api/v1/validation/daily-plan')).json();
    assert.equal(daily.forecasts.length,32);assert.equal(daily.operational_dispatch,false);
    assert.ok(daily.forecasts.every(model=>model.operational_forecast_for_tomorrow===null));
    const maintenance=await(await fetch(base+'/api/v1/validation/maintenance')).json();
    assert.equal(maintenance.sensors.length,11);assert.equal(maintenance.failure_probabilities_validated,false);
  } finally {await new Promise(resolve=>server.close(resolve));}
});
test('La integración conserva ausencia temporal y no publica probabilidades inventadas',async()=>{
  const report=await readValidationReport();
  assert.equal(report.coverage.traffic_matches,0);assert.equal(report.coverage.iot_matches,0);
  assert.ok(report.source_temporal_profiles.trips.end<report.source_temporal_profiles.traffic.start);
  assert.ok(report.maintenance.every(sensor=>sensor.failure_probability===null));
  assert.equal(report.streaming.redelivery_outcomes.duplicate,report.streaming.input_events);
  assert.equal(report.streaming.recovery,'passed');
  assert.equal(report.portable_join.input_records,report.portable_join.output_records);
});
