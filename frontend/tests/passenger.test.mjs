import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createApp, readPassengerReport } from '../server.mjs';
import { alternativesFor, zonesFrom, sortAlternatives, tripExamples, routeAnswer, parseSaved, savedKey } from '../src/passenger-model.js';
let report = null;
try { report = JSON.parse(await readFile(new URL('../../P3/evidencias/movilidad_pasajero_latest.json', import.meta.url), 'utf8')); } catch {}
const requireExport = { skip: !report && 'Pendiente de la consulta real de Gold' };

test('Sin datos no se inventan rutas, precios o estaciones', () => {
  assert.deepEqual(alternativesFor([], '', ''), []); assert.deepEqual(tripExamples([]), []);
  assert.deepEqual(zonesFrom([]), []); assert.match(routeAnswer('precio', []), /Busca primero/);
  assert.deepEqual(parseSaved('not json'), []); assert.deepEqual(parseSaved('{"origin":"invalid"}'), []);
});
test('Alternativas: medias exactas SUM/COUNT del trayecto observado', requireExport, () => {
  const observed = report.routes[0], alternatives = alternativesFor(report.routes, observed.origin_zone_id, observed.dest_zone_id);
  assert.ok(alternatives.length);
  const selected = report.routes.filter(r => r.origin_zone_id === observed.origin_zone_id && r.dest_zone_id === observed.dest_zone_id);
  assert.equal(alternatives.reduce((n, r) => n + r.totalTrips, 0), selected.reduce((n, r) => n + r.total_trips, 0));
  for (const alternative of alternatives) {
    const rows = selected.filter(r => r.transport_mode === alternative.mode);
    const sum = key => rows.reduce((n, r) => n + (r[key] || 0), 0);
    assert.equal(alternative.avgDuration, sum('duration_count') ? sum('duration_sum') / sum('duration_count') : null);
    assert.equal(alternative.avgFare, sum('fare_count') ? sum('fare_sum') / sum('fare_count') : null);
    assert.deepEqual(alternative.stations, [...new Set(rows.flatMap(r => r.station_ids || []))].sort());
  }
  const ordered = sortAlternatives(alternatives, 'fare').filter(r => r.avgFare != null);
  assert.ok(ordered.every((r, i) => i === 0 || r.avgFare >= ordered[i - 1].avgFare));
});
test('Detalle y filtros conservan únicamente los registros reales exportados', requireExport, () => {
  const observed = report.routes.find(r => r.recent_trips.length), first = observed.recent_trips[0];
  const records = tripExamples(report.routes, { origin: observed.origin_zone_id, destination: observed.dest_zone_id, mode: observed.transport_mode, date: first.travel_date, search: first.trip_id });
  assert.ok(records.length);
  assert.ok(records.every(r => r.trip_id === first.trip_id && r.travel_date === first.travel_date && r.transport_mode === observed.transport_mode));
  const direct = records.find(r => r.county_name === observed.county_name);
  assert.equal(direct.lumina_total_fare_usd, first.lumina_total_fare_usd);
  assert.equal(direct.lumina_duration_min, first.lumina_duration_min);
  const all = tripExamples(report.routes);
  assert.ok(all.length <= report.routes.length * 3);
});
test('Zonas de destino usan nombres observados o el ID; guardados conserva la identidad del trayecto', requireExport, () => {
  const zones = zonesFrom(report.routes);
  for (const zone of zones) {
    assert.ok(report.routes.some(r => r.origin_zone_id === zone.id || r.dest_zone_id === zone.id));
    assert.ok(zone.name === zone.id || report.routes.some(r => r.origin_zone_id === zone.id && r.origin_zone_name === zone.name));
  }
  const first = report.routes[0], saved = { origin: first.origin_zone_id, destination: first.dest_zone_id, mode: first.transport_mode };
  assert.equal(savedKey(parseSaved(JSON.stringify([saved]))[0]), savedKey(saved));
});
test('API del pasajero declara el alcance del detalle y conserva el total de Gold', requireExport, async () => {
  const loaded = await readPassengerReport(); assert.equal(loaded.live_query, false);
  const server = createApp(); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/v1/passenger-data`), data = await response.json();
    assert.equal(response.status, 200); assert.equal(data.total_source_records, report.total_source_records);
    assert.ok(data.trip_examples_scope.includes('3')); assert.equal(data.source_modified_by_this_process, false);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
test('Sin exportación de trayectos la API responde unavailable', async () => {
  const server = createApp({ passengerEvidencePath: 'no-existe.json' }); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/v1/passenger-data`), data = await response.json();
    assert.equal(response.status, 503); assert.deepEqual(data.routes, []);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
