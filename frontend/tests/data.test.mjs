import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dataTools as D, emptyFilters, selectRows, exportSelection, answerQuestion } from '../src/data.js';
const report = JSON.parse(await readFile(new URL('../../P3/evidencias/datos_existentes_latest.json', import.meta.url), 'utf8'));
const cube = D.unpack(report);

test('La interfaz React preserva los totales de la consulta existente', () => {
  const selected = selectRows(report, cube, emptyFilters);
  const result = D.summary(D.group(selected, 'transport_mode'));
  assert.equal(result.total_trips, report.summary.total_trips);
  assert.ok(Math.abs(result.total_revenue_usd - report.summary.total_revenue_usd) < .01);
});
test('La comparación conserva fecha, territorio y hora al permitir otros modos', () => {
  const row = cube[0], filters = { from: row.travel_date, to: row.travel_date, county: row.county_name, hour: String(row.hod), modes: [row.transport_mode] };
  const selection = selectRows(report, cube, filters), comparison = selectRows(report, cube, filters, true);
  assert.ok(selection.length > 0 && comparison.length >= selection.length);
  assert.ok(selection.every(r => r.transport_mode === row.transport_mode));
  assert.ok(comparison.every(r => r.travel_date === row.travel_date && r.county_name === row.county_name && r.hod === row.hod));
});
test('Rango invertido y selección vacía no producen indicadores de respaldo', () => {
  const selected = selectRows(report, cube, { ...emptyFilters, from: '9999-01-01', to: '0001-01-01' });
  assert.equal(selected.length, 0);
  assert.match(answerQuestion('resumen', [], selected), /No hay registros/);
});
test('El CSV identifica filtros y la consulta que produjo el desglose', () => {
  const row = cube[0], filters = { ...emptyFilters, county: row.county_name, modes: [row.transport_mode] };
  const grouped = D.group(selectRows(report, cube, filters), 'transport_mode');
  const csv = exportSelection(grouped, filters, report);
  assert.ok(csv.includes(report.exploration.job_id)); assert.ok(csv.includes(row.county_name));
  assert.equal(csv.split('\r\n').length, grouped.length + 1);
});
test('Las consultas guiadas cambian con la selección real', () => {
  const selected = selectRows(report, cube, { ...emptyFilters, modes: [cube[0].transport_mode] });
  const rows = D.group(selected, 'transport_mode');
  assert.ok(answerQuestion('más viajes', rows, selected).includes(String(new Intl.NumberFormat('es-PE').format(rows[0].total_trips))));
  assert.match(answerQuestion('ruta a mi casa', rows, selected), /Puedes consultar/);
});
