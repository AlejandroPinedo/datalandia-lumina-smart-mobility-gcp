import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createApp } from '../server.mjs';

async function withServer(options, run) {
  const server = createApp(options); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try { await run(`http://127.0.0.1:${server.address().port}`); }
  finally { await new Promise(resolve => server.close(resolve)); }
}
test('Node expone la exportación identificada y no la presenta como consulta en vivo', async () => withServer({}, async base => {
  const response = await fetch(base + '/api/v1/olap-kpis'), data = await response.json();
  assert.equal(response.status, 200); assert.equal(data.data_origin, 'existing_project_data');
  assert.equal(data.live_query, false); assert.equal(data.source_modified_by_this_process, false);
  assert.equal(data.summary.total_trips, data.exploration.rows.reduce((sum, row) => sum + row[4], 0));
}));
test('Sin evidencia devuelve unavailable y nunca métricas inventadas', async () => withServer({ evidencePath: 'no-existe.json' }, async base => {
  const response = await fetch(base + '/api/v1/olap-kpis'), data = await response.json();
  assert.equal(response.status, 503); assert.equal(data.summary, null); assert.deepEqual(data.by_mode, []);
}));
test('El servidor no expone credenciales, código privado ni acepta escrituras', async () => withServer({}, async base => {
  for (const pathname of ['/.runtime/gcloud/application_default_credentials.json', '/server.mjs', '/api/no-existe']) {
    const response = await fetch(base + pathname); assert.equal(response.status, 404);
  }
  assert.equal((await fetch(base + '/api/v1/olap-kpis', { method: 'POST' })).status, 405);
}));
