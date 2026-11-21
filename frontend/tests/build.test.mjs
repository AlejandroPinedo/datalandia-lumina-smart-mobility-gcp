import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createApp } from '../server.mjs';

test('La compilación sirve React y sus recursos en Node y en la plantilla Python', async () => {
  const server = createApp();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const route of ['/', '/p3', '/p3/']) {
      const response = await fetch(base + route), html = await response.text();
      assert.equal(response.status, 200); assert.ok(html.includes('id="root"'));
      const assets = [...html.matchAll(/(?:src|href)="([^" ]*assets\/[^" ]+)"/g)].map(match => match[1]);
      assert.equal(assets.length, 2);
      for (const asset of assets) {
        const response = await fetch(new URL(asset, base + '/'));
        assert.equal(response.status, 200);
        const body = await response.text(); assert.ok(body.length > 1000);
        if (asset.endsWith('.js')) {
          assert.ok(body.includes('/api/v1/olap-kpis')); assert.ok(body.includes('Resumen de movilidad'));
          assert.ok(body.includes('/api/v1/passenger-data')); assert.ok(body.includes('Viajes registrados'));
        }
      }
    }
    const inline = await readFile(new URL('../../Avance1_P1_Lumina_Ingesta_Storage/templates/p3_olap.html', import.meta.url), 'utf8');
    assert.ok(inline.includes('type="module"'));
    assert.ok(inline.includes('/api/v1/olap-kpis'));
    assert.ok(inline.includes('Resumen de movilidad'));
    assert.ok(!/<script[^>]*src=/.test(inline));
    assert.ok(!/<link[^>]*rel="stylesheet"/.test(inline));
  } finally { await new Promise(resolve => server.close(resolve)); }
});
