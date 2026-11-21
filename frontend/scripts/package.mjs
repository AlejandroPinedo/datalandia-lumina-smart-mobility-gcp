// Conserva también la URL del servidor Python existente con un build autocontenido.
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = path.resolve(frontend, '..');
let html = await readFile(path.join(frontend, 'dist/index.html'), 'utf8');
const script = /<script\b[^>]*src="([^"]+)"[^>]*><\/script>/g;
const stylesheet = /<link\b[^>]*rel="stylesheet"[^>]*href="([^"]+)"[^>]*>/g;
for (const match of [...html.matchAll(script)]) {
  const filename = path.resolve(frontend, 'dist', match[1]);
  const code = await readFile(filename, 'utf8');
  html = html.replace(match[0], () => `<script type="module">${code.replace(/<\/script/gi, '<\\/script')}</script>`);
}
for (const match of [...html.matchAll(stylesheet)]) {
  const css = await readFile(path.resolve(frontend, 'dist', match[1]), 'utf8');
  html = html.replace(match[0], () => `<style>${css}</style>`);
}
if (!html.includes('/api/v1/olap-kpis')) throw new Error('La interfaz no referencia la API de Gold.');
for (const target of ['Avance1_P1_Lumina_Ingesta_Storage/templates/p3_olap.html', 'dashboard_operativo_lumina_en_vivo.html']) {
  await writeFile(path.join(root, target), html, 'utf8');
}
console.log('React compilado y conectado al servidor Node y a /p3 del servidor Python.');
