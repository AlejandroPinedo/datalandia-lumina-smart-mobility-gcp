import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';

const directory = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(directory, '..');
const reportPath = path.join(root, 'P3/evidencias/datos_existentes_latest.json');
const passengerPath = path.join(root, 'P3/evidencias/movilidad_pasajero_latest.json');
const validationPath = path.join(root, 'P3/evidencias/validacion_fondo_latest.json');
const mimeTypes = { '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml', '.woff2': 'font/woff2', '.ico': 'image/x-icon' };

export async function readReport(filename = reportPath) {
  const data = JSON.parse(await readFile(filename, 'utf8'));
  if (data.status !== 'ok' || data.data_origin !== 'existing_project_data' ||
      !data.source?.startsWith('bigquery:') || !data.job_id || data.source_modified_by_this_process !== false) {
    throw new Error('La exportación no identifica una consulta válida de la fuente existente.');
  }
  return { ...data, backend: 'project_export', live_query: false, served_at: new Date().toISOString() };
}

export async function readPassengerReport(filename = passengerPath) {
  const data = JSON.parse(await readFile(filename, 'utf8'));
  if (data.status !== 'ok' || data.data_origin !== 'existing_project_data' ||
      !data.source?.startsWith('bigquery:') || !data.job_id ||
      data.source_modified_by_this_process !== false || !Array.isArray(data.routes)) {
    throw new Error('No hay una exportación válida de trayectos existentes.');
  }
  if (data.routes.reduce((total, row) => total + Number(row.total_trips), 0) !== data.total_source_records) {
    throw new Error('Los trayectos no conservan los registros de Gold.');
  }
  return { ...data, live_query: false };
}

export async function readValidationReport(filename=validationPath) {
  const data=JSON.parse(await readFile(filename,'utf8'));
  if (data.source_data_origin!=='existing_project_data' || data.generated_records_used!==false || data.historical_sources_modified!==false) throw new Error('Evidencia de validación no identificada');
  return data;
}

export function createApp({ evidencePath = reportPath, passengerEvidencePath = passengerPath, validationEvidencePath=validationPath, distPath = path.join(directory, 'dist') } = {}) {
  return createServer(async (req, res) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'same-origin');
    if (!['GET', 'HEAD'].includes(req.method)) { res.writeHead(405, { Allow: 'GET, HEAD' }); res.end(); return; }
    const send = (status, body, type) => { res.writeHead(status, { 'Content-Type': type }); res.end(req.method === 'HEAD' ? undefined : body); };
    const json = (status, value) => { res.setHeader('Cache-Control', 'no-store'); send(status, JSON.stringify(value), 'application/json; charset=utf-8'); };
    try {
      const pathname = new URL(req.url, 'http://localhost').pathname;
      if (pathname === '/p3/') { res.writeHead(302, { Location: '/p3' }); res.end(); return; }
      if (['/api/v1/operations','/api/v1/validation/daily-plan','/api/v1/validation/maintenance'].includes(pathname)) {
        try {
          const data=await readValidationReport(validationEvidencePath);
          if (pathname!=='/api/v1/operations' && data.status!=='validated_olap_pipeline') {
            json(503,{status:'validation_not_finished',run_id:data.run_id});return;
          }
          if (pathname.endsWith('/daily-plan')) json(200,{status:'historical_evaluation',run_id:data.run_id,forecasts:data.forecasts||[],operational_dispatch:false});
          else if (pathname.endsWith('/maintenance')) json(200,{status:'historical_diagnostics',run_id:data.run_id,sensors:data.maintenance||[],failure_probabilities_validated:false});
          else json(200,data);
        } catch { json(503,{status:'unavailable',reason:'Ejecuta P3/completar_validacion_fondo.ps1.'}); }
        return;
      }
      if (pathname === '/api/v1/passenger-data') {
        try { json(200, await readPassengerReport(passengerEvidencePath)); }
        catch { json(503, { status: 'unavailable', data_origin: null, routes: [],
          reason: 'Actualiza los trayectos existentes ejecutando P3/recuperar_web_pasajero.ps1.' }); }
        return;
      }
      if (pathname === '/api/v1/olap-kpis' || pathname === '/api/v1/dashboard-summary') {
        try { json(200, await readReport(evidencePath)); }
        catch { json(503, { status: 'unavailable', backend: 'project_export', data_origin: null, by_mode: [], summary: null,
          reason: 'No hay una consulta válida disponible. Ejecuta P3/usar_datos_existentes.ps1 para actualizar Gold.' }); }
        return;
      }
      if (pathname === '/' || pathname === '/p3') {
        res.setHeader('Cache-Control', 'no-cache');
        try { send(200, await readFile(path.join(distPath, 'index.html')), 'text/html; charset=utf-8'); }
        catch { send(503, 'Compila la interfaz con npm run build dentro de frontend.', 'text/plain; charset=utf-8'); }
        return;
      }
      if (pathname.startsWith('/assets/')) {
        const filename = path.resolve(distPath, '.' + decodeURIComponent(pathname));
        const assetsRoot = path.resolve(distPath, 'assets') + path.sep;
        if (!filename.startsWith(assetsRoot)) { json(404, { status: 'not_found' }); return; }
        try {
          if (!(await stat(filename)).isFile()) throw new Error('not a file');
          res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
          send(200, await readFile(filename), mimeTypes[path.extname(filename)] || 'application/octet-stream');
        } catch { json(404, { status: 'not_found' }); }
        return;
      }
      json(404, { status: 'not_found' });
    } catch { json(400, { status: 'bad_request' }); }
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const port = Number(process.env.PORT || 3000);
  createApp().listen(port, '127.0.0.1', () => console.log(`Lumina · React + Node.js\nhttp://127.0.0.1:${port}/p3`));
}
