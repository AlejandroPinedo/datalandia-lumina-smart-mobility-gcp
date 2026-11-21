"""Generador autocontenido basado en la documentación vigente y el código actual."""
from pathlib import Path
import base64
import html
import re

BASE = Path(__file__).resolve().parent
CODE = BASE / "Avance1_P1_Lumina_Ingesta_Storage"


def inline(text):
    value = html.escape(text)
    value = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', value)
    return re.sub(r'`([^`]+)`', r'<code>\1</code>', value)


def markdown(text):
    result, paragraph, table, listing = [], [], False, False
    lines = text.splitlines()
    i = 0
    def flush():
        if paragraph:
            result.append('<p>' + inline(' '.join(paragraph)) + '</p>')
            paragraph.clear()
    while i < len(lines):
        line = lines[i]
        if line.startswith('```'):
            flush()
            code = []
            i += 1
            while i < len(lines) and not lines[i].startswith('```'):
                code.append(lines[i]); i += 1
            result.append('<pre><code>' + html.escape('\n'.join(code)) + '</code></pre>')
        elif line.startswith('|'):
            flush()
            if not table:
                result.append('<div class="table"><table>'); table = True
            cells = line.strip('|').split('|')
            if not all(re.fullmatch(r'\s*:?-+:?\s*', c) for c in cells):
                tag = 'th' if i + 1 < len(lines) and re.match(r'\|[ :|-]+\|', lines[i+1]) else 'td'
                result.append('<tr>' + ''.join(f'<{tag}>{inline(c.strip())}</{tag}>' for c in cells) + '</tr>')
        else:
            if table:
                result.append('</table></div>'); table = False
            if line.startswith('#'):
                flush()
                depth = min(len(line) - len(line.lstrip('#')), 4)
                result.append(f'<h{depth}>{inline(line.lstrip("# "))}</h{depth}>')
            elif not line.strip():
                flush()
            else:
                paragraph.append(line)
        i += 1
    flush()
    if table:
        result.append('</table></div>')
    return '\n'.join(result)


def build(target, title, documents, include_evidence=True, include_diagrams=True, extra_files=(), cloud_verified=False):
    parts = []
    for filename in documents:
        parts.append('<section>' + markdown((BASE / filename).read_text(encoding='utf-8')) + '</section>')
    diagrams = [('01_lumina_arquitectura_end_to_end.html', 'Arquitectura general'),
                        ('02_lumina_dataflow_multifuente.html', 'Flujo multifuente'),
                        ('03_lumina_erd_modelo_datos.html', 'Modelo de datos')]
    if include_diagrams:
        parts.append('<section><h2>Diagramas de arquitectura objetivo</h2><p>Estos diagramas describen el diseño objetivo original. Las tablas de estado anteriores delimitan qué se ha implementado y qué falta validar.</p>')
    for name, label in diagrams if include_diagrams else []:
        document = (BASE / 'Visualizaciones_Archify' / name).read_bytes()
        encoded = base64.b64encode(document).decode('ascii')
        parts.append(f'<details><summary>{label} · diseño objetivo</summary><iframe sandbox="allow-scripts" title="{label}" src="data:text/html;base64,{encoded}"></iframe></details>')
    if include_diagrams:
        parts.append('</section>')
    code_status = ('El ensayo OLAP P3 se ejecutó en BigQuery. Esto no acredita todos los componentes cloud.'
                   if cloud_verified else 'La validación cloud del conjunto de componentes queda pendiente.')
    parts.append('<section><h2>Código y evidencias de la revisión</h2><p>Esta copia incluye los módulos compartidos. ' + code_status + '</p>')
    for path in sorted(CODE.glob('*.py')) + [BASE / name for name in extra_files]:
        parts.append(f'<details><summary>{html.escape(path.name)}</summary><pre><code>{html.escape(path.read_text(encoding="utf-8"))}</code></pre></details>')
    parts.append('</section>')
    if include_evidence:
        parts.append('<section><h2>Evidencias históricas y sus límites</h2>')
        captions = {
            'C1': 'Gold histórico: 4,998,926 filas. No es una ejecución de la revisión actual.',
            'C2': 'Tres reglas SQL históricas. El porcentaje financiero se muestra redondeado.',
            'C3': 'Tópico y suscripción. No mide latencia ni continuidad de procesamiento.',
            'G4': 'Parquet en bucket regional us-central1. No acredita Iceberg ni réplica dual.',
            'G5': 'Fallo de Java/Spark seguido de DEMO PYTHON con 600 filas. No acredita Spark real.',
        }
        for name, caption in captions.items():
            data = base64.b64encode((BASE / 'Capturas de pantalla' / (name + '.png')).read_bytes()).decode()
            parts.append(f'<figure><figcaption><strong>{name}.</strong> {caption}</figcaption><img loading="lazy" alt="{html.escape(caption)}" src="data:image/png;base64,{data}"></figure>')
        parts.append('</section>')
    page = '''<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>''' + html.escape(title) + '''</title>
<style>
:root{color-scheme:light;--ink:#132438;--muted:#4e6073;--line:#dbe4ec;--accent:#126a60}
*{box-sizing:border-box}body{margin:0;background:#edf3f6;color:var(--ink);font:16px/1.65 "Segoe UI",Arial,sans-serif}
header{background:#102d3b;color:white;padding:48px max(6vw,24px)}header p{max-width:780px;color:#d0e2e9}
header h1{font-size:clamp(26px,4vw,42px);line-height:1.2;margin:12px 0}.tag{letter-spacing:.12em;font-size:12px;font-weight:700;text-transform:uppercase;color:#9bddcc}
main{max-width:1180px;margin:32px auto;padding:0 20px}section{background:white;padding:clamp(20px,4vw,44px);margin:24px 0;border:1px solid var(--line);border-radius:14px}
h1,h2,h3{line-height:1.3}section h1{font-size:28px}h2{margin-top:32px;font-size:23px}p{max-width:96ch}code{font-size:.92em}
.notice{background:#fff4d9;color:#513a10;padding:18px 24px;border-radius:10px}.table{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px;margin:16px 0}
td,th{text-align:left;padding:12px;border-bottom:1px solid var(--line);vertical-align:top}th{background:#e8f2ef;color:#164c43}td:first-child{font-weight:600;min-width:165px}
details{border:1px solid var(--line);border-radius:8px;padding:12px 16px;margin:12px 0}summary{cursor:pointer;font-weight:650}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f6f8;padding:20px;border-radius:8px;font:13px/1.6 Consolas,monospace}
iframe{width:100%;height:680px;border:0;margin-top:16px}figure{margin:32px 0}figcaption{margin:12px 0;color:var(--muted)}img{max-width:100%;height:auto;border:1px solid var(--line);border-radius:8px}
button{border:1px solid #a4c8bd;background:#e9f8f2;color:#164c43;padding:10px 16px;border-radius:8px;cursor:pointer;font-weight:600}
@media print{body{background:white;font-size:11pt}header{padding:18px;background:white;color:black}header p{color:black}section{padding:12px;border:0}button{display:none}iframe{height:500px}details{break-inside:avoid}h2,h3,figcaption{break-after:avoid}}
</style></head><body><header><div class="tag">UTEC · Big Data · Grupo 7</div><h1>''' + html.escape(title) + '''</h1>
<p>Herles Pinedo · David Jimenez · José Nomberto</p><p>Arquitectura y procesamiento para la movilidad integrada de DataLandia.</p>
<button onclick="document.querySelectorAll('details').forEach(d=>d.open=true);window.print()">Imprimir o guardar PDF</button></header>
<main><aside class="notice"><strong>Estado de la revisión:</strong> código corregido y pruebas locales. Las capturas son históricas.
No se ha desplegado esta versión ni validado sus consultas completas en GCP, Spark, Beam o Airflow.</aside>''' + ''.join(parts) + '</main></body></html>'
    if cloud_verified:
        page = page.replace('código corregido y pruebas locales. Las capturas son históricas.\nNo se ha desplegado esta versión ni validado sus consultas completas en GCP, Spark, Beam o Airflow.',
                            'ensayo OLAP P3 completado en BigQuery con datos sintéticos, job IDs y API contra Gold. '
                            'No se acredita despliegue Cloud Run ni validación de Spark, Beam o Airflow.')
    page = '\n'.join(line.rstrip() for line in page.splitlines()) + '\n'
    (BASE / target).write_text(page, encoding='utf-8')
    print(f'Generado {target}: {len(page.encode("utf-8")):,} bytes')
