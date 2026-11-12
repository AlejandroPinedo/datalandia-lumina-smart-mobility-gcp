#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_entrega2_html.py
Genera el HTML 100% AUTOCONTENIDO (Single-File Deliverable) para la ENTREGA 2 (P2 - 25%):
Procesamiento, Transformación y Modelado — Grupo 7: DataLandia (Lumina Smart Mobility).

Todo está embebido dentro de un único archivo HTML sin dependencias externas:
  1. Carátula Institucional UTEC + Integrantes del Grupo 7 (Herles Pinedo, David Jimenez, José Nomberto).
  2. Modo Pestañas Interactivas + Botón "📖 Mostrar Todo (Modo Lectura Continua / PDF)".
  3. Los 3 Diagramas Archify INTERACTIVOS (visor completo con zoom/pan/foco) incrustados en gzip+Base64, con SVG estático de respaldo para impresión.
  4. Todo el sustento teórico, fórmulas matemáticas, diccionario de datos DDL de las tablas Silver y Gold, y justificación de particionamiento/clusterización.
  5. Visor de Código Fuente Completo (100% de las líneas de los 6 scripts de producción: SQL Medallion, PySpark Dataproc, Apache Beam Streaming, Simulador Pub/Sub, DAG Airflow y Verificador Multifuente) con botón de copiado.
  6. Las 5 Capturas Reales de GCP (C1, C2, C3, G4 actualizada, G5) incrustadas en Base64 con Lightbox de alta resolución.
"""

import base64
import gzip
import html as html_lib
import os
import re

BASE_DIR = "/Users/herivera/Documents/MAESTRIA/Big Data/Tareas"
CODE_DIR = os.path.join(BASE_DIR, "Avance1_P1_Lumina_Ingesta_Storage")


def load_archify_diagrams():
    """Extrae los 3 diagramas vectoriales Archify de nivel enterprise."""
    master_path = os.path.join(BASE_DIR, "PROYECTO_MASTER_DATALANDIA_LUMINA.html")
    with open(master_path, "r", encoding="utf-8") as f:
        master = f.read()

    # 1. Diagrama Principal de Arquitectura E2E (con su <style> scoped)
    s_arch = master.find('<div class="diagram-preview preview-lumina-arch-main"')
    e_arch = master.find("</div></figure>", s_arch) + 6
    arch_html = master[s_arch:e_arch]

    # 2. Diagrama ERD / Esquema Estrella Gold (con su <style> scoped)
    s_erd = master.find('<div class="diagram-preview preview-lumina-erd"')
    e_erd = master.find("</div></figure>", s_erd) + 6
    erd_html = master[s_erd:e_erd]

    # 3. Diagrama de Pipeline de Procesamiento y Cruce Multifuente (02_lumina_dataflow_multifuente.html)
    df_path = os.path.join(BASE_DIR, "Visualizaciones_Archify", "02_lumina_dataflow_multifuente.html")
    with open(df_path, "r", encoding="utf-8") as f:
        df_page = f.read()
    s_svg = df_page.find('<svg viewBox="0 0 1068 632"')
    e_svg = df_page.find("</svg>", s_svg) + 6
    df_svg = df_page[s_svg:e_svg]

    # Aislar IDs internos del SVG de Dataflow para evitar colisiones DOM
    ids = set(re.findall(r'\bid="([^"]+)"', df_svg))
    for old_id in sorted(ids, key=len, reverse=True):
        new_id = f"preview-df-{old_id}"
        df_svg = re.sub(rf'\bid="{re.escape(old_id)}"', f'id="{new_id}"', df_svg)
        df_svg = df_svg.replace(f"url(#{old_id})", f"url(#{new_id})")
        df_svg = df_svg.replace(f'href="#{old_id}"', f'href="#{new_id}"')
    df_svg = df_svg.replace(
        'aria-labelledby="archify-diagram-title archify-diagram-description"',
        'aria-labelledby="preview-df-archify-diagram-title preview-df-archify-diagram-description"',
    )

    df_html = (
        '<div class="diagram-preview preview-lumina-arch-main" role="img" '
        'aria-label="Pipeline de Procesamiento Híbrido y Cruce Multifuente en GCP" '
        'style="--native-width:1068px">'
        f"{df_svg}</div>"
    )
    return arch_html, df_html, erd_html


def load_interactive_viewer_b64(filename: str) -> str:
    """Comprime (gzip) y codifica en Base64 un visor Archify interactivo completo para incrustarlo en el HTML."""
    path = os.path.join(BASE_DIR, "Visualizaciones_Archify", filename)
    with open(path, "rb") as f:
        raw = f.read()
    return base64.b64encode(gzip.compress(raw, compresslevel=9)).decode("ascii")


def encode_image_b64(filename: str) -> str:
    """Convierte una captura PNG en un Data URI Base64 autocontenido."""
    img_path = os.path.join(BASE_DIR, "Capturas de pantalla", filename)
    with open(img_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def load_script_escaped(filename: str) -> tuple[str, int]:
    """Lee un script Python completo y retorna su contenido escapado para HTML y número de líneas."""
    path = os.path.join(CODE_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    lines = raw.count("\n") + 1
    return html_lib.escape(raw), lines


def build_html():
    arch_html, df_html, erd_html = load_archify_diagrams()
    viewer_arch_b64 = load_interactive_viewer_b64("01_lumina_arquitectura_end_to_end.html")
    viewer_df_b64 = load_interactive_viewer_b64("02_lumina_dataflow_multifuente.html")
    viewer_erd_b64 = load_interactive_viewer_b64("03_lumina_erd_modelo_datos.html")

    img_c1 = encode_image_b64("C1.png")
    img_c2 = encode_image_b64("C2.png")
    img_c3 = encode_image_b64("C3.png")
    img_g4 = encode_image_b64("G4.png")
    img_g5 = encode_image_b64("G5.png")

    code_06_sql, lines_06 = load_script_escaped("06_pipeline_medallion_silver_gold_ml.py")
    code_06b_spark, lines_06b = load_script_escaped("06b_pipeline_pyspark_dataproc_silver_gold.py")
    code_04_beam, lines_04 = load_script_escaped("04_pipeline_streaming_beam_bq.py")
    code_07_dag, lines_07 = load_script_escaped("07_dag_managed_airflow_composer.py")
    code_03_sim, lines_03 = load_script_escaped("03_simulador_streaming_iot_aot_pubsub.py")
    code_05_ver, lines_05 = load_script_escaped("05_verificar_cruce_multifuente_bq.py")

    html = f"""<!doctype html>
<html lang="es" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Entrega 2 (P2): Procesamiento, Transformación y Modelado — Grupo 7 DataLandia (Lumina) | UTEC Big Data</title>
  <style>
    :root {{
      --bg: #070b14;
      --card-bg: #0f172a;
      --card-alt: #131d33;
      --card-border: #1e293b;
      --card-hover: #1e293b;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #3b82f6;
      --accent-glow: rgba(59, 130, 246, 0.22);
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --purple: #8b5cf6;
      --amber: #f59e0b;
      --cyan: #06b6d4;
      --red: #ef4444;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
    }}
    [data-theme="light"] {{
      --bg: #f1f5f9;
      --card-bg: #ffffff;
      --card-alt: #f8fafc;
      --card-border: #cbd5e1;
      --card-hover: #e2e8f0;
      --text: #0f172a;
      --text-muted: #475569;
      --accent: #2563eb;
      --accent-glow: rgba(37, 99, 235, 0.12);
      --green: #059669;
      --green-glow: rgba(5, 150, 105, 0.12);
      --purple: #7c3aed;
      --amber: #d97706;
      --cyan: #0891b2;
      --red: #dc2626;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background-color: var(--bg);
      color: var(--text);
      font-family: var(--font-sans);
      line-height: 1.65;
      padding: 0;
      transition: background 0.25s, color 0.25s;
    }}
    .container {{
      max-width: 1420px;
      margin: 0 auto;
      padding: 28px 24px 64px;
    }}

    /* TOP HERO & INSTITUTIONAL COVER */
    header.hero {{
      background: linear-gradient(135deg, rgba(30, 58, 138, 0.38) 0%, rgba(15, 23, 42, 0.94) 55%, rgba(6, 78, 59, 0.38) 100%);
      border: 1px solid var(--card-border);
      border-radius: 22px;
      padding: 36px 40px;
      margin-bottom: 24px;
      position: relative;
      box-shadow: 0 16px 36px -8px rgba(0, 0, 0, 0.45);
    }}
    [data-theme="light"] header.hero {{
      background: linear-gradient(135deg, #eff6ff 0%, #ffffff 55%, #ecfdf5 100%);
      box-shadow: 0 10px 25px -5px rgba(15, 23, 42, 0.08);
    }}
    .top-actions {{
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 18px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--card-border);
    }}
    .btn-toggle {{
      background: var(--card-bg);
      color: var(--text);
      border: 1px solid var(--card-border);
      padding: 8px 15px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 7px;
      transition: all 0.2s;
      text-decoration: none;
    }}
    .btn-toggle:hover {{
      border-color: var(--accent);
      transform: translateY(-1px);
    }}
    .btn-toggle.active-mode {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .badge-bar {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 13px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }}
    .badge-primary {{ background: rgba(59,130,246,0.18); color: #60a5fa; border: 1px solid rgba(59,130,246,0.4); }}
    .badge-success {{ background: rgba(16,185,129,0.18); color: #34d399; border: 1px solid rgba(16,185,129,0.4); }}
    .badge-purple {{ background: rgba(139,92,246,0.18); color: #c084fc; border: 1px solid rgba(139,92,246,0.4); }}
    .badge-amber {{ background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid rgba(245,158,11,0.4); }}
    [data-theme="light"] .badge-primary {{ color: #1d4ed8; background: #dbeafe; }}
    [data-theme="light"] .badge-success {{ color: #047857; background: #d1fae5; }}
    [data-theme="light"] .badge-purple {{ color: #6d28d9; background: #ede9fe; }}
    [data-theme="light"] .badge-amber {{ color: #b45309; background: #fef3c7; }}

    .hero h1 {{
      font-size: 30px;
      font-weight: 800;
      line-height: 1.25;
      margin-bottom: 10px;
      letter-spacing: -0.02em;
      color: var(--text);
    }}
    .hero-subtitle {{
      color: var(--text-muted);
      font-size: 15.5px;
      max-width: 960px;
      margin-bottom: 24px;
    }}

    /* CARÁTULA E IDENTIFICACIÓN DEL GRUPO */
    .caratula-grid {{
      display: grid;
      grid-template-columns: 1.2fr 1.5fr 1.3fr;
      gap: 16px;
      padding-top: 22px;
      border-top: 1px solid var(--card-border);
    }}
    @media (max-width: 980px) {{
      .caratula-grid {{ grid-template-columns: 1fr; }}
      .top-actions {{ justify-content: flex-start; }}
    }}
    .caratula-box {{
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 16px 20px;
    }}
    [data-theme="light"] .caratula-box {{
      background: rgba(255, 255, 255, 0.85);
    }}
    .caratula-label {{
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--accent);
      margin-bottom: 6px;
    }}
    .caratula-title {{
      font-size: 15px;
      font-weight: 700;
      color: var(--text);
      margin-bottom: 4px;
    }}
    .caratula-sub {{
      font-size: 13px;
      color: var(--text-muted);
    }}
    .team-chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 8px;
    }}
    .member-chip {{
      display: inline-flex;
      align-items: center;
      gap: 7px;
      background: var(--card-alt);
      border: 1px solid var(--card-border);
      padding: 6px 12px;
      border-radius: 10px;
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
    }}
    .member-avatar {{
      width: 22px;
      height: 22px;
      border-radius: 50%;
      background: linear-gradient(135deg, #3b82f6, #10b981);
      color: #fff;
      font-size: 11px;
      font-weight: 800;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}

    /* METRICS RIBBON */
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .metric-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 20px 22px;
      position: relative;
      transition: transform 0.2s, border-color 0.2s;
    }}
    .metric-card:hover {{
      transform: translateY(-2px);
      border-color: var(--accent);
    }}
    .metric-title {{
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }}
    .metric-val {{
      font-size: 29px;
      font-weight: 800;
      color: var(--text);
      font-family: var(--font-mono);
      margin-bottom: 4px;
    }}
    .metric-sub {{
      font-size: 12.5px;
      color: var(--text-muted);
    }}
    .text-green {{ color: var(--green) !important; }}
    .text-blue {{ color: var(--accent) !important; }}
    .text-purple {{ color: var(--purple) !important; }}
    .text-amber {{ color: var(--amber) !important; }}

    /* TABS NAVIGATION */
    .tabs-nav {{
      display: flex;
      gap: 8px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 24px;
      overflow-x: auto;
      padding-bottom: 10px;
    }}
    .tab-btn {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      color: var(--text-muted);
      padding: 10px 15px;
      border-radius: 11px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 7px;
      white-space: nowrap;
      transition: all 0.2s;
    }}
    .tab-btn:hover {{
      background: var(--card-hover);
      color: var(--text);
    }}
    .tab-btn.active {{
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
      box-shadow: 0 4px 14px var(--accent-glow);
    }}
    .tab-pane {{
      display: none;
      animation: fadeIn 0.25s ease-out;
    }}
    .tab-pane.active {{
      display: block;
    }}
    body.show-all-sections .tab-pane {{
      display: block !important;
      margin-bottom: 32px;
    }}
    body.show-all-sections .tabs-nav {{
      display: none;
    }}
    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(5px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    /* CONTENT CARDS */
    .card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 18px;
      padding: 28px;
      margin-bottom: 24px;
      box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
    }}
    .card h2 {{
      font-size: 21px;
      font-weight: 700;
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--text);
    }}
    .card h3 {{
      font-size: 16.5px;
      font-weight: 700;
      margin: 20px 0 10px;
      color: var(--text);
    }}
    .card p {{
      color: var(--text-muted);
      font-size: 14.8px;
      margin-bottom: 14px;
    }}

    /* ARCHIFY DIAGRAM CONTAINERS */
    .archify-showcase-card {{
      background: #020617;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
      margin: 20px 0 28px;
      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.5);
    }}
    .archify-bar {{
      background: #0f172a;
      border-bottom: 1px solid #1e293b;
      padding: 12px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .archify-bar-title {{
      font-size: 13.5px;
      font-weight: 700;
      color: #f8fafc;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .archify-bar-actions {{
      display: flex;
      gap: 8px;
    }}
    .btn-diagram-action {{
      background: #1e293b;
      color: #e2e8f0;
      border: 1px solid #334155;
      padding: 6px 13px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.15s;
    }}
    .btn-diagram-action:hover {{
      background: #3b82f6;
      border-color: #60a5fa;
      color: #fff;
    }}
    .archify-canvas-wrap {{
      padding: 16px;
      overflow-x: auto;
      background: radial-gradient(circle at 50% 0%, rgba(30, 58, 138, 0.18), transparent 70%);
    }}
    .archify-canvas-wrap svg {{
      width: 100%;
      height: auto;
      display: block;
      min-width: 820px;
    }}

    .archify-canvas-wrap.has-viewer {{ padding: 0; }}
    .archify-viewer-frame {{
      width: 100%;
      height: clamp(560px, 46vw, 760px);
      border: 0;
      display: block;
      background: #020617;
    }}
    .archify-viewer-hint {{
      font-size: 12px;
      color: #94a3b8;
      padding: 8px 20px;
      border-top: 1px solid #1e293b;
      background: #0b1220;
    }}
    .archify-showcase-card:fullscreen {{ border-radius: 0; margin: 0; display: flex; flex-direction: column; }}
    .archify-showcase-card:fullscreen .archify-canvas-wrap {{ flex: 1; }}
    .archify-showcase-card:fullscreen .archify-viewer-frame {{ height: calc(100vh - 100px); }}
    @media print {{
      .archify-viewer-frame, .archify-viewer-hint, .top-actions, .archify-bar-actions {{ display: none !important; }}
      .archify-canvas-wrap .static-fallback {{ display: block !important; }}
    }}

    /* TABLES */
    .table-responsive {{
      overflow-x: auto;
      margin: 16px 0;
      border: 1px solid var(--card-border);
      border-radius: 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13.8px;
      text-align: left;
    }}
    th {{
      background: var(--card-alt);
      color: var(--text);
      font-weight: 700;
      padding: 13px 16px;
      border-bottom: 1px solid var(--card-border);
    }}
    td {{
      padding: 12px 16px;
      border-bottom: 1px solid var(--card-border);
      color: var(--text-muted);
      vertical-align: top;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: var(--card-hover); color: var(--text); }}
    .mono {{ font-family: var(--font-mono); font-size: 12.5px; }}

    /* CODE BLOCKS & INTEGRATED REPOSITORY VIEWER */
    pre {{
      background: #090d16;
      color: #e6edf3;
      padding: 18px 20px;
      border-radius: 12px;
      border: 1px solid #1e293b;
      font-family: var(--font-mono);
      font-size: 12.8px;
      overflow-x: auto;
      margin: 12px 0;
      line-height: 1.55;
    }}
    code {{
      font-family: var(--font-mono);
      color: #60a5fa;
      font-size: 0.92em;
    }}
    pre code {{ color: #e2e8f0; }}

    .repo-explorer {{
      border: 1px solid var(--card-border);
      border-radius: 16px;
      overflow: hidden;
      background: #090d16;
      margin-top: 18px;
    }}
    .repo-tabs {{
      display: flex;
      gap: 6px;
      padding: 10px 14px;
      background: #0f172a;
      border-bottom: 1px solid #1e293b;
      overflow-x: auto;
    }}
    .repo-tab-btn {{
      background: #1e293b;
      color: #94a3b8;
      border: 1px solid #334155;
      padding: 7px 13px;
      border-radius: 8px;
      font-family: var(--font-mono);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
      transition: all 0.15s;
    }}
    .repo-tab-btn:hover {{ color: #fff; border-color: #60a5fa; }}
    .repo-tab-btn.active {{
      background: #2563eb;
      color: #fff;
      border-color: #3b82f6;
    }}
    .repo-pane {{ display: none; }}
    .repo-pane.active {{ display: block; }}
    .repo-pane-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 18px;
      background: #111827;
      border-bottom: 1px solid #1e293b;
      font-size: 12.5px;
      color: #94a3b8;
    }}
    .repo-pane pre {{
      margin: 0;
      border: none;
      border-radius: 0;
      max-height: 620px;
      overflow-y: auto;
    }}

    /* FORMULA CALLOUT */
    .formula-box {{
      background: var(--card-alt);
      border-left: 4px solid var(--accent);
      border-radius: 0 12px 12px 0;
      padding: 18px 22px;
      margin: 16px 0;
      font-family: var(--font-mono);
      font-size: 13.5px;
      color: var(--text);
    }}

    /* SCREENSHOT GALLERY */
    .evidence-block {{
      background: var(--card-alt);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 24px;
      margin-bottom: 28px;
      box-shadow: 0 6px 24px rgba(0, 0, 0, 0.22);
    }}
    .evidence-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 12px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--card-border);
    }}
    .evidence-title {{
      font-size: 17px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--text);
    }}
    .img-wrapper {{
      position: relative;
      border-radius: 12px;
      overflow: hidden;
      border: 1px solid var(--card-border);
      background: #000;
      cursor: zoom-in;
      transition: border-color 0.2s, transform 0.2s;
    }}
    .img-wrapper:hover {{
      border-color: var(--accent);
      transform: scale(1.002);
    }}
    .img-wrapper img {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .img-caption {{
      margin-top: 10px;
      font-size: 12.5px;
      color: var(--text-muted);
      display: flex;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 8px;
    }}

    /* LIGHTBOX MODAL FOR IMAGES & FULLSCREEN SVG */
    .lightbox-modal {{
      display: none;
      position: fixed;
      z-index: 9999;
      top: 0; left: 0;
      width: 100vw; height: 100vh;
      background: rgba(2, 6, 23, 0.95);
      backdrop-filter: blur(8px);
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }}
    .lightbox-modal.active {{
      display: flex;
    }}
    .lightbox-toolbar {{
      position: absolute;
      top: 18px;
      left: 24px;
      right: 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      z-index: 10001;
      pointer-events: none;
    }}
    .lightbox-toolbar > * {{
      pointer-events: auto;
    }}
    .lightbox-title {{
      background: rgba(15, 23, 42, 0.9);
      border: 1px solid #334155;
      color: #f8fafc;
      padding: 8px 16px;
      border-radius: 10px;
      font-size: 13.5px;
      font-weight: 700;
    }}
    .lightbox-controls {{
      display: flex;
      gap: 8px;
    }}
    .lightbox-btn {{
      background: #1e293b;
      color: #fff;
      border: 1px solid #475569;
      padding: 8px 14px;
      border-radius: 9px;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }}
    .lightbox-btn:hover {{
      background: #3b82f6;
      border-color: #60a5fa;
    }}
    .lightbox-stage {{
      width: 96vw;
      height: 86vh;
      overflow: auto;
      display: flex;
      align-items: center;
      justify-content: center;
      margin-top: 36px;
    }}
    .lightbox-img {{
      max-width: 100%;
      max-height: 100%;
      border-radius: 12px;
      box-shadow: 0 25px 50px rgba(0, 0, 0, 0.85);
      border: 1px solid #334155;
      transition: transform 0.2s ease;
    }}
    .lightbox-svg-holder {{
      width: 100%;
      background: #020617;
      border: 1px solid #334155;
      border-radius: 14px;
      padding: 24px;
      transition: transform 0.2s ease;
      transform-origin: center top;
    }}

    .grid-2 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(440px, 1fr));
      gap: 20px;
    }}
    @media (max-width: 600px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>

<div class="container">

  <!-- HERO HEADER & CARÁTULA INSTITUCIONAL UTEC -->
  <header class="hero">
    <div class="top-actions">
      <button class="btn-toggle" onclick="toggleAllSections()" id="allSectionsBtn" title="Despliega las 8 secciones simultáneamente para lectura continua o exportar a PDF">📖 Mostrar Todo (Lectura Continua)</button>
      <button class="btn-toggle" onclick="toggleTheme()" id="themeBtn">🌓 Alternar Tema</button>
      <a href="https://github.com/AlejandroPinedo/datalandia-lumina-smart-mobility-gcp" target="_blank" class="btn-toggle">
        <svg width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.012 8.012 0 0 0 16 8c0-4.42-3.58-8-8-8z"/></svg>
        Repositorio GitHub
      </a>
    </div>

    <div class="badge-bar">
      <span class="badge badge-primary">🏛️ Universidad de Ingeniería y Tecnología (UTEC)</span>
      <span class="badge badge-success">✅ ENTREGA 2: Proyecto Parte 2 (P2 · 25%) — Mid-term de Código</span>
      <span class="badge badge-purple">☁️ GCP Project: bigdata-herles-pinedo (us-central1)</span>
      <span class="badge badge-amber">📦 Entregable 100% Autocontenido</span>
    </div>

    <h1>🏙️ Proyecto Parte 2 (P2): Procesamiento, Transformación y Modelado de Datos</h1>
    <p class="hero-subtitle">
      Implementación práctica End-to-End de la capa de cómputo híbrido (Streaming de baja latencia en <strong>Cloud Pub/Sub + Apache Beam</strong> y procesamiento masivo por lotes en <strong>BigQuery Serverless SQL + Apache Spark / PySpark</strong>), cruce de las 4 fuentes heterogéneas y modelado dimensional en estrella para la <strong>Autoridad de Tránsito Unificada "Lumina" (DataLandia Metropolitan)</strong>.
    </p>

    <!-- CARÁTULA DEL GRUPO 7 -->
    <div class="caratula-grid">
      <div class="caratula-box">
        <div class="caratula-label">📚 Curso</div>
        <div class="caratula-title">Curso: Big Data</div>
        <div class="caratula-sub">
          Maestría en Ciencia de Datos e Inteligencia Artificial — UTEC
        </div>
      </div>

      <div class="caratula-box">
        <div class="caratula-label">👥 Grupo 7: DataLandia — Integrantes del Equipo</div>
        <div class="team-chips">
          <span class="member-chip"><span class="member-avatar">HP</span> Herles Pinedo</span>
          <span class="member-chip"><span class="member-avatar">DJ</span> David Jimenez</span>
          <span class="member-chip"><span class="member-avatar">JN</span> José Nomberto</span>
        </div>
        <div class="caratula-sub" style="margin-top:8px;">
          <strong>Caso de Estudio:</strong> Transporte y Movilidad Urbana Inteligente (Smart Mobility)
        </div>
      </div>

      <div class="caratula-box">
        <div class="caratula-label">🎯 Alcance Evaluativo (Rúbrica P2 · 25%)</div>
        <div class="caratula-sub">
          • <strong>Pipelines de Cómputo:</strong> BigQuery SQL, PySpark (Dataproc) y Apache Beam.<br>
          • <strong>Procesamiento Híbrido:</strong> Real-Time (&lt; 2.5s con DLQ) + Batch (5M filas).<br>
          • <strong>Modelado Analítico:</strong> Esquema en Estrella Gold particionado y clusterizado.
        </div>
      </div>
    </div>
  </header>

  <!-- METRICS RIBBON -->
  <div class="metrics-grid">
    <div class="metric-card">
      <div class="metric-title">Viajes Limpios en Silver <span>🧹</span></div>
      <div class="metric-val text-blue">4,998,926</div>
      <div class="metric-sub">1,349.06 MB · Deduplicación <code>QUALIFY ROW_NUMBER() = 1</code></div>
    </div>
    <div class="metric-card">
      <div class="metric-title">Fact Table Gold Multimodal <span>⭐</span></div>
      <div class="metric-val text-green">4,998,926</div>
      <div class="metric-sub">1.80 GB lógicos (321.44 MB físicos · 59 particiones)</div>
    </div>
    <div class="metric-card">
      <div class="metric-title">Gobernanza Knowledge Catalog <span>🛡️</span></div>
      <div class="metric-val text-purple">100.0%</div>
      <div class="metric-sub">3 Reglas DQ auditadas en BigQuery (SLA &ge; 95.0%)</div>
    </div>
    <div class="metric-card">
      <div class="metric-title">Superación Cuello Vial 24 km/h <span>🚄</span></div>
      <div class="metric-val text-amber">42.4 km/h</div>
      <div class="metric-sub">Riel OART exclusivo · Ahorro medio de 14.8 min/viaje</div>
    </div>
  </div>

  <!-- TABS NAVIGATION -->
  <nav class="tabs-nav">
    <button class="tab-btn active" onclick="openTab(event, 'tab-hibrida')">🔀 1. Arquitectura Híbrida & Diagramas</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-rubrica')">📋 2. Cumplimiento Rúbrica P2</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-motores')">⚙️ 3. Motores & Código Completo (6 Scripts)</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-cruce')">🔄 4. Cruce Multifuente & Fórmulas</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-estrella')">⭐ 5. Modelo Estrella Gold & DDL</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-calidad')">🛡️ 6. Calidad & Gobernanza DQ</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-airflow')">⏱️ 7. Orquestación Airflow</button>
    <button class="tab-btn" onclick="openTab(event, 'tab-evidencias')">📸 8. Evidencias Reales en GCP (5 Capturas)</button>
  </nav>

  <!-- TAB 1: ARQUITECTURA HÍBRIDA Y DIAGRAMAS INTERACTIVOS ARCHIFY -->
  <div id="tab-hibrida" class="tab-pane active">
    <div class="card">
      <h2>🔀 1. Arquitectura de Cómputo Híbrida y Flujo de Transformación en GCP</h2>
      <p>
        La plataforma de datos de <strong>Lumina Smart Mobility</strong> implementa una arquitectura <strong>Lakehouse Medallion Híbrida (Lambda / Kappa modernizada)</strong> sobre Google Cloud Platform. El diseño separa físicamente el <strong>Carril de Tiempo Real (Streaming IoT &lt; 2.5s)</strong> del <strong>Carril Masivo por Lotes (Batch Medallion de 5,000,000 de registros)</strong>, convergiendo en la capa analítica gobernada <strong>BigQuery Gold</strong> y el bucket abierto <strong>Cloud Storage Parquet</strong>.
      </p>

      <!-- DIAGRAMA ARCHIFY 1: ARQUITECTURA END-TO-END -->
      <div class="archify-showcase-card">
        <div class="archify-bar">
          <div class="archify-bar-title">
            <span>🏛️</span>
            <span>Diagrama Maestro 1: Arquitectura Híbrida End-to-End de Lumina en GCP (Carriles Streaming vs. Batch + Gobierno Transversal)</span>
          </div>
          <div class="archify-bar-actions">
            <button type="button" class="btn-diagram-action" onclick="fullscreenDiagram('diag-wrap-1')">⛶ Pantalla Completa</button>
          </div>
        </div>
        <div class="archify-canvas-wrap" id="diag-wrap-1" data-viewer="arch">
          <div class="static-fallback">{arch_html}</div>
        </div>
        <div class="archify-viewer-hint">🖱️ Diagrama interactivo embebido: arrastra para desplazar, usa la rueda para hacer zoom y haz clic en un nodo para ver sus conexiones.</div>
      </div>

      <!-- DIAGRAMA ARCHIFY 2: PIPELINE DE PROCESAMIENTO Y CRUCE MULTIFUENTE -->
      <div class="archify-showcase-card">
        <div class="archify-bar">
          <div class="archify-bar-title">
            <span>⚡</span>
            <span>Diagrama Maestro 2: Pipeline de Cómputo, Validación Física (DLQ), Enriquecimiento GIS y Cruce de las 4 Fuentes</span>
          </div>
          <div class="archify-bar-actions">
            <button type="button" class="btn-diagram-action" onclick="fullscreenDiagram('diag-wrap-2')">⛶ Pantalla Completa</button>
          </div>
        </div>
        <div class="archify-canvas-wrap" id="diag-wrap-2" data-viewer="df">
          <div class="static-fallback">{df_html}</div>
        </div>
        <div class="archify-viewer-hint">🖱️ Diagrama interactivo embebido: arrastra para desplazar, usa la rueda para hacer zoom y haz clic en un nodo para ver sus conexiones.</div>
      </div>

      <div class="grid-2">
        <div class="caratula-box">
          <div class="caratula-label">⚡ Carril Rápido (Streaming · Baja Latencia)</div>
          <div class="caratula-sub">
            1. <strong>Captura:</strong> Tópico <code>lumina-iot-aot-telemetry-topic</code> y suscripción <code>lumina-iot-aot-telemetry-sub</code> en Cloud Pub/Sub.<br>
            2. <strong>Procesamiento en Vuelo:</strong> Pipeline <code>04_pipeline_streaming_beam_bq.py</code> (Apache Beam / Cloud Dataflow) que evalúa coherencia física de vibración (0–100 mm/s) y temperatura (-40°C a 180°C).<br>
            3. <strong>Tolerancia a Fallos (DLQ):</strong> Eventos corruptos o fuera de umbral se desvían automáticamente a <code>lumina_bronze_raw.ingesta_deadletter_raw</code> sin detener el stream.
          </div>
        </div>
        <div class="caratula-box">
          <div class="caratula-label">🏗️ Carril Masivo (Batch · Medallion Lakehouse)</div>
          <div class="caratula-sub">
            1. <strong>Data Lake Multicapacidad:</strong> Bucket <code>gs://bigdata-herles-pinedo-lumina-datalake/</code> estructurado en <code>bronze/</code>, <code>raw/tlc_trips_5m/</code> (84 shards Parquet), <code>silver/</code> y <code>gold/</code>.<br>
            2. <strong>Transformación Silver:</strong> Deduplicación determinista (<code>QUALIFY ROW_NUMBER() = 1</code>), geocálculos nativos (<code>ST_GEOGPOINT</code>) y seudonimización criptográfica <code>SHA-256</code> con sal.<br>
            3. <strong>Modelado Gold:</strong> Esquema Estrella con 4,998,926 viajes enriquecidos con velocidades de congestión y demografía censal.
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 2: CUMPLIMIENTO DE LA RÚBRICA OFICIAL -->
  <div id="tab-rubrica" class="tab-pane">
    <div class="card">
      <h2>📋 2. Matriz de Trazabilidad y Cumplimiento de la Rúbrica Oficial UTEC (P2 · 25%)</h2>
      <p>
        Cada punto exigido en la sección <strong>"3. Proyecto Parte 2 (P2): Procesamiento, Transformación y Modelado (25%)"</strong> del documento oficial del curso ha sido implementado, ejecutado y certificado en Google Cloud Platform:
      </p>

      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Criterio de Evaluación UTEC</th>
              <th>Requisito de la Rúbrica</th>
              <th>Solución Técnica Implementada por el Grupo 7</th>
              <th>Scripts Funcionales Embebidos</th>
              <th>Evidencia Verificada en GCP</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>1. Pipelines de Cómputo / Transformación</strong></td>
              <td>Código funcional o scripts en motores de procesamiento (Spark, Flink, SQL, etc.) para la limpieza, enriquecimiento y cruce de los datos heterogéneos.</td>
              <td>
                • <strong>Motor 1 (BigQuery Serverless SQL):</strong> Deduplicación <code>QUALIFY ROW_NUMBER() = 1</code>, puntos geoespaciales <code>ST_GEOGPOINT</code>, seudonimización <code>SHA256</code> y Stored Procedure <code>sp_orquestar_medallion_lumina()</code>.<br>
                • <strong>Motor 2 (Apache Spark / PySpark):</strong> Job distribuido compatible con Cloud Dataproc usando <code>Window.partitionBy</code> y Adaptive Query Execution (AQE).<br>
                • <strong>Motor 3 (Apache Beam / Dataflow):</strong> Grafo <code>beam.DoFn</code> con salidas etiquetadas (<code>TaggedOutput('deadletter')</code>).
              </td>
              <td class="mono">
                06_pipeline_medallion_silver_gold_ml.py<br><br>
                06b_pipeline_pyspark_dataproc_silver_gold.py<br><br>
                04_pipeline_streaming_beam_bq.py
              </td>
              <td>
                <strong>4,998,926 registros</strong> procesados en Silver y Gold.<br>
                Capturas <strong>C1, C2 y G5</strong>.
              </td>
            </tr>
            <tr>
              <td><strong>2. Procesamiento Híbrido</strong></td>
              <td>Implementación funcional de transformaciones para flujos en tiempo real (baja latencia) y procesamientos masivos por lotes (batch).</td>
              <td>
                • <strong>Tiempo Real (Streaming):</strong> Publicación continua de sensores AoT/OART en Cloud Pub/Sub, validación física sub-3 segundos e inserción streaming en BigQuery + desvío de anomalías a DLQ.<br>
                • <strong>Por Lotes (Batch):</strong> Consolidación masiva de 5M de viajes históricos (84 archivos Parquet en GCS) cruzados con velocidades viales de Uber Movement y Censo INE, orquestado con Cloud Composer (Airflow).
              </td>
              <td class="mono">
                03_simulador_streaming_iot_aot_pubsub.py<br><br>
                04_pipeline_streaming_beam_bq.py<br><br>
                07_dag_managed_airflow_composer.py
              </td>
              <td>
                Tópico Pub/Sub activo y Data Lake Medallion completo.<br>
                Capturas <strong>C3 y G4</strong>.
              </td>
            </tr>
            <tr>
              <td><strong>3. Modelado para Analítica</strong></td>
              <td>Creación de las estructuras finales de datos (tablas consolidadas, esquemas en estrella/copo de nieve o colecciones optimizadas) listos para su consumo.</td>
              <td>
                • <strong>Esquema en Estrella (Star Schema) en <code>lumina_gold_governed</code>:</strong><br>
                &nbsp;&nbsp;1. <code>fact_movilidad_multimodal_gold</code> (Particionada por día en <code>travel_date</code> y clusterizada por <code>county_id, origin_zone_id, transport_mode</code>).<br>
                &nbsp;&nbsp;2. <code>dim_zonas_condados_gold</code> (Dimensión conformada con geometrías GIS).<br>
                &nbsp;&nbsp;3. <code>kpi_ejecutivo_condados_gold</code> (Data Mart pre-agregado para Looker Studio y API REST).
              </td>
              <td class="mono">
                Dataset BQ: lumina_gold_governed<br><br>
                GCS: gs://.../gold/*.parquet<br><br>
                05_verificar_cruce_multifuente_bq.py
              </td>
              <td>
                59 particiones activas, compresión física a 321.44 MB.<br>
                Captura <strong>C1</strong>.
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- TAB 3: MOTORES DE CÓMPUTO Y EXPLORADOR DE CÓDIGO FUENTE COMPLETO -->
  <div id="tab-motores" class="tab-pane">
    <div class="card">
      <h2>⚙️ 3. Motores de Procesamiento y Código Fuente Completo Embebido (6 Scripts de Producción)</h2>
      <p>
        Dado que este documento HTML constituye el entregable integral de la <strong>Entrega 2 (P2)</strong>, a continuación se incluye tanto el resumen comparativo de los 3 motores como el <strong>Visor de Código Fuente Completo (100% de las líneas de código de los 6 scripts del proyecto)</strong> para revisión directa sin necesidad de abrir archivos externos:
      </p>

      <!-- VISOR DE REPOSITORIO INTEGRADO (6 SCRIPTS COMPLETOS) -->
      <div class="repo-explorer">
        <div class="repo-tabs">
          <button type="button" class="repo-tab-btn active" onclick="openRepoFile(event, 'repo-file-06')">1. 06_pipeline_medallion_silver_gold_ml.py ({lines_06} lín.)</button>
          <button type="button" class="repo-tab-btn" onclick="openRepoFile(event, 'repo-file-06b')">2. 06b_pipeline_pyspark_dataproc_silver_gold.py ({lines_06b} lín.)</button>
          <button type="button" class="repo-tab-btn" onclick="openRepoFile(event, 'repo-file-04')">3. 04_pipeline_streaming_beam_bq.py ({lines_04} lín.)</button>
          <button type="button" class="repo-tab-btn" onclick="openRepoFile(event, 'repo-file-07')">4. 07_dag_managed_airflow_composer.py ({lines_07} lín.)</button>
          <button type="button" class="repo-tab-btn" onclick="openRepoFile(event, 'repo-file-03')">5. 03_simulador_streaming_iot_aot_pubsub.py ({lines_03} lín.)</button>
          <button type="button" class="repo-tab-btn" onclick="openRepoFile(event, 'repo-file-05')">6. 05_verificar_cruce_multifuente_bq.py ({lines_05} lín.)</button>
        </div>

        <div id="repo-file-06" class="repo-pane active">
          <div class="repo-pane-header">
            <span>📄 <strong>06_pipeline_medallion_silver_gold_ml.py</strong> — Pipeline Principal Medallion (Bronze ➔ Silver ➔ DQ ➔ Gold Star Schema + Stored Procedure)</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-06', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-06">{code_06_sql}</code></pre>
        </div>

        <div id="repo-file-06b" class="repo-pane">
          <div class="repo-pane-header">
            <span>📄 <strong>06b_pipeline_pyspark_dataproc_silver_gold.py</strong> — Pipeline Distribuido en Apache Spark (PySpark / Cloud Dataproc con Window Functions y AQE)</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-06b', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-06b">{code_06b_spark}</code></pre>
        </div>

        <div id="repo-file-04" class="repo-pane">
          <div class="repo-pane-header">
            <span>📄 <strong>04_pipeline_streaming_beam_bq.py</strong> — Pipeline Streaming en Apache Beam / Cloud Dataflow con Validación Física y Dead-Letter Queue (DLQ)</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-04', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-04">{code_04_beam}</code></pre>
        </div>

        <div id="repo-file-07" class="repo-pane">
          <div class="repo-pane-header">
            <span>📄 <strong>07_dag_managed_airflow_composer.py</strong> — DAG de Orquestación Automatizada en Cloud Composer / Apache Airflow</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-07', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-07">{code_07_dag}</code></pre>
        </div>

        <div id="repo-file-03" class="repo-pane">
          <div class="repo-pane-header">
            <span>📄 <strong>03_simulador_streaming_iot_aot_pubsub.py</strong> — Productor de Eventos IoT en Tiempo Real hacia Cloud Pub/Sub</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-03', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-03">{code_03_sim}</code></pre>
        </div>

        <div id="repo-file-05" class="repo-pane">
          <div class="repo-pane-header">
            <span>📄 <strong>05_verificar_cruce_multifuente_bq.py</strong> — Auditoría y Verificación Analítica del Cruce de las 4 Fuentes en BigQuery</span>
            <button type="button" class="btn-diagram-action" onclick="copyCodeBlock('code-raw-05', this)">📋 Copiar Código Completo</button>
          </div>
          <pre><code id="code-raw-05">{code_05_ver}</code></pre>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 4: CRUCE MULTIFUENTE Y FÓRMULAS MATEMÁTICAS -->
  <div id="tab-cruce" class="tab-pane">
    <div class="card">
      <h2>🔄 4. Integración y Cruce de las 4 Fuentes Heterogéneas: Superando el Cuello Vial de 24 km/h</h2>
      <p>
        El objetivo central del negocio de Lumina es demostrar con datos empíricos que la combinación de primera/última milla con la red ferroviaria eléctrica <strong>OART</strong> supera el cuello de botella vehicular de <strong>24 km/h (15 mph)</strong> que sufren los automóviles privados en DataLandia. Para ello, el pipeline Gold ejecuta un <strong>Geo-Join / Temporal-Join</strong> de las 4 fuentes oficiales:
      </p>

      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Fuente Oficial</th>
              <th>Tabla Silver Limpia</th>
              <th>Volumen Auditado</th>
              <th>Llaves de Cruce (Join Keys)</th>
              <th>Variables Aportadas al Modelo Gold</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>F1: Viajes Multimodales (NYC TLC Adaptado)</strong></td>
              <td><code>lumina_silver_clean.tlc_trips_silver</code></td>
              <td><strong>4,998,926 filas</strong><br>(1,349.06 MB)</td>
              <td class="mono">origin_zone_id, dest_zone_id, hod, DATE(pickup_datetime)</td>
              <td>Duración real Lumina (<code>lumina_duration_min</code>), velocidad efectiva (<code>lumina_speed_kmh</code>), tarifa unificada y costo por km (<code>$0.51 - $0.69/km</code>).</td>
            </tr>
            <tr>
              <td><strong>F2: Congestión Vial (Uber Movement Speeds)</strong></td>
              <td><code>lumina_silver_clean.uber_movement_silver</code></td>
              <td><strong>504 corredores horarios</strong><br>(0.06 MB)</td>
              <td class="mono">origin_zone_id = u.origin_zone_id AND dest_zone_id = u.dest_zone_id AND hod = u.hod</td>
              <td>Velocidad real de calle (<code>street_traffic_speed_kmh</code> ~21.5–24.1 km/h en hora punta) y flag booleano <code>is_severe_congestion_24kmh</code>.</td>
            </tr>
            <tr>
              <td><strong>F3: Telemetría IoT y Rieles (Array of Things + OART)</strong></td>
              <td><code>lumina_silver_clean.iot_aot_telemetry_silver</code></td>
              <td><strong>Streaming continuo</strong><br>(24 estaciones OART)</td>
              <td class="mono">origin_zone_id = i.zone_id (agregado por estación)</td>
              <td>Ocupación de trenes (<code>station_oart_load_pct</code>), vibración máxima de riel (<code>max_vibration_mms</code>) y alertas de mantenimiento predictivo (<code>station_pdm_alerts</code>).</td>
            </tr>
            <tr>
              <td><strong>F4: Censo Demográfico (INE DataLandia)</strong></td>
              <td><code>lumina_silver_clean.dim_geografia_censo_silver</code></td>
              <td><strong>8 Condados Maestros</strong><br>(9.25M habitantes)</td>
              <td class="mono">origin_zone_id = c.zone_id</td>
              <td>Demografía (<code>pct_adulto_mayor_65</code>, <code>pct_jovenes_18_34</code>), cobertura OART a &lt; 8 km (<code>89%–96.5%</code>) e índice de prioridad de primera milla.</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h3>Fórmulas de Negocio Implementadas en el Pipeline Gold</h3>
      <div class="formula-box">
        1. <strong>Duración Benchmark Auto Particular (min)</strong> = ROUND((trip_distance_km / IFNULL(u.avg_speed_kmh, 23.8)) * 60.0, 2)<br>
        2. <strong>Costo Total Benchmark Auto Particular (USD)</strong> = ROUND(3.20 + (trip_distance_km * 1.18), 2) &nbsp;[Peaje/Estacionamiento base $3.20 + $1.18/km operativo]<br>
        3. <strong>Ahorro Económico Neto por Viaje (USD)</strong> = car_benchmark_total_cost_usd - lumina_total_fare_usd<br>
        4. <strong>Tiempo Ahorrado por Ciudadano (min)</strong> = car_benchmark_duration_min - lumina_duration_min<br>
        5. <strong>Seudonimización Criptográfica (Cloud DLP)</strong> = TO_HEX(SHA256(CONCAT(IFNULL(rider_id_hash, 'anon'), '_lumina_dlp_salt')))
      </div>
    </div>
  </div>

  <!-- TAB 5: MODELO ESTRELLA GOLD (ERD) Y DICCIONARIO DDL -->
  <div id="tab-estrella" class="tab-pane">
    <div class="card">
      <h2>⭐ 5. Modelado Dimensional para Analítica (Esquema en Estrella Gold y Diccionario de Datos)</h2>
      <p>
        El modelado final en el dataset <code>lumina_gold_governed</code> sigue un diseño dimensional en <strong>Esquema Estrella (Star Schema)</strong> acompañado de una tabla agregada OLAP (<code>kpi_ejecutivo_condados_gold</code>) para alimentar tableros ejecutivos y el simulador de rutas en vivo con latencia sub-segundo:
      </p>

      <!-- DIAGRAMA ARCHIFY 3: MODELO ENTIDAD RELACIÓN (ERD) -->
      <div class="archify-showcase-card">
        <div class="archify-bar">
          <div class="archify-bar-title">
            <span>📐</span>
            <span>Diagrama Maestro 3: Modelo Entidad-Relación (ERD) y Esquema en Estrella en BigQuery Gold</span>
          </div>
          <div class="archify-bar-actions">
            <button type="button" class="btn-diagram-action" onclick="fullscreenDiagram('diag-wrap-3')">⛶ Pantalla Completa</button>
          </div>
        </div>
        <div class="archify-canvas-wrap" id="diag-wrap-3" data-viewer="erd">
          <div class="static-fallback">{erd_html}</div>
        </div>
        <div class="archify-viewer-hint">🖱️ Diagrama interactivo embebido: arrastra para desplazar, usa la rueda para hacer zoom y haz clic en un nodo para ver sus conexiones.</div>
      </div>

      <h3>Resumen Físico de las Tablas del Dataset <code>lumina_gold_governed</code></h3>
      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Tabla en <code>lumina_gold_governed</code></th>
              <th>Rol Dimensional</th>
              <th>Volumetría Real Auditada</th>
              <th>Particionamiento Físico</th>
              <th>Clusterización (Block Pruning)</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><code>fact_movilidad_multimodal_gold</code></td>
              <td><strong>Tabla de Hechos Central (Fact Table)</strong></td>
              <td><strong>4,998,926 filas</strong><br>1.80 GB lógicos · <strong>321.44 MB físicos</strong> (compresión Capacitor 5.6x)</td>
              <td><code>PARTITION BY travel_date</code><br>(59 particiones diarias activas · ~5.4 MB físicos/día)</td>
              <td><code>CLUSTER BY county_id, origin_zone_id, transport_mode</code></td>
            </tr>
            <tr>
              <td><code>dim_zonas_condados_gold</code></td>
              <td><strong>Dimensión Geográfica y Demográfica</strong></td>
              <td><strong>8 filas maestras</strong><br>(8 Condados / Polos con geometrías <code>ST_GEOGPOINT</code>)</td>
              <td>Sin partición (Tabla dimensión compacta para Broadcast Join en memoria)</td>
              <td><code>CLUSTER BY county_id, zone_id</code></td>
            </tr>
            <tr>
              <td><code>kpi_ejecutivo_condados_gold</code></td>
              <td><strong>Data Mart Ejecutivo Pre-agregado (OLAP)</strong></td>
              <td><strong>8 filas consolidadas</strong><br>(1 por Condado con recomendaciones prescriptivas)</td>
              <td>Actualización atómica vía Stored Procedure</td>
              <td><code>CLUSTER BY county_id, zone_id</code></td>
            </tr>
          </tbody>
        </table>
      </div>

      <h3>Diccionario de Datos Detallado: Tabla de Hechos <code>fact_movilidad_multimodal_gold</code></h3>
      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Columna</th>
              <th>Tipo BigQuery</th>
              <th>Rol / Índice</th>
              <th>Origen Multifuente</th>
              <th>Descripción Técnica y de Negocio</th>
            </tr>
          </thead>
          <tbody>
            <tr><td class="mono">trip_id</td><td class="mono">STRING</td><td>PK Lógica</td><td>F1 (TLC Trips)</td><td>Identificador único deduplicado del viaje multimodal.</td></tr>
            <tr><td class="mono">travel_date</td><td class="mono">DATE</td><td><strong>PARTITION KEY</strong></td><td>F1 (TLC Trips)</td><td>Fecha de viaje utilizada para el particionamiento diario (59 particiones).</td></tr>
            <tr><td class="mono">county_id</td><td class="mono">STRING</td><td><strong>CLUSTER KEY 1 / FK</strong></td><td>F4 (Censo INE)</td><td>Llave foránea del condado metropolitano (<code>CNT-01</code> a <code>CNT-08</code>).</td></tr>
            <tr><td class="mono">origin_zone_id</td><td class="mono">STRING</td><td><strong>CLUSTER KEY 2 / FK</strong></td><td>F1 + F4</td><td>Zona de origen del viaje (<code>ZDL-001</code> a <code>ZDL-008</code>).</td></tr>
            <tr><td class="mono">dest_zone_id</td><td class="mono">STRING</td><td>FK</td><td>F1 (TLC Trips)</td><td>Zona de destino del viaje.</td></tr>
            <tr><td class="mono">transport_mode</td><td class="mono">STRING</td><td><strong>CLUSTER KEY 3</strong></td><td>F1 (TLC Trips)</td><td>Modo multimodal (<code>LUMINA_MULTIMODAL_OART</code>, <code>LUMINA_EV_MINIVAN_POOL</code>, etc.).</td></tr>
            <tr><td class="mono">connected_oart_station_id</td><td class="mono">STRING</td><td>FK Estación</td><td>F1 + F3 (OART)</td><td>Estación de interconexión férrea OART utilizada en el trayecto.</td></tr>
            <tr><td class="mono">trip_distance_km</td><td class="mono">FLOAT64</td><td>Métrica</td><td>F1 (TLC Trips)</td><td>Distancia total recorrida en kilómetros.</td></tr>
            <tr><td class="mono">lumina_duration_min</td><td class="mono">FLOAT64</td><td>Métrica</td><td>F1 (TLC Trips)</td><td>Duración real del viaje en la red integrada Lumina (minutos).</td></tr>
            <tr><td class="mono">lumina_speed_kmh</td><td class="mono">FLOAT64</td><td>Métrica</td><td>F1 (TLC Trips)</td><td>Velocidad efectiva alcanzada por el ciudadano en Lumina (km/h).</td></tr>
            <tr><td class="mono">lumina_cost_per_km_usd</td><td class="mono">FLOAT64</td><td>Métrica FinOps</td><td>F1 (TLC Trips)</td><td>Costo unitario por kilómetro en la tarifa unificada Lumina (USD/km).</td></tr>
            <tr><td class="mono">street_traffic_speed_kmh</td><td class="mono">FLOAT64</td><td>Métrica Cruce F2</td><td>F2 (Uber Speeds)</td><td>Velocidad promedio de los autos particulares en superficie en esa misma ruta y hora.</td></tr>
            <tr><td class="mono">is_street_congested_24kmh</td><td class="mono">BOOL</td><td>Flag Cuello Vial</td><td>F2 (Uber Speeds)</td><td><code>TRUE</code> cuando la velocidad vial cae en el cuello de botella (&le; 24.5 km/h).</td></tr>
            <tr><td class="mono">car_benchmark_duration_min</td><td class="mono">FLOAT64</td><td>Métrica Benchmark</td><td>Cálculo F1 + F2</td><td>Tiempo que habría tomado realizar el mismo viaje en automóvil particular.</td></tr>
            <tr><td class="mono">time_saved_min_vs_car</td><td class="mono">FLOAT64</td><td>KPI de Valor</td><td>Cálculo F1 + F2</td><td>Minutos netos ahorrados por el pasajero al usar Lumina frente al auto privado.</td></tr>
            <tr><td class="mono">savings_usd_vs_car</td><td class="mono">FLOAT64</td><td>KPI de Valor</td><td>Cálculo F1 + F2</td><td>Dólares ahorrados por viaje frente al costo operativo de conducir un auto privado.</td></tr>
            <tr><td class="mono">station_oart_load_pct</td><td class="mono">FLOAT64</td><td>Métrica Cruce F3</td><td>F3 (IoT AoT)</td><td>Porcentaje de ocupación en tiempo real de la estación OART conectada.</td></tr>
            <tr><td class="mono">rider_id_dlp_sha256</td><td class="mono">STRING</td><td>Seguridad DLP</td><td>Transformación Silver</td><td>Hash criptográfico irreversible SHA-256 con sal para protección de datos personales.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- TAB 6: CALIDAD Y GOBERNANZA -->
  <div id="tab-calidad" class="tab-pane">
    <div class="card">
      <h2>🛡️ 6. Auditoría Automática de Calidad y Gobernanza (Knowledge Catalog DQ)</h2>
      <p>
        Siguiendo las mejores prácticas de ingeniería de datos vistas en clase, ningún dato pasa a consumo ejecutivo sin superar un umbral mínimo de calidad (<strong>SLA &ge; 95.0%</strong>). La tabla de auditoría <code>lumina_audit_dq.auditoria_calidad_knowledge_catalog</code> registra el resultado real sobre los casi 5 millones de viajes procesados:
      </p>

      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Dimensión de Calidad Evaluada</th>
              <th>Tabla Auditada</th>
              <th>Registros Evaluados</th>
              <th>Registros Válidos</th>
              <th>% Cumplimiento</th>
              <th>SLA Mínimo</th>
              <th>Estado de Gobernanza</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>1. Completitud de Llaves Espaciales (<code>zone_id / station_id</code>)</strong></td>
              <td class="mono">SILVER_TRIPS &amp; SILVER_IOT</td>
              <td class="mono">4,998,926</td>
              <td class="mono">4,998,926</td>
              <td><strong class="text-green">100.0%</strong></td>
              <td class="mono">95.0%</td>
              <td><span class="badge badge-success">CUMPLE SLA (&gt;= 95%)</span></td>
            </tr>
            <tr>
              <td><strong>2. Validez Física de Telemetría OART (Vibración y Temperatura)</strong></td>
              <td class="mono">SILVER_IOT_AOT</td>
              <td class="mono">11</td>
              <td class="mono">11</td>
              <td><strong class="text-green">100.0%</strong></td>
              <td class="mono">95.0%</td>
              <td><span class="badge badge-success">CUMPLE SLA (&gt;= 95%)</span></td>
            </tr>
            <tr>
              <td><strong>3. Exactitud Financiera y Costo por Km Positivo (NYC TLC)</strong></td>
              <td class="mono">SILVER_TRIPS</td>
              <td class="mono">4,998,926</td>
              <td class="mono">4,998,918</td>
              <td><strong class="text-green">100.0%</strong></td>
              <td class="mono">95.0%</td>
              <td><span class="badge badge-success">CUMPLE SLA (&gt;= 95%)</span></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- TAB 7: ORQUESTACIÓN AIRFLOW -->
  <div id="tab-airflow" class="tab-pane">
    <div class="card">
      <h2>⏱️ 7. Orquestación Automatizada con Apache Airflow / Cloud Composer</h2>
      <p>
        El ciclo de vida completo del procesamiento Medallion (validación de llegada de objetos a Cloud Storage, transformación Bronze ➔ Silver, compuerta de calidad Knowledge Catalog DQ y refresco del modelo Estrella Gold) está gobernado por el DAG <code>07_dag_managed_airflow_composer.py</code> y el Stored Procedure transaccional <code>sp_orquestar_medallion_lumina()</code>:
      </p>

      <pre><code># Definición del Grafo de Dependencias en Cloud Composer (07_dag_managed_airflow_composer.py)
with DAG(
    dag_id="lumina_medallion_bronze_silver_gold_daily",
    schedule_interval="0 4 * * *",  # Ejecución diaria 04:00 AM (cumple SLA antes de las 05:00 AM)
    catchup=False,
    tags=["lumina", "datalandia", "medallion", "utec-p2"],
) as dag:

    # 1. Verificar arribo de lotes en gs://bigdata-herles-pinedo-lumina-datalake/
    t1_verificar_landing_gcs = GCSObjectExistenceSensor(
        task_id="verificar_lotes_landing_gcs",
        bucket="bigdata-herles-pinedo-lumina-datalake",
        object="landing/batch/tlc_trips/nyc_tlc_trips_lumina_multimodal.csv",
    )

    # 2. Ejecutar transición Bronze -> Silver y Auditoría DQ en BigQuery
    t2_transformar_silver = BigQueryInsertJobOperator(
        task_id="transformar_bronze_a_silver_y_dlp",
        configuration={{"query": {{"query": "CALL `bigdata-herles-pinedo.lumina_gold_governed.sp_orquestar_medallion_lumina`();", "useLegacySql": False}}}},
    )

    # 3. Compuerta de Calidad (Quality Gate SLA >= 95%)
    t3_quality_gate_dq = BigQueryCheckOperator(
        task_id="verificar_sla_calidad_95pct",
        sql="SELECT MIN(pct_cumplimiento) &gt;= 95.0 FROM `bigdata-herles-pinedo.lumina_audit_dq.auditoria_calidad_knowledge_catalog`",
        use_legacy_sql=False,
    )

    t1_verificar_landing_gcs &gt;&gt; t2_transformar_silver &gt;&gt; t3_quality_gate_dq</code></pre>
    </div>
  </div>

  <!-- TAB 8: EVIDENCIAS REALES DE GCP (5 CAPTURAS EMBEBIDAS) -->
  <div id="tab-evidencias" class="tab-pane">
    <div class="card">
      <h2>📸 8. Evidencias Verificadas en Google Cloud Platform (Proyecto <code>bigdata-herles-pinedo</code>)</h2>
      <p>
        Todas las capturas presentadas a continuación corresponden a la ejecución real en la cuenta de Google Cloud del equipo y están embebidas directamente dentro de este archivo HTML. <em>(Haz clic sobre cualquier imagen para ampliarla en pantalla completa con controles de zoom).</em>
      </p>

      <!-- EVIDENCIA 1: C1.png -->
      <div class="evidence-block">
        <div class="evidence-header">
          <div class="evidence-title">
            <span>📊</span>
            <span>Evidencia 1: Datasets Medallion y Tabla de Hechos <code>fact_movilidad_multimodal_gold</code> en BigQuery Studio</span>
          </div>
          <span class="badge badge-success">4,998,926 Filas · 59 Particiones · 1.8 GB Lógicos (321.44 MB Físicos)</span>
        </div>
        <p>
          En el panel izquierdo se observan los 4 datasets de la arquitectura Medallion (<code>lumina_bronze_raw</code>, <code>lumina_silver_clean</code>, <code>lumina_gold_governed</code> y <code>lumina_audit_dq</code>) con sus respectivas tablas. En el panel central se verifica la configuración física de <code>fact_movilidad_multimodal_gold</code>: particionada por día sobre el campo <code>travel_date</code> y clusterizada por <code>county_id, origin_zone_id, transport_mode</code>.
        </p>
        <div class="img-wrapper" onclick="openImageLightbox(this.querySelector('img').src, 'Evidencia 1: BigQuery Studio — Datasets Medallion y Fact Table Gold (4,998,926 filas)')">
          <img src="{img_c1}" alt="Evidencia 1: BigQuery Studio Datasets y Fact Table Gold">
        </div>
        <div class="img-caption">
          <span>Fuente: Google Cloud Console — BigQuery Studio (Proyecto <code>bigdata-herles-pinedo</code>)</span>
          <span class="mono">Captura C1.png (3292 × 1994 px · Embebida en Base64)</span>
        </div>
      </div>

      <!-- EVIDENCIA 2: C2.png -->
      <div class="evidence-block">
        <div class="evidence-header">
          <div class="evidence-title">
            <span>🛡️</span>
            <span>Evidencia 2: Resultados de Auditoría de Calidad de Datos (Knowledge Catalog DQ)</span>
          </div>
          <span class="badge badge-purple">100.0% Cumplimiento en las 3 Reglas (SLA &ge; 95.0%)</span>
        </div>
        <p>
          Ejecución de la consulta sobre <code>bigdata-herles-pinedo.lumina_audit_dq.auditoria_calidad_knowledge_catalog</code>. Se comprueba que las 3 dimensiones críticas (Validez física de telemetría OART, Exactitud financiera sobre 4,998,926 viajes y Completitud de llaves espaciales) alcanzan <strong>100.0% de cumplimiento</strong> con estado <code>CUMPLE SLA (&gt;= 95%)</code>.
        </p>
        <div class="img-wrapper" onclick="openImageLightbox(this.querySelector('img').src, 'Evidencia 2: Resultados de Auditoría Knowledge Catalog DQ (100% Cumplimiento)')">
          <img src="{img_c2}" alt="Evidencia 2: Auditoría de Calidad Knowledge Catalog DQ">
        </div>
        <div class="img-caption">
          <span>Fuente: Google Cloud Console — BigQuery Query Results</span>
          <span class="mono">Captura C2.png (3292 × 1994 px · Embebida en Base64)</span>
        </div>
      </div>

      <!-- EVIDENCIA 3: C3.png -->
      <div class="evidence-block">
        <div class="evidence-header">
          <div class="evidence-title">
            <span>⚡</span>
            <span>Evidencia 3: Ingesta y Procesamiento Streaming en Cloud Pub/Sub</span>
          </div>
          <span class="badge badge-primary">Tópico y Suscripción Activos en GCP</span>
        </div>
        <p>
          Vista de detalle del tópico <code>projects/bigdata-herles-pinedo/topics/lumina-iot-aot-telemetry-topic</code> y su suscripción asociada <code>lumina-iot-aot-telemetry-sub</code>, encargados de alimentar en tiempo real el pipeline de Apache Beam / Cloud Dataflow con los eventos de sensores Array of Things y trenes OART.
        </p>
        <div class="img-wrapper" onclick="openImageLightbox(this.querySelector('img').src, 'Evidencia 3: Cloud Pub/Sub Topic y Subscription activos en GCP')">
          <img src="{img_c3}" alt="Evidencia 3: Cloud Pub/Sub Topic y Subscription">
        </div>
        <div class="img-caption">
          <span>Fuente: Google Cloud Console — Pub/Sub Topics</span>
          <span class="mono">Captura C3.png (3292 × 1994 px · Embebida en Base64)</span>
        </div>
      </div>

      <!-- EVIDENCIA 4: G4.png (ACTUALIZADA) -->
      <div class="evidence-block">
        <div class="evidence-header">
          <div class="evidence-title">
            <span>🗄️</span>
            <span>Evidencia 4: Data Lakehouse Medallion Completo en Cloud Storage (Capas Bronze, Silver, Gold y 84 Parquet Masivos)</span>
          </div>
          <span class="badge badge-success">Arquitectura Medallion Física en Formato Parquet</span>
        </div>
        <p>
          En el árbol de directorios izquierdo del bucket <code>bigdata-herles-pinedo-lumina-datalake</code> (región <code>us-central1</code>) se observa la estructura completa de capas: <code>bronze/</code>, <code>silver/</code> (con <code>censo_demografia_clean/</code>, <code>iot_telemetry_clean/</code> y <code>uber_movement_clean/</code>), <code>gold/</code> (con <code>dim_zonas_condados/</code> y <code>kpi_ejecutivo_condados/</code>), <code>landing/</code> y <code>raw/tlc_trips_5m/</code>. A la derecha se aprecian los archivos columnares <code>.parquet</code> que respaldan los 5 millones de viajes.
        </p>
        <div class="img-wrapper" onclick="openImageLightbox(this.querySelector('img').src, 'Evidencia 4: Bucket Cloud Storage con estructura Medallion completa y archivos Parquet')">
          <img src="{img_g4}" alt="Evidencia 4: Bucket Cloud Storage con estructura Medallion completa y archivos Parquet">
        </div>
        <div class="img-caption">
          <span>Fuente: Google Cloud Console — Cloud Storage Bucket <code>bigdata-herles-pinedo-lumina-datalake</code></span>
          <span class="mono">Captura Actualizada G4.png (3292 × 1994 px · Embebida en Base64)</span>
        </div>
      </div>

      <!-- EVIDENCIA 5: G5.png -->
      <div class="evidence-block">
        <div class="evidence-header">
          <div class="evidence-title">
            <span>💻</span>
            <span>Evidencia 5: Ejecución del Pipeline Distribuido en Apache Spark (PySpark)</span>
          </div>
          <span class="badge badge-amber">Motor Spark AQE + Window Functions</span>
        </div>
        <p>
          Ejecución del script <code>06b_pipeline_pyspark_dataproc_silver_gold.py</code> mostrando la limpieza y deduplicación de registros en la transición Bronze ➔ Silver y la generación del Data Mart Ejecutivo <code>kpi_ejecutivo_condados_gold</code>, donde entre el <strong>82.8% y el 100.0% de los viajes multimodales superan la barrera de congestión de 24 km/h</strong>.
        </p>
        <div class="img-wrapper" onclick="openImageLightbox(this.querySelector('img').src, 'Evidencia 5: Ejecución del Pipeline PySpark Medallion en Consola')">
          <img src="{img_g5}" alt="Evidencia 5: Consola PySpark Medallion">
        </div>
        <div class="img-caption">
          <span>Fuente: Terminal de Ejecución — Pipeline PySpark Dataproc</span>
          <span class="mono">Captura G5.png (2168 × 1868 px · Embebida en Base64)</span>
        </div>
      </div>

    </div>
  </div>

</div>

<!-- UNIFIED FULLSCREEN LIGHTBOX MODAL (FOR BOTH IMAGES AND INTERACTIVE SVGS) -->
<div id="lightbox" class="lightbox-modal" onclick="if(event.target === this) closeLightbox()">
  <div class="lightbox-toolbar">
    <div id="lightboxTitle" class="lightbox-title">Vista Ampliada</div>
    <div class="lightbox-controls">
      <button type="button" class="lightbox-btn" onclick="zoomLightbox(-0.2)">➖ Zoom -</button>
      <button type="button" class="lightbox-btn" onclick="resetZoomLightbox()">100%</button>
      <button type="button" class="lightbox-btn" onclick="zoomLightbox(0.2)">➕ Zoom +</button>
      <button type="button" class="lightbox-btn" onclick="closeLightbox()" style="background:#dc2626;border-color:#ef4444;">✕ Cerrar (Esc)</button>
    </div>
  </div>
  <div class="lightbox-stage">
    <img id="lightboxImg" class="lightbox-img" src="" alt="Vista ampliada" style="display:none;">
    <div id="lightboxSvgHolder" class="lightbox-svg-holder" style="display:none;"></div>
  </div>
</div>

<script id="archify-viewer-payloads" type="application/json">{{"arch":"{viewer_arch_b64}","df":"{viewer_df_b64}","erd":"{viewer_erd_b64}"}}</script>
<script>
  let currentZoom = 1.0;
  const VIEWER_PAYLOADS = JSON.parse(document.getElementById('archify-viewer-payloads').textContent);
  const VIEWER_SETUP = `<style>
    html, body {{ height:100% !important; min-height:0 !important; overflow:hidden !important; margin:0 !important; padding:0 !important; }}
    .container {{ height:100% !important; min-height:0 !important; display:flex !important; flex-direction:column !important; padding:0.5rem 1rem !important; gap:0 !important; }}
    .header {{ display:none !important; }}
    .diagram-container {{ flex:1 1 auto !important; min-height:0 !important; height:100% !important; overflow:hidden !important; }}
    #reader-rail, #node-outline, #rail-reveal, .rail-controls, .rail-reveal, .node-outline {{ display:none !important; }}
  </style><script>(()=>{{
    try {{ localStorage.setItem('archify-theme','dark'); localStorage.setItem('archify-preset','classic'); localStorage.setItem('archify-motion','live'); localStorage.setItem('archify-rail-collapsed','1'); }} catch(_) {{}}
    const init = () => {{ try {{
      const r = document.documentElement;
      r.setAttribute('data-theme','dark'); r.setAttribute('data-preset','classic'); r.setAttribute('data-motion','live'); r.setAttribute('data-ambient-motion','running'); r.setAttribute('data-present','true');
      if (window.Archify?.presentation?.setActive) window.Archify.presentation.setActive(true);
      if (window.Archify?.view?.fit) window.Archify.view.fit();
    }} catch(_) {{}} }};
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
    addEventListener('resize', () => {{ try {{ window.Archify?.view?.fit?.(); }} catch(_) {{}} }});
  }})();<\\/script>`;

  async function inflateViewer(key) {{
    const bytes = Uint8Array.from(atob(VIEWER_PAYLOADS[key]), c => c.charCodeAt(0));
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
    return await new Response(stream).text();
  }}

  // Carga perezosa: el visor se crea cuando su sección es visible (evita encuadres en 0px dentro de pestañas ocultas).
  function ensureViewers() {{
    if (typeof DecompressionStream === 'undefined') return; // navegador antiguo: queda el SVG estático
    document.querySelectorAll('.archify-canvas-wrap[data-viewer]').forEach(wrap => {{
      if (wrap.dataset.loaded || wrap.offsetParent === null) return;
      wrap.dataset.loaded = 'loading';
      inflateViewer(wrap.dataset.viewer).then(raw => {{
        let html = raw.replace(/<html\b([^>]*)>/i, '<html$1 data-theme="dark" data-preset="classic" data-motion="live" data-ambient-motion="running" data-present="true">');
        const end = html.toLowerCase().lastIndexOf('</body>');
        html = end < 0 ? html + VIEWER_SETUP : html.slice(0, end) + VIEWER_SETUP + html.slice(end);
        const frame = document.createElement('iframe');
        frame.className = 'archify-viewer-frame';
        frame.setAttribute('sandbox', 'allow-scripts allow-modals');
        frame.title = wrap.closest('.archify-showcase-card')?.querySelector('.archify-bar-title')?.innerText || 'Diagrama interactivo';
        frame.srcdoc = html;
        wrap.querySelector('.static-fallback').style.display = 'none';
        wrap.classList.add('has-viewer');
        wrap.appendChild(frame);
        wrap.dataset.loaded = 'true';
      }}).catch(() => {{ wrap.dataset.loaded = 'error'; }});
    }});
  }}

  function fullscreenDiagram(wrapId) {{
    const card = document.getElementById(wrapId)?.closest('.archify-showcase-card');
    if (!card) return;
    if (document.fullscreenElement) document.exitFullscreen();
    else if (card.requestFullscreen) card.requestFullscreen();
    else openSvgLightbox(wrapId, 'Diagrama');
  }}
  document.addEventListener('DOMContentLoaded', ensureViewers);

  function openTab(evt, tabId) {{
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
    if (evt && evt.currentTarget) {{
      evt.currentTarget.classList.add('active');
    }}
    const target = document.getElementById(tabId);
    if (target) target.classList.add('active');
    requestAnimationFrame(ensureViewers);
  }}

  function openRepoFile(evt, paneId) {{
    document.querySelectorAll('.repo-tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.repo-pane').forEach(pane => pane.classList.remove('active'));
    if (evt && evt.currentTarget) {{
      evt.currentTarget.classList.add('active');
    }}
    const target = document.getElementById(paneId);
    if (target) target.classList.add('active');
  }}

  function copyCodeBlock(codeId, btn) {{
    const el = document.getElementById(codeId);
    if (!el) return;
    navigator.clipboard.writeText(el.innerText).then(() => {{
      const orig = btn.innerText;
      btn.innerText = '✅ ¡Copiado al portapapeles!';
      setTimeout(() => {{ btn.innerText = orig; }}, 2000);
    }});
  }}

  function toggleAllSections() {{
    const body = document.body;
    const btn = document.getElementById('allSectionsBtn');
    body.classList.toggle('show-all-sections');
    requestAnimationFrame(ensureViewers);
    if (body.classList.contains('show-all-sections')) {{
      btn.classList.add('active-mode');
      btn.innerText = '🗂️ Volver a Vista por Pestañas';
    }} else {{
      btn.classList.remove('active-mode');
      btn.innerText = '📖 Mostrar Todo (Lectura Continua)';
    }}
  }}

  function toggleTheme() {{
    const html = document.documentElement;
    const current = html.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
  }}

  function openImageLightbox(src, title) {{
    currentZoom = 1.0;
    const modal = document.getElementById('lightbox');
    const img = document.getElementById('lightboxImg');
    const svgHolder = document.getElementById('lightboxSvgHolder');
    document.getElementById('lightboxTitle').innerText = title || 'Evidencia Verificada en GCP';
    svgHolder.style.display = 'none';
    svgHolder.innerHTML = '';
    img.src = src;
    img.style.transform = 'scale(1)';
    img.style.display = 'block';
    modal.classList.add('active');
  }}

  function openSvgLightbox(wrapId, title) {{
    currentZoom = 1.0;
    const wrap = document.getElementById(wrapId);
    if (!wrap) return;
    const modal = document.getElementById('lightbox');
    const img = document.getElementById('lightboxImg');
    const svgHolder = document.getElementById('lightboxSvgHolder');
    document.getElementById('lightboxTitle').innerText = title || 'Diagrama de Arquitectura';
    img.style.display = 'none';
    svgHolder.innerHTML = (wrap.querySelector('.static-fallback') || wrap).innerHTML;
    svgHolder.style.transform = 'scale(1)';
    svgHolder.style.display = 'block';
    modal.classList.add('active');
  }}

  function zoomLightbox(delta) {{
    currentZoom = Math.min(3.0, Math.max(0.5, currentZoom + delta));
    const img = document.getElementById('lightboxImg');
    const svgHolder = document.getElementById('lightboxSvgHolder');
    if (img.style.display !== 'none') {{
      img.style.transform = `scale(${{currentZoom}})`;
    }}
    if (svgHolder.style.display !== 'none') {{
      svgHolder.style.transform = `scale(${{currentZoom}})`;
    }}
  }}

  function resetZoomLightbox() {{
    currentZoom = 1.0;
    zoomLightbox(0);
  }}

  function closeLightbox() {{
    const modal = document.getElementById('lightbox');
    modal.classList.remove('active');
  }}

  document.addEventListener('keydown', (e) => {{
    if (e.key === 'Escape') closeLightbox();
  }});
</script>

</body>
</html>
"""
    target_path = os.path.join(BASE_DIR, "00_DataLandia_Lumina_Entrega2_Procesamiento_Transformacion.html")
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ HTML 100% autocontenido generado en: {target_path} ({len(html):,} bytes)")


if __name__ == "__main__":
    build_html()
