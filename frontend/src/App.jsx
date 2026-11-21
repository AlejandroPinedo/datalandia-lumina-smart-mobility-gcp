import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, LayoutDashboard, Table2, GitCompareArrows, Database, MessageSquareText,
  ChevronDown, ChevronRight, Download, RefreshCw, SlidersHorizontal, X, Search,
  ArrowUpDown, Bike, BusFront, CarFront, TramFront, MapPin, CalendarDays, Clock3,
  Check, Info, PanelLeftClose, Menu, RotateCcw, CircleAlert, ExternalLink, FileText } from 'lucide-react';
import { dataTools as D, emptyFilters, modeColors, nameOf, format, dateLabel, selectRows, exportSelection, answerQuestion } from './data.js';
import { CountyBars, Empty, HourChart, ModeBars, TrendChart } from './charts.jsx';
import Operations from './Operations.jsx';

const navigation = [
  ['overview', 'Resumen', LayoutDashboard], ['explore', 'Explorador', Table2],
  ['compare', 'Comparar modos', GitCompareArrows], ['questions', 'Consultas', MessageSquareText],
  ['source', 'Fuente de datos', Database],
  ['operations', 'Validación integral', Activity],
];
const titles = {
  overview: ['Resumen de movilidad', 'Consulta los viajes y sus patrones de movilidad.'],
  explore: ['Explorador de viajes', 'Revisa los indicadores de cada modo de transporte.'],
  compare: ['Comparar transporte', 'Compara dos modos dentro del mismo período y territorio.'],
  questions: ['Consulta tus datos', 'Obtén respuestas sobre los indicadores de tu selección.'],
  source: ['Fuente de datos', 'Revisa el origen, la calidad y las consultas ejecutadas.'],
  operations: ['Validación del pipeline', 'Resultados comprobados de procesamiento, modelos, resiliencia y exposición.'],
};
const metricOptions = [['total_trips', 'Registros'], ['total_km', 'Distancia · km'],
  ['avg_duration_min', 'Duración · min'], ['revenue_per_km_usd', 'Tarifa/km · USD']];
const icons = [Bike, BusFront, CarFront, TramFront, CarFront];
const validView = () => navigation.some(([id]) => id === location.hash.slice(1)) ? location.hash.slice(1) : 'overview';

function Field({ label, children, className = '' }) {
  return <label className={`field ${className}`}><span>{label}</span>{children}</label>;
}
function Select({ children, ...props }) { return <span className="select-wrap"><select {...props}>{children}</select><ChevronDown size={14} aria-hidden="true" /></span>; }
function Panel({ title, subtitle, action, children, className = '' }) {
  return <section className={`panel ${className}`}><div className="panel-heading"><div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>{action}</div>{children}</section>;
}
function ModeIcon({ mode, allModes, small = false }) {
  const index = allModes.indexOf(mode), Icon = icons[index] || Activity;
  return <span className={`mode-icon ${small ? 'small' : ''}`} style={{ '--mode-color': modeColors[index % modeColors.length] }}><Icon size={small ? 16 : 21} strokeWidth={1.7} /></span>;
}
function downloadCsv(rows, filters, report) {
  const url = URL.createObjectURL(new Blob([exportSelection(rows, filters, report)], { type: 'text/csv;charset=utf-8' }));
  const link = document.createElement('a'); link.href = url; link.download = 'lumina-seleccion.csv';
  document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function Filters({ filters, setFilters, report, cube, allModes }) {
  const [expanded, setExpanded] = useState(false);
  const dictionaries = report?.exploration?.dictionaries;
  const dates = [...(dictionaries?.travel_date || [])].filter(Boolean).sort();
  const change = (key, value) => setFilters(previous => ({ ...previous, [key]: value }));
  const toggleMode = mode => change('modes', filters.modes.includes(mode) ? filters.modes.filter(m => m !== mode) : [...filters.modes, mode]);
  const activeCount = ['from', 'to', 'county', 'hour'].filter(key => filters[key] !== '').length + (filters.modes.length ? 1 : 0);
  return <section className="filter-area" aria-label="Filtros de los datos"><div className="filter-toolbar"><div className="filter-primary"><CalendarDays size={17} /><span>Período</span><input aria-label="Fecha inicial" type="date" value={filters.from} min={dates[0]} max={dates.at(-1)} disabled={!cube} onChange={e => change('from', e.target.value)} /><span className="date-separator">—</span><input aria-label="Fecha final" type="date" value={filters.to} min={dates[0]} max={dates.at(-1)} disabled={!cube} onChange={e => change('to', e.target.value)} /></div><div className="filter-end"><div className="county-filter"><MapPin size={16} /><Select aria-label="Filtrar por condado" value={filters.county} disabled={!cube} onChange={e => change('county', e.target.value)}><option value="">Todos los condados</option>{(dictionaries?.county_name || []).filter(v => v != null).map(county => <option key={county} value={county}>{county.replace(/^Condado /, '')}</option>)}</Select></div><button className={`button filter-toggle ${expanded ? 'is-active' : ''}`} aria-expanded={expanded} onClick={() => setExpanded(!expanded)}><SlidersHorizontal size={16} />Filtros{activeCount > 0 && <span className="count-badge">{activeCount}</span>}</button></div></div>
    {expanded && <div className="filter-expanded"><div className="filter-mode-heading"><span>Modos de transporte</span><button className="text-button" onClick={() => change('modes', [])}>Todos los modos</button></div><div className="mode-chips">{allModes.map(mode => <button key={mode} aria-pressed={filters.modes.includes(mode)} className={`mode-chip ${filters.modes.includes(mode) ? 'selected' : ''}`} onClick={() => toggleMode(mode)}>{filters.modes.includes(mode) && <Check size={13} />}{nameOf(mode)}</button>)}</div><Field label="Hora de salida" className="hour-field"><Select value={filters.hour} disabled={!cube} onChange={e => change('hour', e.target.value)}><option value="">Todas las horas</option>{(dictionaries?.hod || []).filter(h => h != null).sort((a, b) => a - b).map(hour => <option key={hour} value={String(hour)}>{String(hour).padStart(2, '0')}:00 – {String((Number(hour) + 1) % 24).padStart(2, '0')}:00</option>)}</Select></Field></div>}
    {activeCount > 0 && <div className="active-filters"><span>Selección activa</span>{filters.from && <button onClick={() => change('from', '')}>Desde {dateLabel(filters.from)}<X size={12} /></button>}{filters.to && <button onClick={() => change('to', '')}>Hasta {dateLabel(filters.to)}<X size={12} /></button>}{filters.county && <button onClick={() => change('county', '')}>{filters.county.replace(/^Condado /, '')}<X size={12} /></button>}{filters.hour !== '' && <button onClick={() => change('hour', '')}>{filters.hour}:00 h<X size={12} /></button>}{filters.modes.map(mode => <button key={mode} onClick={() => toggleMode(mode)}>{nameOf(mode)}<X size={12} /></button>)}<button className="clear-filters" onClick={() => setFilters({ ...emptyFilters })}><RotateCcw size={12} />Limpiar</button></div>}
    {filters.from && filters.to && filters.from > filters.to && <p className="validation-message" role="alert">La fecha inicial debe ser anterior o igual a la fecha final.</p>}
    {!cube && <p className="inline-note"><Info size={14} />Los filtros de fecha, condado y hora requieren el desglose de Gold. La selección de modos está disponible.</p>}
  </section>;
}

function Stats({ rows, report }) {
  const summary = D.summary(rows), share = report.summary.total_trips ? summary.total_trips / report.summary.total_trips * 100 : 0;
  return <section className="stats" aria-label="Indicadores de la selección"><div><span className="stat-label">Registros de viajes</span><strong>{format(summary.total_trips, 0)}</strong><span className="stat-note"><span className="selection-percent">{format(share, 1)} %</span> del total consultado</span></div><div><span className="stat-label">Distancia recorrida</span><strong>{format(summary.total_km, 0)}<small>km</small></strong><span className="stat-note">Acumulada en la selección</span></div><div><span className="stat-label">Importes de tarifa</span><strong><small className="currency">$</small>{format(summary.total_revenue_usd, 0)}<small>USD</small></strong><span className="stat-note">Excluye el auto de referencia</span></div></section>;
}

function Overview({ selected, rows, cube, allModes, setFilters, onDetail, onExplore }) {
  const [metric, setMetric] = useState('total_trips');
  const dimensions = useMemo(() => cube ? {
    dates: D.group(selected, 'travel_date').sort((a, b) => String(a.travel_date).localeCompare(String(b.travel_date))),
    hours: D.group(selected, 'hod').sort((a, b) => (a.hod ?? -1) - (b.hod ?? -1)),
    counties: D.group(selected, 'county_name').sort((a, b) => b.total_trips - a.total_trips),
  } : null, [selected, cube]);
  const missing = <Empty>Actualiza el desglose de Gold para explorar esta dimensión.</Empty>;
  return <><div className="overview-grid"><Panel title="Actividad en el tiempo" subtitle="Distribución diaria de los registros seleccionados" action={<span className="subtle-label">Por día</span>} className="trend-panel">{dimensions ? <TrendChart rows={dimensions.dates} onSelect={date => date && setFilters(f => ({ ...f, from: date, to: date }))} /> : missing}</Panel><Panel title="Modos de transporte" action={<Select aria-label="Indicador de distribución" value={metric} onChange={e => setMetric(e.target.value)}>{metricOptions.map(([value, title]) => <option key={value} value={value}>{title}</option>)}</Select>} className="transport-panel"><ModeBars rows={rows} metric={metric} onSelect={onDetail} allModes={allModes} /><button className="panel-link" onClick={onExplore}>Explorar el detalle<ChevronRight size={15} /></button></Panel></div>
    <div className="secondary-grid"><Panel title="¿Cuándo se viaja?" subtitle="Registros según la hora de salida">{dimensions ? <HourChart rows={dimensions.hours} onSelect={hour => setFilters(f => ({ ...f, hour }))} /> : missing}</Panel><Panel title="Distribución por condado" subtitle="Participación en los registros seleccionados">{dimensions ? <CountyBars rows={dimensions.counties} onSelect={county => setFilters(f => ({ ...f, county }))} /> : missing}</Panel></div><p className="chart-hint"><Info size={14} />Selecciona una fecha, una hora o un condado en los gráficos para filtrar los resultados.</p></>;
}

function Explorer({ rows, allModes, onDetail }) {
  const [search, setSearch] = useState(''), [sort, setSort] = useState({ key: 'total_trips', direction: -1 });
  const filtered = useMemo(() => rows.filter(row => nameOf(row.transport_mode).toLowerCase().includes(search.toLowerCase()) || row.transport_mode?.toLowerCase().includes(search.toLowerCase())).sort((a, b) => a[sort.key] == null ? b[sort.key] == null ? 0 : 1 : b[sort.key] == null ? -1 : sort.direction * (typeof a[sort.key] === 'string' ? a[sort.key].localeCompare(b[sort.key]) : a[sort.key] - b[sort.key])), [rows, search, sort]);
  const columns = [['transport_mode', 'Transporte'], ['total_trips', 'Registros'], ['total_km', 'Distancia (km)'], ['avg_duration_min', 'Duración (min)'], ['revenue_per_km_usd', 'Tarifa/km (USD)'], ['avg_passengers', 'Pasajeros medios']];
  return <Panel title="Detalle por modo" subtitle="Indicadores agregados para tu selección" className="table-panel" action={<div className="search-input"><Search size={16} /><input aria-label="Buscar transporte" placeholder="Buscar transporte…" value={search} onChange={e => setSearch(e.target.value)} /></div>}><div className="table-scroll"><table><thead><tr>{columns.map(([key, title]) => <th key={key} aria-sort={sort.key === key ? sort.direction === 1 ? 'ascending' : 'descending' : 'none'}><button onClick={() => setSort({ key, direction: sort.key === key ? -sort.direction : -1 })}>{title}<ArrowUpDown size={12} /></button></th>)}<th><span className="sr-only">Detalle</span></th></tr></thead><tbody>{filtered.map(row => <tr key={row.transport_mode}><td><button className="table-mode" onClick={() => onDetail(row)}><ModeIcon mode={row.transport_mode} allModes={allModes} small /><span>{nameOf(row.transport_mode)}</span></button></td>{columns.slice(1).map(([key]) => <td key={key}>{format(row[key], key === 'total_trips' ? 0 : key === 'total_km' ? 1 : 2)}</td>)}<td><button className="icon-button" aria-label={`Ver detalle de ${nameOf(row.transport_mode)}`} onClick={() => onDetail(row)}><ChevronRight size={17} /></button></td></tr>)}</tbody></table></div>{!filtered.length && <Empty>{search ? 'No encontramos un modo con ese nombre.' : 'No hay registros para esta selección.'}</Empty>}<div className="table-footer"><span>{filtered.length} de {rows.length} modos</span><span>Selecciona un modo para ver su ficha</span></div></Panel>;
}

function Comparison({ rows, allModes }) {
  const [a, setA] = useState(allModes[0] || ''), [b, setB] = useState(allModes[1] || allModes[0] || '');
  const one = rows.find(row => row.transport_mode === a), two = rows.find(row => row.transport_mode === b);
  const metrics = [['total_trips', 'Registros', ''], ['total_km', 'Distancia acumulada', 'km'], ['avg_duration_min', 'Duración media', 'min'], ['revenue_per_km_usd', 'Tarifa por kilómetro', 'USD/km'], ['avg_passengers', 'Pasajeros medios', '']];
  return <Panel title="Comparación de indicadores" subtitle="Se aplican los filtros de fecha, condado y hora. Los modos se eligen aquí."><div className="compare-selectors">{[[a, setA, 'Primer modo'], [b, setB, 'Segundo modo']].map(([value, setter, label]) => <Field label={label} key={label}><Select value={value} onChange={e => setter(e.target.value)}>{allModes.map(mode => <option key={mode} value={mode}>{nameOf(mode)}</option>)}</Select></Field>)}</div>
    {(!one || !two) ? <Empty>Uno de estos modos no tiene registros con los filtros actuales.</Empty> : <><div className="comparison-table"><div className="comparison-header"><span>Indicador</span><span><ModeIcon mode={a} allModes={allModes} />{nameOf(a)}</span><span><ModeIcon mode={b} allModes={allModes} />{nameOf(b)}</span></div>{metrics.map(([key, title, unit]) => <div className="comparison-row" key={key}><span>{title}</span>{[one, two].map((row, index) => <span key={index}><strong>{format(row[key], key === 'total_trips' ? 0 : 2)}</strong>{unit && <small>{unit}</small>}</span>)}</div>)}</div><div className="comparison-note"><GitCompareArrows size={18} /><div>{a === b ? 'Elige dos modos distintos para ver sus diferencias.' : <><strong>{format(one.total_trips - two.total_trips, 0)} registros de diferencia</strong><p>{one.avg_duration_min != null && two.avg_duration_min != null ? `La diferencia de duración media es ${format(one.avg_duration_min - two.avg_duration_min)} min (primer modo menos segundo).` : 'La duración media no está disponible para ambos modos.'}</p></>}</div></div></>}
    <p className="inline-note"><Info size={14} />Esta comparación describe los registros disponibles; no mide el efecto de cambiar de transporte en un mismo trayecto.</p></Panel>;
}

function Questions({ rows, selected, report }) {
  const [question, setQuestion] = useState(''), [asked, setAsked] = useState('');
  const prompts = ['Resumen de la selección', '¿Qué modo tiene más viajes?', 'Comparar duración', 'Comparar tarifa', 'Revisar calidad'];
  return <Panel title="¿Qué quieres conocer?" subtitle="Elige una consulta o escribe una pregunta sobre los indicadores disponibles." className="questions-panel"><div className="question-prompts">{prompts.map(prompt => <button key={prompt} onClick={() => { setQuestion(prompt); setAsked(prompt); }}><MessageSquareText size={16} />{prompt}<ChevronRight size={15} /></button>)}</div><form className="question-form" onSubmit={e => { e.preventDefault(); setAsked(question); }}><label className="sr-only" htmlFor="question">Tu pregunta</label><input id="question" placeholder="Por ejemplo: ¿qué modo tiene más viajes?" value={question} onChange={e => setQuestion(e.target.value)} required /><button className="button primary" type="submit">Consultar</button></form>{asked && <div className="query-answer" aria-live="polite"><span className="answer-label">Resultado de tu selección</span><h3>{asked}</h3><p>{answerQuestion(asked, rows, selected)}</p><small>Se actualiza al cambiar los filtros · Consulta {report.job_id}</small></div>}<p className="inline-note"><Info size={14} />Las respuestas se calculan sobre los datos disponibles. Consultas admitidas: resumen, viajes, duración, tarifa y calidad.</p></Panel>;
}

function Source({ report, selected }) {
  const [copied, setCopied] = useState(false);
  const quality = [['missing_distance_rows', 'Distancias sin dato'], ['missing_amount_rows', 'Importes sin dato'], ['negative_distance_rows', 'Distancias negativas']];
  const metadata = [['Tabla', report.source.replace(/^bigquery:/, '')], ['Consulta del resumen', report.job_id],
    ['Consulta del desglose', report.exploration?.job_id], ['Consultado el', new Date(report.queried_at).toLocaleString('es-PE')],
    ['Última modificación de la tabla', report.processed_at ? new Date(report.processed_at).toLocaleString('es-PE') : null],
    ['Acceso', report.live_query ? 'Consulta de solo lectura' : 'Resultado exportado de BigQuery'], ['Procedencia externa', 'Sin verificación independiente']];
  return <><div className="source-grid"><Panel title="Calidad de la selección" subtitle="Controles disponibles en la consulta actual"><div className="quality-list">{quality.map(([key, title]) => <div key={key}><span>{title}</span><strong>{format(D.sum(selected, key), 0)}</strong></div>)}</div><p className="inline-note"><Info size={14} />La cobertura explícita de tráfico e IoT no está disponible en esta consulta.</p></Panel><Panel title="Acerca de esta fuente"><div className="source-summary"><Database size={23} /><div><strong>Gold de movilidad multimodal</strong><p>{format(report.summary.total_trips, 0)} registros consultados</p></div></div><p>Se utiliza la tabla existente del proyecto. No se generan registros ni se modifica la fuente.</p><p className="inline-note">Los importes son valores de sus columnas, no ingresos auditados. La presencia de la tabla en BigQuery no certifica su procedencia externa.</p></Panel></div><Panel title="Trazabilidad"><dl className="metadata-list">{metadata.map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value ?? 'No disponible'}</dd></div>)}</dl></Panel><Panel title="Consulta SQL" subtitle="Operación de lectura ejecutada en BigQuery" action={<button className="button compact" onClick={async () => { try { await navigator.clipboard.writeText(report.query_sql); setCopied(true); setTimeout(() => setCopied(false), 2000); } catch { setCopied(false); } }}>{copied ? <Check size={14} /> : <FileText size={14} />}{copied ? 'Copiado' : 'Copiar SQL'}</button>}><pre className="sql-block">{report.query_sql}</pre>{report.exploration?.query_sql && <details className="source-details"><summary>Consulta del desglose por fecha, condado, modo y hora<ChevronDown size={16} /></summary><pre className="sql-block">{report.exploration.query_sql}</pre></details>}<details className="source-details"><summary>Esquema de la tabla · {report.source_table?.schema?.length || 0} columnas<ChevronDown size={16} /></summary><div className="schema-grid">{report.source_table?.schema?.map(field => <div key={field.name}><code>{field.name}</code><span>{field.type}</span></div>)}</div></details></Panel></>;
}

function Detail({ row, allModes, onClose }) {
  const ref = useRef(null);
  useEffect(() => { const dialog = ref.current; dialog.showModal(); return () => { if (dialog.open) dialog.close(); }; }, []);
  const metrics = [['total_trips', 'Registros', ''], ['total_km', 'Distancia', 'km'], ['avg_duration_min', 'Duración media', 'min'], ['revenue_per_km_usd', 'Tarifa por km', 'USD'], ['avg_passengers', 'Pasajeros medios', ''], ['total_revenue_usd', 'Importe de tarifa', 'USD'], ['missing_distance_rows', 'Distancias nulas', ''], ['missing_amount_rows', 'Importes nulos', ''], ['negative_distance_rows', 'Distancias negativas', '']];
  return <dialog ref={ref} className="detail-dialog" onCancel={onClose} onClick={e => { if (e.target === e.currentTarget) onClose(); }} aria-labelledby="detail-title"><div className="dialog-top"><ModeIcon mode={row.transport_mode} allModes={allModes} /><button className="icon-button" aria-label="Cerrar detalle" onClick={onClose}><X size={20} /></button></div><h2 id="detail-title">{nameOf(row.transport_mode)}</h2><p className="mode-code">{row.transport_mode}</p><dl className="detail-metrics">{metrics.map(([key, title, unit]) => <div key={key}><dt>{title}</dt><dd>{format(row[key], key === 'total_trips' || key.endsWith('_rows') ? 0 : 2)}{unit && <small>{unit}</small>}</dd></div>)}</dl><p className="inline-note">Valores de la selección actual. N/D indica una medición no disponible.</p><button className="button dialog-close" onClick={onClose}>Cerrar detalle</button></dialog>;
}

export default function App() {
  const [view, setView] = useState(validView), [report, setReport] = useState(null), [loading, setLoading] = useState(true), [error, setError] = useState('');
  const [filters, setFilters] = useState({ ...emptyFilters }), [detailMode, setDetailMode] = useState(null), [mobileMenu, setMobileMenu] = useState(false);
  const controller = useRef(null);
  const refresh = async () => {
    controller.current?.abort(); const request = new AbortController(); controller.current = request;
    setLoading(true); setError('');
    try {
      const response = await fetch('/api/v1/olap-kpis', { cache: 'no-store', signal: request.signal });
      const data = await response.json();
      if (!response.ok || data.status !== 'ok' || data.data_origin !== 'existing_project_data' || !Array.isArray(data.by_mode)) throw new Error(data.reason || 'No hay una consulta válida de Gold disponible.');
      setReport(data); setDetailMode(null);
    } catch (failure) { if (failure.name !== 'AbortError') { setReport(null); setError(failure.message); } }
    finally { if (controller.current === request) setLoading(false); }
  };
  useEffect(() => {
    if (view==='operations') {controller.current?.abort();setLoading(false);setError('');return;}
    refresh();return () => controller.current?.abort();
  }, [view==='operations']);
  useEffect(() => { const handler = () => setView(validView()); window.addEventListener('hashchange', handler); return () => window.removeEventListener('hashchange', handler); }, []);
  const navigate = next => { location.hash = next; setView(next); setMobileMenu(false); window.scrollTo({ top: 0, behavior: 'instant' }); };
  const cube = useMemo(() => report ? D.unpack(report) : null, [report]);
  const allModes = useMemo(() => report?.by_mode.map(row => row.transport_mode) || [], [report]);
  const selected = useMemo(() => selectRows(report, cube, filters), [report, cube, filters]);
  const rows = useMemo(() => cube ? D.group(selected, 'transport_mode') : selected, [selected, cube]);
  const compareRows = useMemo(() => { const values = selectRows(report, cube, filters, true); return cube ? D.group(values, 'transport_mode') : values; }, [report, cube, filters]);
  const detail = rows.find(row => row.transport_mode === detailMode);
  const title = titles[view];
  const dates = report?.exploration?.dictionaries?.travel_date.filter(Boolean).sort() || [];
  return <div className="app-shell"><a className="skip-link" href="#main-content">Ir al contenido</a>{mobileMenu && <button className="sidebar-scrim" aria-label="Cerrar navegación" onClick={() => setMobileMenu(false)} />}
    <aside className={`sidebar ${mobileMenu ? 'mobile-open' : ''}`}><a href="#overview" className="brand" onClick={() => navigate('overview')} aria-label="Lumina, inicio"><span className="brand-mark"><Activity size={23} strokeWidth={2} /></span><span>lumina<span className="brand-period">.</span></span></a><div className="workspace-label"><span className="workspace-avatar">D</span><div><strong>DataLandia</strong><span>Movilidad urbana</span></div></div><div className="nav-label">ESPACIO DE TRABAJO</div><nav aria-label="Navegación principal">{navigation.map(([id, label, Icon]) => <a href={`#${id}`} key={id} aria-current={view === id ? 'page' : undefined} className={`nav-item ${view === id ? 'active' : ''}`} onClick={() => navigate(id)}><Icon size={18} strokeWidth={1.7} /><span>{label}</span>{view === id && <span className="active-indicator" />}</a>)}</nav><div className="sidebar-bottom"><div className="project-note"><span className="project-note-icon"><Database size={16} /></span><div><strong>Datos del proyecto</strong><span>Gold de movilidad</span></div></div><div className="sidebar-profile"><span className="profile-avatar">L</span><div><strong>Proyecto Lumina</strong><span>UTEC · Grupo 7</span></div><span className="version-label">P3</span></div></div></aside>
    <div className="main-shell"><header className="topbar"><div className="breadcrumb"><button className="icon-button mobile-menu" aria-label="Abrir navegación" onClick={() => setMobileMenu(!mobileMenu)}><Menu size={20} /></button><span>Espacio de trabajo</span><ChevronRight size={13} /><strong>{navigation.find(([id]) => id === view)[1]}</strong></div><div className="topbar-right"><span className="project-chip">Smart Mobility</span><span className="top-avatar" aria-label="Proyecto Lumina">L</span></div></header>
      <main id="main-content"><a className="text-button" href="#viajar" style={{display:'inline-flex',marginBottom:20}}>Web del pasajero</a><div className="page-heading"><div><div className="page-kicker">DATALANDIA / MOVILIDAD URBANA</div><h1>{title[0]}</h1><p>{title[1]}</p></div>{view!=='operations'&&<div className="page-actions"><button className="button icon-button refresh-button" aria-label="Actualizar datos de la API" title="Actualizar datos" disabled={loading} onClick={refresh}><RefreshCw size={17} className={loading ? 'spinning' : ''} /></button><button className="button primary" disabled={!report || loading} onClick={() => downloadCsv(rows, filters, report)}><Download size={16} />Exportar CSV</button></div>}</div>
      {error && <div className="error-panel" role="alert"><CircleAlert size={22} /><div><h2>No pudimos cargar los datos</h2><p>{error}</p><button className="button" onClick={refresh}>Volver a intentar</button></div></div>}
      {loading && !report && <div className="loading-state" role="status"><div className="skeleton skeleton-filters" /><div className="skeleton skeleton-stats" /><div className="skeleton skeleton-chart" /><span>Cargando los datos de movilidad…</span></div>}
      {view==='operations'&&<Operations/>}
      {report && view!=='operations' && <><div className="data-context"><span className="source-tag"><Database size={13} />{report.live_query ? 'Consulta de Gold' : 'Histórico de Gold'}</span><span>{dates.length ? `${dateLabel(dates[0])} – ${dateLabel(dates.at(-1))} ${dates[0].slice(0, 4)}` : 'Desglose por modo'}</span><button onClick={() => navigate('source')}>Ver fuente<ExternalLink size={12} /></button></div>
        {view !== 'source' && <><Filters filters={filters} setFilters={setFilters} report={report} cube={cube} allModes={allModes} /><Stats rows={rows} report={report} /></>}
        <div className={loading ? 'content-refreshing' : ''} aria-busy={loading}>
          {view === 'overview' && <Overview selected={selected} rows={rows} cube={cube} allModes={allModes} setFilters={setFilters} onDetail={row => setDetailMode(row.transport_mode)} onExplore={() => navigate('explore')} />}
          {view === 'explore' && <Explorer rows={rows} allModes={allModes} onDetail={row => setDetailMode(row.transport_mode)} />}
          {view === 'compare' && <Comparison rows={compareRows} allModes={allModes} />}
          {view === 'questions' && <Questions rows={rows} selected={selected} report={report} />}
          {view === 'source' && <Source report={report} selected={selected} />}
        </div><footer className="page-footer"><span>Lumina · Análisis de movilidad urbana</span><span>{report.live_query ? 'Consulta' : 'Exportación'} del {new Date(report.queried_at).toLocaleDateString('es-PE')} · {new Date(report.queried_at).toLocaleTimeString('es-PE', { hour: '2-digit', minute: '2-digit' })}</span></footer></>}
      </main></div>{detail && <Detail key={detail.transport_mode} row={detail} allModes={allModes} onClose={() => setDetailMode(null)} />}
  </div>;
}
