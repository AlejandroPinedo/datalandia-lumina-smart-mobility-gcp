import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ArrowDownUp, Bookmark, BusFront, Bike, CarFront, TramFront, ChevronRight,
  ChevronDown, Check, Clock3, Coins, Database, Download, ExternalLink, GitCompareArrows,
  Info, LayoutDashboard, MapPin, MessageSquareText, Network, RefreshCw, Route, Search,
  Share2, SlidersHorizontal, Trash2, X } from 'lucide-react';
import { dateLabel, format, nameOf, dataTools } from './data.js';
import { alternativesFor, zonesFrom, sortAlternatives, tripExamples, routeAnswer, parseSaved, savedKey } from './passenger-model.js';
import './passenger.css';

const STORAGE_KEY = 'lumina.saved-routes.v1';
const modeIcons = { LUMINA_BIKE_AND_OART: Bike, LUMINA_BUS_RAPID: BusFront,
  LUMINA_EV_MINIVAN_POOL: CarFront, LUMINA_MULTIMODAL_OART: TramFront, PERSONAL_CAR_BENCHMARK: CarFront };
const navItems = [['viajar', 'Trayectos', Route], ['registros', 'Viajes', Clock3],
  ['guardados', 'Guardados', Bookmark], ['red', 'Conexiones', Network]];
const getView = () => navItems.some(([id]) => id === location.hash.slice(1)) ? location.hash.slice(1) : 'viajar';
function ModeSymbol({ mode }) { const Icon = modeIcons[mode] || BusFront; return <span className={`p-mode-icon ${mode === 'PERSONAL_CAR_BENCHMARK' ? 'reference' : ''}`}><Icon size={23} strokeWidth={1.6} /></span>; }
function OptionSelect({ label, value, onChange, children, disabled, id }) {
  return <label className="p-field" htmlFor={id}><span>{label}</span><span className="p-select"><select id={id} value={value} onChange={e => onChange(e.target.value)} disabled={disabled}>{children}</select><ChevronDown size={15} /></span></label>;
}
function Blank({ title, children, icon: Icon = Route }) { return <div className="p-empty"><span><Icon size={26} strokeWidth={1.4} /></span><h3>{title}</h3><p>{children}</p></div>; }
function saveCsv(records, report) {
  const columns = ['trip_id', 'travel_date', 'pickup_datetime', 'origin_zone_id', 'origin_zone_name', 'dest_zone_id', 'county_name', 'transport_mode', 'trip_distance_km', 'lumina_duration_min', 'lumina_total_fare_usd', 'source', 'query_job_id', 'queried_at', 'export_scope'];
  const values = records.map(record => ({ ...record, source: report.source, query_job_id: report.job_id, queried_at: report.queried_at, export_scope: report.trip_examples_scope }));
  const csv = '\uFEFF' + dataTools.csv(values, columns);
  const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }));
  const link = document.createElement('a'); link.href = url; link.download = 'lumina-registros-disponibles.csv';
  document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const ZONE_PRIMARY_OART = {
  'ZDL-001': 'OART-ST-01', 'ZDL-002': 'OART-ST-02', 'ZDL-003': 'OART-ST-03', 'ZDL-004': 'OART-ST-04',
  'ZDL-005': 'OART-ST-05', 'ZDL-006': 'OART-ST-06', 'ZDL-007': 'OART-ST-09', 'ZDL-008': 'OART-ST-10',
  'ZDL-009': 'OART-ST-11', 'ZDL-010': 'OART-ST-15', 'ZDL-011': 'OART-ST-16', 'ZDL-012': 'OART-ST-17',
  'ZDL-013': 'OART-ST-21', 'ZDL-014': 'OART-ST-22', 'ZDL-015': 'OART-ST-23', 'ZDL-016': 'OART-ST-28',
  'ZDL-017': 'OART-ST-29', 'ZDL-018': 'OART-ST-30', 'ZDL-019': 'OART-ST-33', 'ZDL-020': 'OART-ST-34',
  'ZDL-021': 'OART-ST-35', 'ZDL-022': 'OART-ST-40', 'ZDL-023': 'OART-ST-41', 'ZDL-024': 'OART-ST-42',
};
function describeModeStages(mode, originLabel, destLabel, originStation, destStation) {
  const oSt = originStation || 'estación OART de origen';
  const dSt = destStation || 'estación OART de destino';
  switch (mode) {
    case 'LUMINA_BIKE_AND_OART':
      return {
        title: 'Bicicleta + Tren OART (Primera y última milla)',
        summary: 'Combina bicicleta compartida en los extremos con el tren metropolitano OART para evitar la congestión vial.',
        steps: [
          ['1. Primera milla (Bici)', `Recorrido en bicicleta desde ${originLabel} hasta ${oSt}.`],
          ['2. Tramo troncal (OART)', oSt === dSt ? `Conexión ferroviaria local en ${oSt}.` : `Viaje en tren rápido OART de ${oSt} a ${dSt}.`],
          ['3. Última milla', `Tramo final en bici o caminata desde ${dSt} hasta ${destLabel}.`],
        ],
      };
    case 'LUMINA_BUS_RAPID':
      return {
        title: 'Bus Rápido (BRT con carril exclusivo)',
        summary: 'Corredor de autobús de alta capacidad con prioridad semafórica y alimentación directa a la red OART.',
        steps: [
          ['1. Abordaje BRT', `Subida en paradero troncal de ${originLabel}.`],
          ['2. Corredor expreso', `Recorrido por carril segregado con enlace en ${dSt}.`],
          ['3. Llegada', `Descenso en zona de destino: ${destLabel}.`],
        ],
      };
    case 'LUMINA_EV_MINIVAN_POOL':
      return {
        title: 'Minivan Eléctrica Compartida (Ride-Pooling)',
        summary: 'Vehículo eléctrico de baja emisión compartido entre pasajeros con rutas similares, sin depender de auto propio.',
        steps: [
          ['1. Recojo compartido', `Punto de encuentro en ${originLabel}.`],
          ['2. Traslado directo / alimentador', `Ruta optimizada hacia ${destLabel} (conexión opcional en ${dSt}).`],
        ],
      };
    case 'LUMINA_MULTIMODAL_OART':
      return {
        title: 'Multimodal OART (Tarifa integrada)',
        summary: 'Integra transporte alimentador urbano + tren OART + transferencia final bajo un solo viaje y tarifa unificada.',
        steps: [
          ['1. Alimentador de origen', `Bus/micromovilidad desde ${originLabel} hacia ${oSt}.`],
          ['2. Tren metropolitano OART', oSt === dSt ? `Operación en nodo ${oSt}.` : `Tramo principal de alta velocidad ${oSt} → ${dSt}.`],
          ['3. Transferencia de destino', `Enlace integrado hasta ${destLabel}.`],
        ],
      };
    case 'PERSONAL_CAR_BENCHMARK':
      return {
        title: 'Auto Particular (Referencia vial)',
        summary: 'Línea base en automóvil propio por red vial urbana (sujeto a congestión y costo fijo + variable por km).',
        steps: [
          ['1. Trayecto directo vial', `Conducción continua de ${originLabel} a ${destLabel} sin integración OART.`],
        ],
      };
    default:
      return null;
  }
}

function RouteScheme({ origin, destination, stations = [], alternatives = [], activeAlternative, zoneName }) {
  const sameZone = origin === destination;
  const totalTrips = alternatives.reduce((sum, item) => sum + item.totalTrips, 0);
  const durations = alternatives.map(item => item.avgDuration).filter(v => v != null);
  const distances = alternatives.map(item => item.avgDistance).filter(v => v != null);
  const fares = alternatives.map(item => item.avgFare).filter(v => v != null);
  const fastest = sortAlternatives(alternatives.filter(item => item.avgDuration != null), 'duration')[0];
  const cheapest = sortAlternatives(alternatives.filter(item => item.avgFare != null), 'fare')[0];
  const counties = [...new Set(alternatives.flatMap(item => item.counties))].filter(Boolean);
  const originStation = ZONE_PRIMARY_OART[origin];
  const destStation = ZONE_PRIMARY_OART[destination];
  const primaryStations = [...new Set([originStation, destStation].filter(Boolean))];
  const extraStations = stations.filter(s => !primaryStations.includes(s));
  const modeInfo = activeAlternative ? describeModeStages(activeAlternative.mode, zoneName(origin), zoneName(destination), originStation, destStation) : null;

  return <section className="p-scheme"><div className="p-section-heading"><div><h2>Tu trayecto, de un vistazo</h2><p>{sameZone ? 'Desplazamiento dentro de la misma zona' : 'Conexión entre distintas zonas'} · {counties.join(', ') || 'DataLandia'}</p></div><span className="p-label">{sameZone ? 'Misma zona' : 'Entre zonas'}</span></div>
    <div className="p-route-line"><div className="p-endpoint"><span className="p-point origin"><MapPin size={19} /></span><small>ORIGEN</small><strong>{zoneName(origin)}</strong><span>{origin}{originStation ? ` · ${originStation}` : ''}</span></div><div className="p-connection"><span /><div className="p-connection-pill"><Route size={16} strokeWidth={1.6} />{durations.length > 0 && <strong>{format(Math.min(...durations), 1)}–{format(Math.max(...durations), 1)} min</strong>}{distances.length > 0 && <small>{format(Math.min(...distances), 1)}–{format(Math.max(...distances), 1)} km</small>}</div><span /></div><div className="p-endpoint"><span className="p-point destination"><MapPin size={19} /></span><small>DESTINO</small><strong>{zoneName(destination)}</strong><span>{destination}{destStation ? ` · ${destStation}` : ''}</span></div></div>
    {alternatives.length > 0 && <div className="p-scheme-highlights">
      {fastest && <div><small>Más rápido</small><strong>{nameOf(fastest.mode)}</strong><span>{format(fastest.avgDuration, 1)} min</span></div>}
      {cheapest && <div><small>Menor tarifa</small><strong>{nameOf(cheapest.mode)}</strong><span>USD {format(cheapest.avgFare)}</span></div>}
      <div><small>Volumen observado</small><strong>{format(totalTrips, 0)} viajes</strong><span>{fares.length > 0 ? `USD ${format(Math.min(...fares))} – ${format(Math.max(...fares))}` : 'N/D'}</span></div>
    </div>}
    {modeInfo && <div className="p-mode-breakdown">
      <div className="p-mode-breakdown-head"><strong>{modeInfo.title}</strong><span>{format(activeAlternative.avgDuration, 1)} min · USD {format(activeAlternative.avgFare)} · {format(activeAlternative.avgDistance)} km</span></div>
      <p>{modeInfo.summary}</p>
      <ul>{modeInfo.steps.map(([label, text]) => <li key={label}><b>{label}:</b> {text}</li>)}</ul>
    </div>}
    {stations.length > 0 && <div className="p-station-associations"><span><TramFront size={16} />Conexiones OART vinculadas</span><div>{primaryStations.map(station => <span className="p-station p-station-primary" key={station}>{station} {station === originStation && station === destStation ? '(Origen/Destino)' : station === originStation ? '(Origen)' : '(Destino)'}</span>)}{extraStations.map(station => <span className="p-station" key={station}>{station}</span>)}</div><p>La fuente vincula estas estaciones al trayecto; no especifica una secuencia de paradas.</p></div>}
    <p className="p-fine-print">Diagrama sin escala geográfica. No representa navegación ni ubicación de vehículos en vivo.</p>
  </section>;
}

function TripDialog({ trip, zoneName, onClose }) {
  const dialog = useRef(null);
  useEffect(() => { dialog.current.showModal(); }, []);
  return <dialog ref={dialog} className="p-trip-dialog" aria-labelledby="trip-detail-title" onCancel={onClose} onClick={e => { if (e.target === e.currentTarget) onClose(); }}><div className="p-dialog-heading"><ModeSymbol mode={trip.transport_mode} /><button className="icon-button" aria-label="Cerrar viaje" onClick={onClose}><X size={21} /></button></div><span className="p-eyebrow">REGISTRO DE GOLD</span><h2 id="trip-detail-title">Detalle del viaje</h2><p className="p-trip-id">{trip.trip_id}</p><div className="p-dialog-route"><span><i />{zoneName(trip.origin_zone_id)}</span><span><i />{zoneName(trip.dest_zone_id)}</span></div><dl className="p-trip-metrics">{[
    ['Transporte', nameOf(trip.transport_mode)], ['Fecha de viaje', dateLabel(trip.travel_date, true)],
    ['Salida registrada', trip.pickup_datetime ? new Date(trip.pickup_datetime).toLocaleString('es-PE') : 'N/D'],
    ['Condado de origen', trip.county_name], ['Duración registrada', `${format(trip.lumina_duration_min)} min`],
    ['Distancia registrada', `${format(trip.trip_distance_km)} km`], ['Tarifa registrada', `USD ${format(trip.lumina_total_fare_usd)}`],
  ].map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value ?? 'N/D'}</dd></div>)}</dl><p className="p-fine-print">Este es un registro de la fuente histórica. No se ha asociado a tu identidad ni representa una reserva.</p><button className="button p-dialog-close" onClick={onClose}>Cerrar detalle</button></dialog>;
}

function Records({ routes, zones, zoneName, report, initialOrigin = '', initialDestination = '', onDetail }) {
  const [filters, setFilters] = useState({ origin: initialOrigin, destination: initialDestination, mode: '', date: '', search: '' });
  const [page, setPage] = useState(0);
  const change = (key, value) => { setFilters(f => ({ ...f, [key]: value })); setPage(0); };
  const modes = [...new Set(routes.map(row => row.transport_mode))];
  const allTrips = useMemo(() => tripExamples(routes), [routes]);
  const records = useMemo(() => tripExamples(routes, filters), [routes, filters]);
  const perPage = 15, pages = Math.max(1, Math.ceil(records.length / perPage));
  const currentPage = Math.min(page, pages - 1);
  return <section className="p-records"><div className="p-section-heading"><div><h2>Viajes registrados</h2><p>Consulta el detalle de los últimos registros exportados de cada alternativa.</p></div><button className="button" disabled={!records.length} onClick={() => saveCsv(records, report)}><Download size={15} />Descargar CSV</button></div>
    <div className="p-scope-note"><Info size={17} /><p>Disponibles: <strong>{format(allTrips.length, 0)} registros</strong>. La exportación incluye hasta los 3 últimos viajes por origen, destino, condado y modo. No es el listado completo de Gold ni un historial personal.</p></div>
    <div className="p-record-filters"><label className="p-field"><span>ID del viaje</span><span className="p-search-input"><Search size={15} /><input placeholder="Buscar un ID…" value={filters.search} onChange={e => change('search', e.target.value)} /></span></label><OptionSelect id="record-origin" label="Origen" value={filters.origin} onChange={value => change('origin', value)}><option value="">Todos los orígenes</option>{zones.filter(z => z.origin).map(z => <option key={z.id} value={z.id}>{z.name}</option>)}</OptionSelect><OptionSelect id="record-destination" label="Destino" value={filters.destination} onChange={value => change('destination', value)}><option value="">Todos los destinos</option>{zones.filter(z => z.destination).map(z => <option key={z.id} value={z.id}>{z.name}</option>)}</OptionSelect><OptionSelect id="record-mode" label="Transporte" value={filters.mode} onChange={value => change('mode', value)}><option value="">Todos los modos</option>{modes.map(mode => <option key={mode} value={mode}>{nameOf(mode)}</option>)}</OptionSelect><label className="p-field"><span>Fecha de viaje</span><input type="date" value={filters.date} onChange={e => change('date', e.target.value)} /></label></div>
    {(Object.values(filters).some(Boolean)) && <button className="p-text-button p-reset-records" onClick={() => { setFilters({ origin: '', destination: '', mode: '', date: '', search: '' }); setPage(0); }}>Limpiar filtros</button>}
    <div className="p-record-table"><table><thead><tr><th>Fecha / ID</th><th>Trayecto</th><th>Transporte</th><th>Minutos</th><th>Tarifa USD</th><th><span className="sr-only">Detalle</span></th></tr></thead><tbody>{records.slice(currentPage * perPage, (currentPage + 1) * perPage).map(trip => <tr key={[trip.trip_id, trip.transport_mode, trip.origin_zone_id, trip.dest_zone_id, trip.county_name].join('|')}><td><span className="p-record-date">{dateLabel(trip.travel_date)}</span><button className="p-record-id" title={trip.trip_id} onClick={() => onDetail(trip)}>{trip.trip_id}</button></td><td><span className="p-record-zone">{zoneName(trip.origin_zone_id)}</span><span className="p-record-destination">a {zoneName(trip.dest_zone_id)}</span></td><td>{nameOf(trip.transport_mode)}</td><td>{format(trip.lumina_duration_min)}</td><td>{format(trip.lumina_total_fare_usd)}</td><td><button className="icon-button" aria-label={`Ver viaje ${trip.trip_id}`} onClick={() => onDetail(trip)}><ChevronRight size={17} /></button></td></tr>)}</tbody></table></div>
    {!records.length && <Blank title="No encontramos registros con estos filtros">Prueba otra fecha o zona. La búsqueda se realiza sobre los últimos registros exportados, no sobre todos los viajes de Gold.</Blank>}
    <div className="p-pagination"><span>{format(records.length, 0)} registros encontrados</span><div><button className="button compact" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Anterior</button><span>{currentPage + 1} / {pages}</span><button className="button compact" disabled={currentPage >= pages - 1} onClick={() => setPage(currentPage + 1)}>Siguiente</button></div></div>
  </section>;
}

function ConnectionNetwork({ routes, zones, zoneName, onOpen }) {
  const [origin, setOrigin] = useState(''), [search, setSearch] = useState('');
  const grouped = useMemo(() => {
    const pairs = new Map();
    for (const route of routes) {
      if (origin && route.origin_zone_id !== origin) continue;
      const key = JSON.stringify([route.origin_zone_id, route.dest_zone_id]);
      if (!pairs.has(key)) pairs.set(key, { origin: route.origin_zone_id, destination: route.dest_zone_id, count: 0, modes: new Set() });
      pairs.get(key).count += Number(route.total_trips); pairs.get(key).modes.add(route.transport_mode);
    }
    return [...pairs.values()].filter(pair => `${zoneName(pair.origin)} ${zoneName(pair.destination)}`.toLowerCase().includes(search.toLowerCase())).sort((a, b) => b.count - a.count);
  }, [routes, origin, search, zoneName]);
  const [page, setPage] = useState(0), pageSize = 12, totalPages = Math.max(1, Math.ceil(grouped.length / pageSize));
  const currentPage = Math.min(page, totalPages - 1);
  return <section className="p-network"><div className="p-section-heading"><div><h2>Red de conexiones observadas</h2><p>Explora las zonas conectadas en el histórico y abre sus alternativas.</p></div><Network size={24} strokeWidth={1.4} /></div><div className="p-network-filters"><OptionSelect id="network-origin" label="Zona de origen" value={origin} onChange={value => { setOrigin(value); setPage(0); }}><option value="">Todas las zonas</option>{zones.filter(zone => zone.origin).map(zone => <option key={zone.id} value={zone.id}>{zone.name}</option>)}</OptionSelect><label className="p-field"><span>Buscar conexión</span><span className="p-search-input"><Search size={16} /><input placeholder="Nombre de una zona…" value={search} onChange={e => { setSearch(e.target.value); setPage(0); }} /></span></label></div><p className="p-network-count">{format(grouped.length, 0)} conexiones registradas · esquema sin escala geográfica</p><div className="p-network-grid">{grouped.slice(currentPage * pageSize, (currentPage + 1) * pageSize).map(pair => <button className="p-network-card" key={JSON.stringify([pair.origin, pair.destination])} onClick={() => onOpen(pair.origin, pair.destination)}><div className="p-mini-route"><span /><i /><MapPin size={16} /><i /><span /></div><strong>{zoneName(pair.origin)}</strong><span className="p-network-to">a {zoneName(pair.destination)}</span><div><span>{pair.modes.size} modos</span><span>{format(pair.count, 0)} registros</span><ChevronRight size={16} /></div></button>)}</div>{!grouped.length && <Blank title="No hay conexiones con estos filtros">Selecciona otra zona o cambia la búsqueda.</Blank>}<div className="p-pagination"><span>Conexiones basadas en registros de Gold</span><div><button className="button compact" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Anterior</button><span>{currentPage + 1} / {totalPages}</span><button className="button compact" disabled={currentPage >= totalPages - 1} onClick={() => setPage(currentPage + 1)}>Siguiente</button></div></div></section>;
}

export default function PassengerApp() {
  const [view, setView] = useState(getView), [report, setReport] = useState(null), [loading, setLoading] = useState(true), [error, setError] = useState('');
  const params = new URLSearchParams(location.search);
  const [origin, setOrigin] = useState(params.get('origen') || ''), [destination, setDestination] = useState(params.get('destino') || '');
  const [query, setQuery] = useState(params.get('origen') && params.get('destino') ? { origin: params.get('origen'), destination: params.get('destino') } : null);
  const [preference, setPreference] = useState('duration'), [selectedMode, setSelectedMode] = useState(''), [trip, setTrip] = useState(null);
  const [saved, setSaved] = useState(() => { try { return parseSaved(localStorage.getItem(STORAGE_KEY)); } catch { return []; } });
  const [notice, setNotice] = useState(''), [question, setQuestion] = useState(''), [asked, setAsked] = useState('');
  const requestRef = useRef(null), resultsRef = useRef(null);
  useEffect(() => {
    if (query && view === 'viajar') resultsRef.current?.scrollIntoView({ block: 'start', behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' });
  }, [query, view]);
  const fetchData = async () => {
    requestRef.current?.abort(); const request = new AbortController(); requestRef.current = request;
    setLoading(true); setError('');
    try {
      const response = await fetch('/api/v1/passenger-data', { cache: 'no-store', signal: request.signal }), data = await response.json();
      if (!response.ok || data.status !== 'ok' || data.data_origin !== 'existing_project_data' || !Array.isArray(data.routes)) throw new Error(data.reason || 'No hay una consulta válida de trayectos disponible.');
      setReport(data);
    } catch (failure) { if (failure.name !== 'AbortError') { setReport(null); setError(failure.message); } }
    finally { if (requestRef.current === request) setLoading(false); }
  };
  useEffect(() => { fetchData(); return () => requestRef.current?.abort(); }, []);
  useEffect(() => { const handler = () => { if (location.hash !== '#passenger-content') setView(getView()); }; window.addEventListener('hashchange', handler); return () => window.removeEventListener('hashchange', handler); }, []);
  const navigate = next => { location.hash = next; setView(next); window.scrollTo({ top: 0, behavior: 'instant' }); };
  const routes = report?.routes || [];
  const zones = useMemo(() => zonesFrom(routes), [report]);
  const zoneMap = useMemo(() => new Map(zones.map(zone => [zone.id, zone.name])), [zones]);
  const zoneName = id => zoneMap.get(id) || id || 'Zona sin dato';
  const alternatives = useMemo(() => query ? alternativesFor(routes, query.origin, query.destination) : [], [report, query]);
  const sorted = useMemo(() => sortAlternatives(alternatives, preference), [alternatives, preference]);
  const activeAlternative = sorted.find(item => item.mode === selectedMode) || sorted[0];
  const totalRecords = alternatives.reduce((n, item) => n + item.totalTrips, 0);
  const dates = routes.flatMap(route => [route.first_date, route.last_date]).filter(Boolean).sort();
  const firstDate = dates[0], lastDate = dates.at(-1);
  const searchRoute = (from, to, mode = '') => { setOrigin(from); setDestination(to); setQuery({ origin: from, destination: to }); setSelectedMode(mode); setAsked(''); navigate('viajar'); };
  const saveRoute = mode => {
    if (!query) return;
    const item = { ...query, mode, savedAt: new Date().toISOString() };
    if (saved.some(value => savedKey(value) === savedKey(item))) { setNotice('Este trayecto ya está guardado.'); return; }
    const next = [...saved, item];
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)); setSaved(next); setNotice('Trayecto guardado en este navegador. Lo encontrarás en Guardados.'); }
    catch { setNotice('El navegador no permitió guardar el trayecto. Revisa el almacenamiento local.'); }
  };
  const removeSaved = item => { const next = saved.filter(value => savedKey(value) !== savedKey(item)); try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)); setSaved(next); setNotice('Trayecto eliminado de Guardados.'); } catch { setNotice('El navegador no permitió actualizar Guardados.'); } };
  const shareRoute = async () => {
    if (!query) return;
    const url = new URL(location.href); url.search = new URLSearchParams({ origen: query.origin, destino: query.destination }).toString(); url.hash = 'viajar';
    try { await navigator.clipboard.writeText(url.href); setNotice('Enlace del trayecto copiado. Funciona en un equipo que pueda acceder a esta web.'); }
    catch { setNotice('El navegador no permitió copiar el enlace.'); }
  };
  return <div className="traveler"><a className="skip-link" href="#passenger-content">Ir al contenido</a><header className="p-header"><a className="brand" href="#viajar" onClick={() => navigate('viajar')} aria-label="Lumina, buscar un trayecto"><span className="brand-mark"><Activity size={23} /></span><span>lumina<span className="brand-period">.</span></span></a><nav aria-label="Navegación del pasajero">{navItems.map(([id, title, Icon]) => <a key={id} href={`#${id}`} className={view === id ? 'active' : ''} aria-current={view === id ? 'page' : undefined} onClick={() => navigate(id)}><Icon size={17} /><span>{title}</span>{id === 'guardados' && saved.length > 0 && <small>{saved.length}</small>}</a>)}</nav><a className="p-analysis-link" href="#overview"><LayoutDashboard size={16} /><span>Análisis</span><ExternalLink size={12} /></a></header>
    <main className="p-main" id="passenger-content"><div className="p-page-title"><div><span className="p-eyebrow">DATALANDIA · MOVILIDAD URBANA</span><h1>{view === 'viajar' ? 'Encuentra tu próxima alternativa.' : view === 'guardados' ? 'Tus trayectos, a mano.' : view === 'red' ? 'Descubre cómo se conecta la ciudad.' : 'Consulta los viajes registrados.'}</h1><p>{view === 'viajar' ? 'Elige dos zonas y compara las opciones que ya aparecen en nuestros datos.' : view === 'guardados' ? 'Vuelve a consultar los trayectos que guardaste en este navegador.' : view === 'red' ? 'Explora las conexiones que aparecen en el histórico de movilidad.' : 'Revisa fechas, duración, distancia y tarifa de los registros disponibles.'}</p></div><button className="button p-refresh" onClick={fetchData} disabled={loading}><RefreshCw size={16} className={loading ? 'spinning' : ''} /><span>Actualizar</span></button></div>
    {notice && <div className="p-notice" role="status"><Info size={17} /><span>{notice}</span><button className="icon-button" aria-label="Cerrar aviso" onClick={() => setNotice('')}><X size={16} /></button></div>}
    {error && <div className="p-data-error" role="alert"><Database size={23} /><div><h2>Los trayectos aún no están disponibles</h2><p>{error}</p><button className="button" onClick={fetchData}>Volver a consultar</button></div></div>}
    {loading && !report && <div className="loading-state" role="status"><div className="skeleton skeleton-filters" /><div className="skeleton skeleton-chart" /><span>Consultando los trayectos existentes…</span></div>}
    {report && <><div className="p-data-strip"><span><Database size={14} />Información histórica</span><span>{dateLabel(firstDate)} – {dateLabel(lastDate)} {firstDate?.slice(0, 4)}</span><span>{format(report.total_source_records, 0)} registros consultados</span></div>
      {view === 'viajar' && <><div className="p-search-layout"><section className="p-search-card"><div className="p-search-title"><span className="p-tile-icon"><Route size={21} /></span><div><h2>¿De dónde a dónde?</h2><p>Busca por las zonas disponibles en el histórico.</p></div></div><form onSubmit={e => { e.preventDefault(); if (origin && destination) searchRoute(origin, destination); }}><div className="p-route-fields"><OptionSelect id="journey-origin" label="Origen" value={origin} onChange={setOrigin}><option value="">Selecciona tu punto de partida</option>{zones.filter(zone => zone.origin).map(zone => <option key={zone.id} value={zone.id}>{zone.name}</option>)}</OptionSelect><button className="p-swap" type="button" title="Invertir origen y destino" aria-label="Invertir origen y destino" onClick={() => { setOrigin(destination); setDestination(origin); }}><ArrowDownUp size={18} /></button><OptionSelect id="journey-destination" label="Destino" value={destination} onChange={setDestination}><option value="">Selecciona a dónde quieres ir</option>{zones.filter(zone => zone.destination).map(zone => <option key={zone.id} value={zone.id}>{zone.name}</option>)}</OptionSelect></div><button className="button primary p-search-submit" disabled={!origin || !destination || loading}><Search size={17} />Buscar alternativas</button></form><p className="p-search-note"><Info size={14} />Compararás promedios históricos. No son cotizaciones, horarios ni disponibilidad en tiempo real.</p></section><aside className="p-welcome-panel"><span className="p-eyebrow">UNA FORMA MÁS CLARA DE COMPARAR</span><h2>Tu trayecto.<br />Tus prioridades.</h2><p>Compara duración, tarifa y distancia para conocer mejor las alternativas registradas.</p><div className="p-welcome-modes"><span><Bike size={21} />Bici</span><span><BusFront size={21} />Bus</span><span><CarFront size={21} />Minivan</span><span><TramFront size={21} />OART</span></div><div className="p-welcome-caption">Cada resultado se calcula con registros existentes de Gold.</div></aside></div>
      {query && <div className="p-results" ref={resultsRef}><div className="p-results-heading"><div><span className="p-eyebrow">RESULTADOS DEL TRAYECTO</span><h2>{zoneName(query.origin)}<ChevronRight size={20} />{zoneName(query.destination)}</h2><p>{alternatives.length} alternativas observadas · {format(totalRecords, 0)} registros históricos</p></div>{alternatives.length > 0 && <button className="button" onClick={shareRoute}><Share2 size={15} />Compartir</button>}</div>
      {!alternatives.length ? <Blank title="No hay alternativas registradas para este trayecto">Prueba otra combinación de origen y destino. No se calculan rutas o tarifas de respaldo.</Blank> : <><div className="p-preferences" aria-label="Ordenar alternativas"><span>Ordenar por</span>{[['duration', 'Menor duración', Clock3], ['fare', 'Menor tarifa', Coins], ['distance', 'Menor distancia', Route]].map(([value, title, Icon]) => <button key={value} className={preference === value ? 'active' : ''} aria-pressed={preference === value} onClick={() => setPreference(value)}><Icon size={15} />{title}</button>)}</div><div className="p-alternative-grid">{sorted.map((item, index) => <article className={`p-alternative ${activeAlternative?.mode === item.mode ? 'is-selected' : ''}`} key={item.mode}><div className="p-alternative-top"><ModeSymbol mode={item.mode} /><button className={`icon-button ${saved.some(value => savedKey(value) === savedKey({ ...query, mode: item.mode })) ? 'p-bookmarked' : ''}`} aria-label={`Guardar ${nameOf(item.mode)}`} onClick={() => saveRoute(item.mode)}><Bookmark size={18} /></button></div><div className="p-alternative-title"><h3>{nameOf(item.mode)}</h3>{item.mode === 'PERSONAL_CAR_BENCHMARK' ? <span className="p-label">Referencia</span> : index === 0 && item[preference === 'fare' ? 'avgFare' : preference === 'distance' ? 'avgDistance' : 'avgDuration'] != null && <span className="p-best">Menor {preference === 'fare' ? 'tarifa' : preference === 'distance' ? 'distancia' : 'duración'}</span>}</div><div className="p-alt-duration"><strong>{format(item.avgDuration, 1)}</strong><span>min de media</span></div><dl className="p-alternative-metrics"><div><dt>Tarifa media</dt><dd>USD {format(item.avgFare)}</dd></div><div><dt>Distancia media</dt><dd>{format(item.avgDistance)} km</dd></div></dl><p className="p-observation-count">Basado en {format(item.totalTrips, 0)} registros</p><button className="button p-alt-select" onClick={() => setSelectedMode(item.mode)}>{activeAlternative?.mode === item.mode ? <><Check size={15} />Detalle seleccionado</> : <>Ver detalle<ChevronRight size={15} /></>}</button></article>)}</div>
      {activeAlternative && <div className="p-route-details"><RouteScheme origin={query.origin} destination={query.destination} stations={activeAlternative.stations} alternatives={alternatives} activeAlternative={activeAlternative} zoneName={zoneName} /><section className="p-alternative-detail"><div className="p-section-heading"><div><h2>{nameOf(activeAlternative.mode)}</h2><p>Datos utilizados para esta alternativa</p></div><GitCompareArrows size={20} /></div><dl><div><dt>Período observado</dt><dd>{dateLabel(activeAlternative.firstDate)} – {dateLabel(activeAlternative.lastDate)}</dd></div><div><dt>Mediciones de duración</dt><dd>{format(activeAlternative.durationCount, 0)}</dd></div><div><dt>Mediciones de tarifa</dt><dd>{format(activeAlternative.fareCount, 0)}</dd></div><div><dt>Condados de origen</dt><dd>{activeAlternative.counties.join(', ') || 'Sin dato'}</dd></div></dl><button className="button" onClick={() => navigate('registros')}><Clock3 size={15} />Consultar registros del trayecto</button><p className="p-fine-print">Los promedios conservan los valores ausentes y se calculan con las mediciones disponibles, no promediando promedios.</p></section></div>}
      <section className="p-route-questions"><div className="p-section-heading"><div><h2>Consulta este trayecto</h2><p>Respuestas calculadas a partir de las alternativas seleccionadas.</p></div><MessageSquareText size={21} /></div><div className="p-quick-questions">{['¿Cuál tiene menor duración?', '¿Cuál tiene menor tarifa?', '¿Qué conexiones OART hay?'].map(value => <button key={value} onClick={() => { setQuestion(value); setAsked(value); }}>{value}</button>)}</div><form onSubmit={e => { e.preventDefault(); setAsked(question); }}><input aria-label="Pregunta sobre el trayecto" placeholder="Pregunta por duración, tarifa, modos o conexiones…" value={question} onChange={e => setQuestion(e.target.value)} required /><button className="button primary">Consultar</button></form>{asked && <div className="p-route-answer" role="status">{routeAnswer(asked, alternatives)}</div>}</section></>}
      </div>}
      {!query && <section className="p-how-it-works"><div><span className="p-how-icon"><MapPin size={20} /></span><h3>Elige tus zonas</h3><p>Selecciona un origen y un destino presentes en los datos.</p></div><div><span className="p-how-icon"><GitCompareArrows size={20} /></span><h3>Compara alternativas</h3><p>Revisa el tiempo, la tarifa y la distancia registrados.</p></div><div><span className="p-how-icon"><Bookmark size={20} /></span><h3>Guarda tu trayecto</h3><p>Ten tus consultas frecuentes a mano en este navegador.</p></div></section>}
      </>}
      {view === 'registros' && <Records routes={routes} zones={zones} zoneName={zoneName} report={report} initialOrigin={query?.origin || ''} initialDestination={query?.destination || ''} onDetail={setTrip} />}
      {view === 'red' && <ConnectionNetwork routes={routes} zones={zones} zoneName={zoneName} onOpen={searchRoute} />}
    </>}
    {view === 'guardados' && <section className="p-saved"><div className="p-section-heading"><div><h2>Trayectos guardados</h2><p>Se guardan en este navegador. No son reservas ni un historial asociado a una cuenta.</p></div><span className="p-label">{saved.length} guardados</span></div>{!saved.length ? <Blank title="Tus trayectos favoritos aparecerán aquí" icon={Bookmark}>Busca un trayecto y usa el icono de guardar de una alternativa para volver a consultarla después.</Blank> : <div className="p-saved-grid">{saved.map(item => <article key={savedKey(item)}><div className="p-saved-heading"><ModeSymbol mode={item.mode} /><button className="icon-button" aria-label={`Eliminar trayecto ${zoneName(item.origin)} a ${zoneName(item.destination)}`} onClick={() => removeSaved(item)}><Trash2 size={17} /></button></div><h3>{zoneName(item.origin)}</h3><p>a {zoneName(item.destination)}</p><span>{nameOf(item.mode)}</span><button className="button" disabled={!report} onClick={() => searchRoute(item.origin, item.destination, item.mode)}>Consultar de nuevo<ChevronRight size={15} /></button></article>)}</div>}</section>}
    <footer className="p-footer"><div><a className="p-footer-brand" href="#viajar">lumina.</a><span>Movilidad urbana de DataLandia</span></div><div><a href="#overview">Panel de análisis</a><a href="#source">Fuente de datos</a></div>{report && <p>Consulta histórica del {new Date(report.queried_at).toLocaleString('es-PE')}. La procedencia externa de Gold no ha sido verificada de forma independiente.</p>}</footer>
    </main>{trip && <TripDialog trip={trip} zoneName={zoneName} onClose={() => setTrip(null)} />}</div>;
}
