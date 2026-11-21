import '../../P3/web/analytics.js';

export const dataTools = globalThis.LuminaData;
export const emptyFilters = { from: '', to: '', county: '', hour: '', modes: [] };
export const modeNames = {
  LUMINA_BIKE_AND_OART: 'Bicicleta + OART',
  LUMINA_BUS_RAPID: 'Bus rápido',
  LUMINA_EV_MINIVAN_POOL: 'Minivan eléctrica',
  LUMINA_MULTIMODAL_OART: 'Multimodal OART',
  PERSONAL_CAR_BENCHMARK: 'Auto de referencia',
};
export const modeColors = ['#24746b', '#6c91a8', '#9b88b5', '#c19b6b', '#a6adb9'];
export const nameOf = mode => modeNames[mode] || String(mode ?? 'Sin dato').replaceAll('_', ' ');
export const format = (value, digits = 2) => value == null || !Number.isFinite(Number(value))
  ? 'N/D' : Number(value).toLocaleString('es-PE', { maximumFractionDigits: digits });
export const short = value => value == null ? 'N/D' : new Intl.NumberFormat('es-PE', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
export const dateLabel = (value, long = false) => value ? new Date(value + 'T12:00:00').toLocaleDateString('es-PE', { day: 'numeric', month: long ? 'long' : 'short' }) : 'Sin fecha';

export function selectRows(report, cube, filters, ignoreModes = false) {
  const modes = ignoreModes ? [] : filters.modes;
  return cube ? dataTools.filter(cube, { ...filters, modes, counties: filters.county ? [filters.county] : [] })
    : (report?.by_mode || []).filter(row => !modes.length || modes.includes(row.transport_mode));
}

export function exportSelection(rows, filters, report) {
  const query = report.exploration?.status === 'ok' ? report.exploration : report;
  const values = rows.map(row => ({ ...row, source: report.source, query_job_id: query.job_id,
    queried_at: query.queried_at, filter_from: filters.from, filter_to: filters.to,
    filter_county: filters.county, filter_hour: filters.hour, filter_modes: filters.modes.join('|') }));
  const columns = ['transport_mode', 'total_trips', 'total_km', 'total_revenue_usd', 'revenue_per_km_usd',
    'avg_duration_min', 'avg_passengers', 'missing_distance_rows', 'missing_amount_rows',
    'negative_distance_rows', 'source', 'query_job_id', 'queried_at', 'filter_from', 'filter_to',
    'filter_county', 'filter_hour', 'filter_modes'];
  return '\uFEFF' + dataTools.csv(values, columns);
}

export function answerQuestion(question, rows, selected) {
  const q = question.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (!selected.length) return 'No hay registros para los filtros seleccionados. Prueba con otro período o condado.';
  const summary = dataTools.summary(rows);
  if (/calidad|nulos|negativ/.test(q)) return `La selección contiene ${format(dataTools.sum(selected, 'missing_distance_rows'), 0)} distancias nulas, ${format(dataTools.sum(selected, 'missing_amount_rows'), 0)} importes nulos y ${format(dataTools.sum(selected, 'negative_distance_rows'), 0)} distancias negativas.`;
  if (/duracion|minutos|tarifa|costo/.test(q)) {
    const duration = /duracion|minutos/.test(q), key = duration ? 'avg_duration_min' : 'revenue_per_km_usd';
    const valid = rows.filter(row => row[key] != null).sort((a, b) => a[key] - b[key]);
    return valid.length ? valid.map(row => `${nameOf(row.transport_mode)}: ${format(row[key])} ${duration ? 'min' : 'USD/km'}`).join('\n') : 'Esta selección no tiene mediciones disponibles.';
  }
  if (/viajes|registros|mayor|mas/.test(q)) {
    const top = [...rows].sort((a, b) => b.total_trips - a.total_trips)[0];
    return `${nameOf(top.transport_mode)} concentra el mayor número de registros: ${format(top.total_trips, 0)}, el ${format(top.total_trips / summary.total_trips * 100, 1)} % de la selección.`;
  }
  if (/resumen|seleccion/.test(q)) return `Hay ${format(summary.total_trips, 0)} registros en ${rows.length} modos, con ${format(summary.total_km)} km acumulados. Los importes de tarifa suman USD ${format(summary.total_revenue_usd)}, excluyendo el auto de referencia.`;
  return 'Puedes consultar el resumen, el modo con más viajes, la duración, la tarifa o la calidad de tu selección.';
}
