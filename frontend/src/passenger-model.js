import { nameOf, format } from './data.js';

export function zonesFrom(routes) {
  const zones = new Map();
  for (const route of routes) {
    if (route.origin_zone_id != null) {
      const existing = zones.get(route.origin_zone_id) || { id: route.origin_zone_id, nameWeights: new Map(), counties: new Set() };
      if (route.origin_zone_name) {
        existing.nameWeights.set(route.origin_zone_name, (existing.nameWeights.get(route.origin_zone_name) || 0) + Number(route.total_trips || 1));
      }
      if (route.county_name) existing.counties.add(route.county_name);
      zones.set(existing.id, existing);
    }
  }
  for (const route of routes) if (route.dest_zone_id != null && !zones.has(route.dest_zone_id)) {
    zones.set(route.dest_zone_id, { id: route.dest_zone_id, nameWeights: new Map(), counties: new Set() });
  }
  return [...zones.values()].map(zone => {
    const bestName = [...zone.nameWeights.entries()].sort((a, b) => b[1] - a[1])[0]?.[0];
    return {
      id: zone.id,
      name: bestName || zone.id,
      counties: [...zone.counties], origin: routes.some(r => r.origin_zone_id === zone.id),
      destination: routes.some(r => r.dest_zone_id === zone.id),
    };
  }).sort((a, b) => a.name.localeCompare(b.name));
}

export function alternativesFor(routes, origin, destination) {
  const grouped = new Map();
  for (const route of routes) {
    if (route.origin_zone_id !== origin || route.dest_zone_id !== destination) continue;
    if (!grouped.has(route.transport_mode)) grouped.set(route.transport_mode, {
      mode: route.transport_mode, totalTrips: 0, distanceSum: null, distanceCount: 0,
      durationSum: null, durationCount: 0, fareSum: null, fareCount: 0,
      stations: new Set(), counties: new Set(), dates: [], trips: [],
    });
    const value = grouped.get(route.transport_mode);
    value.totalTrips += Number(route.total_trips);
    for (const key of ['distance', 'duration', 'fare']) {
      if (route[key + '_sum'] != null) value[key + 'Sum'] = (value[key + 'Sum'] ?? 0) + Number(route[key + '_sum']);
      value[key + 'Count'] += Number(route[key + '_count'] || 0);
    }
    for (const station of route.station_ids || []) value.stations.add(station);
    if (route.county_name) value.counties.add(route.county_name);
    if (route.first_date) value.dates.push(route.first_date);
    if (route.last_date) value.dates.push(route.last_date);
    for (const trip of route.recent_trips || []) if (trip && typeof trip === 'object') value.trips.push({
      ...trip, transport_mode: route.transport_mode, origin_zone_id: origin,
      dest_zone_id: destination, origin_zone_name: route.origin_zone_name,
      county_name: route.county_name,
    });
  }
  return [...grouped.values()].map(value => ({ ...value,
    avgDistance: value.distanceCount > 0 && value.distanceSum != null ? value.distanceSum / value.distanceCount : null,
    avgDuration: value.durationCount > 0 && value.durationSum != null ? value.durationSum / value.durationCount : null,
    avgFare: value.fareCount > 0 && value.fareSum != null ? value.fareSum / value.fareCount : null,
    stations: [...value.stations].sort(), counties: [...value.counties].sort(),
    firstDate: value.dates.sort()[0], lastDate: value.dates.sort().at(-1),
  }));
}

export function sortAlternatives(values, preference) {
  const metric = preference === 'fare' ? 'avgFare' : preference === 'distance' ? 'avgDistance' : 'avgDuration';
  return [...values].sort((a, b) => a[metric] == null ? b[metric] == null ? 0 : 1 : b[metric] == null ? -1 : a[metric] - b[metric]);
}

export function tripExamples(routes, { origin = '', destination = '', mode = '', date = '', search = '' } = {}) {
  const unique = new Map();
  for (const route of routes) {
    if (origin && route.origin_zone_id !== origin || destination && route.dest_zone_id !== destination || mode && route.transport_mode !== mode) continue;
    for (const trip of route.recent_trips || []) {
      if (!trip || typeof trip !== 'object' || !trip.trip_id) continue;
      if (date && trip.travel_date !== date || search && !String(trip.trip_id).toLowerCase().includes(search.toLowerCase())) continue;
      const record = { ...trip, transport_mode: route.transport_mode, origin_zone_id: route.origin_zone_id,
        dest_zone_id: route.dest_zone_id, origin_zone_name: route.origin_zone_name, county_name: route.county_name };
      unique.set([record.trip_id, record.transport_mode, record.origin_zone_id, record.dest_zone_id, record.county_name].join('|'), record);
    }
  }
  return [...unique.values()].sort((a, b) => String(b.pickup_datetime || b.travel_date).localeCompare(String(a.pickup_datetime || a.travel_date)) || String(a.trip_id).localeCompare(String(b.trip_id)));
}

export function routeAnswer(question, alternatives) {
  const q = question.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (!alternatives.length) return 'Busca primero un trayecto con alternativas registradas.';
  if (/rapido|duracion|tiempo|barato|econom|tarifa|precio/.test(q)) {
    const fare = /barato|econom|tarifa|precio/.test(q), key = fare ? 'avgFare' : 'avgDuration';
    const best = sortAlternatives(alternatives.filter(item => item[key] != null), fare ? 'fare' : 'duration')[0];
    return best ? `${nameOf(best.mode)} tiene ${fare ? 'la menor tarifa media' : 'la menor duración media'} observada: ${fare ? 'USD ' : ''}${format(best[key])}${fare ? '' : ' min'}. El cálculo usa ${format(best[fare ? 'fareCount' : 'durationCount'], 0)} mediciones de este trayecto. Es un promedio histórico, no una cotización ni una predicción para tu próximo viaje.` : 'La selección no tiene mediciones suficientes para esa comparación.';
  }
  if (/estacion|oart|conexion/.test(q)) {
    const stations = [...new Set(alternatives.flatMap(item => item.stations))];
    return stations.length ? `Gold vincula este trayecto con ${stations.join(', ')}. El dato no especifica el orden de las paradas ni confirma una conexión disponible en este momento.` : 'No hay estaciones OART vinculadas a este trayecto en la fuente.';
  }
  if (/modo|alternativa|resumen|opcion/.test(q)) return `Hay ${alternatives.length} alternativas observadas: ${alternatives.map(item => nameOf(item.mode)).join(', ')}. En conjunto reúnen ${format(alternatives.reduce((n, item) => n + item.totalTrips, 0), 0)} registros históricos.`;
  return 'Puedes consultar qué alternativa tiene menor duración, cuál tiene menor tarifa, los modos disponibles o las conexiones OART del trayecto seleccionado.';
}

export const savedKey = item => JSON.stringify([item.origin, item.destination, item.mode || '']);
export function parseSaved(raw) {
  try { const values = JSON.parse(raw || '[]'); return Array.isArray(values) ? values.filter(item => item && typeof item.origin === 'string' && typeof item.destination === 'string' && (item.mode == null || typeof item.mode === 'string')).slice(0, 100) : []; }
  catch { return []; }
}
