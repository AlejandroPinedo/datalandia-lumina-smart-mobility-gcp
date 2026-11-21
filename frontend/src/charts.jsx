import React, { useId, useState } from 'react';
import { dateLabel, format, short, nameOf, modeColors } from './data.js';

export function Empty({ children = 'No hay registros con estos filtros.' }) {
  return <div className="empty-state"><span className="empty-symbol">∅</span><p>{children}</p></div>;
}

export function ModeBars({ rows, metric = 'total_trips', onSelect, allModes }) {
  const max = Math.max(0, ...rows.map(row => Number(row[metric]) || 0));
  if (!rows.length) return <Empty />;
  return <div className="mode-bars">{rows.map(row => {
    const index = allModes.indexOf(row.transport_mode);
    return <button className="mode-bar" key={row.transport_mode} onClick={() => onSelect(row)}>
      <span className="bar-heading"><span><i style={{ background: modeColors[index % modeColors.length] }} />{nameOf(row.transport_mode)}</span><strong>{format(row[metric], metric === 'total_trips' ? 0 : 2)}</strong></span>
      <span className="bar-track"><span style={{ width: `${max ? Math.max(0, row[metric] || 0) / max * 100 : 0}%`, background: modeColors[index % modeColors.length] }} /></span>
    </button>;
  })}</div>;
}

export function TrendChart({ rows, onSelect }) {
  const [active, setActive] = useState(null), id = useId().replaceAll(':', '');
  if (!rows.length) return <Empty />;
  const width = 760, height = 238, left = 47, right = 20, top = 24, bottom = 36;
  const max = Math.max(1, ...rows.map(row => row.total_trips)) * 1.12;
  const x = i => left + i / Math.max(1, rows.length - 1) * (width - left - right);
  const y = value => height - bottom - value / max * (height - top - bottom);
  const points = rows.map((row, i) => `${x(i)},${y(row.total_trips)}`).join(' ');
  const current = active == null ? null : rows[active];
  const ticks = [...new Set([0, Math.floor((rows.length - 1) / 3), Math.floor((rows.length - 1) * 2 / 3), rows.length - 1])];
  return <div className="trend-chart"><div className="chart-caption" aria-live="polite">{current ? <><strong>{format(current.total_trips, 0)} registros</strong><span>{dateLabel(current.travel_date, true)}</span></> : <><strong>Registros diarios</strong><span>Selecciona un día para explorar</span></>}</div>
    <svg viewBox={`0 0 ${width} ${height}`} role="group" aria-label="Evolución diaria de los registros">
      <defs><linearGradient id={id} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#24746b" stopOpacity=".11" /><stop offset="100%" stopColor="#24746b" stopOpacity="0" /></linearGradient></defs>
      {[0, .25, .5, .75, 1].map(n => <g key={n}><line x1={left} x2={width - right} y1={y(n * max)} y2={y(n * max)} stroke="#e9edf1" strokeDasharray={n ? '3 4' : undefined} /><text x={left - 10} y={y(n * max) + 4} textAnchor="end">{short(n * max)}</text></g>)}
      <polygon points={`${left},${height - bottom} ${points} ${x(rows.length - 1)},${height - bottom}`} fill={`url(#${id})`} />
      <polyline points={points} fill="none" stroke="#24746b" strokeWidth="2.4" strokeLinejoin="round" />
      {ticks.map(i => <text key={i} x={x(i)} y={height - 11} textAnchor={i === 0 ? 'start' : i === rows.length - 1 ? 'end' : 'middle'}>{dateLabel(rows[i].travel_date)}</text>)}
      {rows.map((row, i) => <g key={String(row.travel_date)}><circle cx={x(i)} cy={y(row.total_trips)} r={active === i ? 4 : rows.length === 1 ? 4 : 0} fill="#24746b" stroke="white" strokeWidth="2" /><rect x={x(i) - Math.max(5, (width - left - right) / rows.length / 2)} y={top} width={Math.max(10, (width - left - right) / rows.length)} height={height - top - bottom} fill="transparent" tabIndex="0" role="button" aria-label={`${dateLabel(row.travel_date)}: ${format(row.total_trips, 0)} registros. Filtrar por esta fecha.`} onMouseEnter={() => setActive(i)} onFocus={() => setActive(i)} onClick={() => onSelect(row.travel_date)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect(row.travel_date); } }}><title>{dateLabel(row.travel_date)} · {format(row.total_trips, 0)} registros</title></rect></g>)}
    </svg>
  </div>;
}

export function HourChart({ rows, onSelect }) {
  if (!rows.length) return <Empty />;
  const max = Math.max(1, ...rows.map(row => row.total_trips));
  return <div className="hour-chart"><div className="hour-bars">{rows.map(row => <button key={row.hod ?? 'null'} className="hour-column" title={`${row.hod ?? 'Sin hora'} h · ${format(row.total_trips, 0)} registros`} aria-label={`${row.hod ?? 'Sin dato'} horas: ${format(row.total_trips, 0)} registros`} onClick={() => row.hod != null && onSelect(String(row.hod))}><span className="hour-fill" style={{ height: `${row.total_trips / max * 100}%` }} /><span className="hour-label">{row.hod != null && Number(row.hod) % 4 === 0 ? `${row.hod}h` : ''}</span></button>)}</div><div className="chart-bottom"><span>Hora de salida</span><span>Máx. {short(max)} registros</span></div></div>;
}

export function CountyBars({ rows, onSelect }) {
  if (!rows.length) return <Empty />;
  const total = rows.reduce((n, row) => n + row.total_trips, 0);
  const max = Math.max(1, ...rows.map(row => row.total_trips));
  return <div className="county-list">{rows.map(row => <button key={row.county_name ?? 'null'} className="county-row" title={`${row.county_name ?? 'Sin dato'} · ${format(row.total_trips, 0)} registros`} onClick={() => row.county_name != null && onSelect(row.county_name)}><span>{(row.county_name || 'Sin condado').replace(/^Condado /, '')}</span><span className="county-track"><i style={{ width: `${row.total_trips / max * 100}%` }} /></span><strong>{format(total ? row.total_trips / total * 100 : 0, 1)} %</strong></button>)}</div>;
}
