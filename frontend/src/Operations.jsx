import React, { useEffect, useState } from 'react';
import { Check, Database, Download, Info, RefreshCw } from 'lucide-react';
import { format, nameOf } from './data.js';

function DataTable({ headings, rows }) {
  return <div className="table-scroll"><table><thead><tr>{headings.map(h => <th key={h}>{h}</th>)}</tr></thead><tbody>{rows.map((row,i) => <tr key={i}>{row.map((value,j) => <td key={j} style={{whiteSpace:'normal',textAlign:j===0?'left':'right'}}>{value??'N/D'}</td>)}</tr>)}</tbody></table></div>;
}

export default function Operations() {
  const [data,setData]=useState(null),[error,setError]=useState(''),[loading,setLoading]=useState(true);
  const [zone,setZone]=useState('');
  async function refresh() {
    setLoading(true);setError('');
    try {
      const response=await fetch('/api/v1/operations',{cache:'no-store'}),result=await response.json();
      if (!response.ok || result.source_data_origin!=='existing_project_data') throw new Error(result.reason||'No hay una validación identificada disponible.');
      setData(result);
    } catch (failure) {setData(null);setError(failure.message);}
    finally {setLoading(false);}
  }
  useEffect(()=>{refresh();},[]);
  const download=()=>{
    const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));
    const a=document.createElement('a');a.href=url;a.download='lumina-validacion.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
  };
  if (!data) return <section className="panel"><h2>{loading?'Cargando la validación…':'Validación no disponible'}</h2><p className="inline-note">{error}</p><button className="button" onClick={refresh} disabled={loading}>Consultar resultados</button></section>;
  const coverage=data.coverage||{},forecasts=(data.forecasts||[]).filter(f=>!zone||f.zone_id===zone);
  const zones=data.territorial_context||[],zoneNames=new Map(zones.map(z=>[z.zone_id,z.zone_name]));
  const successful=data.status==='validated_olap_pipeline';
  return <div className="operations-view"><section className="panel"><div className="panel-heading"><div><h2>{successful?'Pipeline completo validado':'Validación en curso o incompleta'}</h2><p>Bronze existente → Silver aislado → calidad → Gold observado → API → esta aplicación</p></div><div className="actions"><button className="button compact" onClick={refresh} disabled={loading}><RefreshCw size={14}/>Actualizar</button><button className="button compact" onClick={download}><Download size={14}/>Evidencia JSON</button></div></div><dl className="metadata-list"><div><dt>Dataset de la validación</dt><dd>{data.target_dataset}</dd></div><div><dt>Fuentes históricas</dt><dd>{data.historical_sources_modified?'Modificadas':'Conservadas; no se reemplazaron las tablas originales'}</dd></div><div><dt>Origen de los registros</dt><dd>Tablas existentes de Lumina; sin generadores</dd></div></dl>{data.error&&<p className="notice">{data.error}</p>}</section>
    <section className="stats"><div><span className="stat-label">Viajes en el Gold validado</span><strong>{format(coverage.total_rows,0)}</strong><span className="stat-note">Fuente de esta validación; independiente del Gold histórico</span></div><div><span className="stat-label">Series de demanda evaluadas</span><strong>{format(data.forecasts?.length,0)}</strong><span className="stat-note">Validación temporal y prueba separadas</span></div><div><span className="stat-label">Eventos procesados</span><strong>{format(data.streaming?.input_events,0)}</strong><span className="stat-note">Reproducción histórica con timestamps originales</span></div></section>
    <section className="panel"><div className="panel-heading"><div><h2>Resultados OLAP del Gold nuevo</h2><p>Estos indicadores provienen del pipeline ejecutado en el dataset aislado.</p></div><Database size={19}/></div><DataTable headings={['Transporte','Registros','Km','USD de tarifa','Tarifa/km USD','Duración media min']} rows={(data.olap||[]).map(r=>[nameOf(r.transport_mode),format(r.trips,0),format(r.distance_km),format(r.fare_usd),format(r.fare_per_km_usd,3),format(r.avg_duration_min)])}/><p className="inline-note"><Info size={14}/>La tarifa por km no representa el costo de operación. La referencia del auto usa registros del mismo origen, destino, fecha y hora; no demuestra causalidad.</p></section>
    <section className="panel"><div className="panel-heading"><div><h2>Calidad y cobertura</h2><p>Las reglas evalúan los datos originales antes de publicar Gold.</p></div></div><DataTable headings={['Regla','Evaluados','Válidos','Cumplimiento','SLA']} rows={(data.quality||[]).map(r=>[r.dimension_calidad,format(r.registros_evaluados,0),format(r.registros_validos,0),format(r.pct_cumplimiento,4)+' %',r.estado_gobernanza])}/><div className="comparison-note"><Info size={18}/><div><strong>Cobertura temporal comprobada</strong><p>Censo: {format(coverage.census_matches,0)} coincidencias. Tráfico: {format(coverage.traffic_matches,0)}. IoT: {format(coverage.iot_matches,0)}.</p><p>{data.coverage_interpretation}</p></div></div><DataTable headings={['Fuente','Inicio observado','Fin observado']} rows={Object.entries(data.source_temporal_profiles||{}).map(([key,value])=>[key,value.start,value.end])}/></section>
    <section className="panel"><div className="panel-heading"><div><h2>Contexto territorial multifuente</h2><p>Cruce por zona con fechas explícitas. Es contexto histórico; no condiciones simultáneas de cada viaje.</p></div></div><DataTable headings={['Zona / censo','Población en fuente','Ejemplos de viaje','Registros tráfico','Velocidad de referencia','Eventos IoT']} rows={zones.map(z=>[z.zone_name,format(z.population_in_source,0),format(z.exported_trip_examples,0),format(z.traffic_observations,0),format(z.traffic_mean_speed_kmh)+' km/h',format(z.iot_observations,0)])}/></section>
    <section className="panel"><div className="panel-heading"><div><h2>Pronóstico de demanda: evaluación histórica</h2><p>Modelos elegidos con 7 días de validación y evaluados en otros 7 días. Los días ausentes no se imputan como cero.</p></div><label className="field"><span>Zona</span><select value={zone} onChange={e=>setZone(e.target.value)}><option value="">Todas</option>{zones.map(z=><option key={z.zone_id} value={z.zone_id}>{z.zone_name}</option>)}</select></label></div><DataTable headings={['Zona / modo','Modelo elegido','MAE prueba','WAPE prueba','Fecha objetivo','Pronóstico histórico']} rows={forecasts.map(f=>[(zoneNames.get(f.zone_id)||f.zone_id)+' / '+nameOf(f.transport_mode),f.selected_model||f.status,format(f.test_metrics?.mae),format(f.test_metrics?.wape_pct)+' %',f.forecast_date,format(f.forecast_trips,0)])}/><p className="inline-note"><Info size={14}/>El histórico termina en febrero de 2026. No se publica este pronóstico como demanda actual de mañana ni se inventa una cantidad de vehículos disponibles.</p></section>
    <section className="panel"><div className="panel-heading"><div><h2>Streaming, reentrega y recuperación</h2><p>Procesamiento incremental durable sobre los eventos existentes.</p></div></div><DataTable headings={['Comprobación','Resultado']} rows={[
      ['Modo',data.streaming?.mode],['Eventos aceptados',format(data.streaming?.outcomes?.accepted,0)],
      ['Duplicados detectados en reentrega',format(data.streaming?.redelivery_outcomes?.duplicate,0)],
      ['Idempotencia',data.streaming?.idempotency],['Respaldo y restauración',data.streaming?.recovery],
      ['Integridad SQLite',data.streaming?.integrity],['p95 de procesamiento local',format(data.streaming?.latency_ms_p95)+' ms'],
      ['Cruce en motor abierto',data.portable_join?.status+' · '+format(data.portable_join?.output_records,0)+' registros'],
    ]}/><p className="inline-note"><Info size={14}/>La reproducción conserva las fechas originales. Estas mediciones no acreditan latencia de Pub/Sub/Dataflow ni carga metropolitana.</p></section>
    <section className="panel"><div className="panel-heading"><div><h2>Diagnóstico de mantenimiento</h2><p>Observaciones y alertas tal como aparecen en la fuente, con su frescura.</p></div></div><DataTable headings={['Sensor','Muestras','Vibración mm/s','Temperatura °C','Alerta registrada','Estado']} rows={(data.maintenance||[]).map(sensor=>[sensor.node_id,format(sensor.observations,0),format(sensor.latest_vibration_mms),format(sensor.latest_temperature_c),sensor.recorded_alert==null?'N/D':sensor.recorded_alert?'Sí':'No',sensor.status==='stale_sensor'?'Sensor desactualizado':sensor.status])}/><p className="inline-note"><Info size={14}/>Hay una observación por sensor y no hay fallas confirmadas. La lógica detecta falta de historial y frescura; no inventa probabilidades de falla.</p></section>
    <section className="panel"><div className="panel-heading"><div><h2>Ejecución y trazabilidad</h2><p>Jobs reales de BigQuery; las etapas reutilizadas conservan su identificación.</p></div></div><DataTable headings={['Etapa','Resultado','Duración s','Job ID']} rows={(data.stages||[]).map(stage=>[stage.name,stage.status,format(stage.elapsed_seconds),stage.job_id])}/></section>
  </div>;
}
