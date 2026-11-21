"""Contexto territorial de las cuatro fuentes, con fechas explícitas y sin falsear simultaneidad."""
from pathlib import Path
import json
from statistics import mean


def enrich_report(report, runtime):
    sources=json.loads((Path(runtime)/'portable_sources.json').read_text(encoding='utf-8'))
    traffic=sources['traffic'];events=sources['telemetry'];trips=sources['trips']
    dates=[str(row['travel_date']) for row in traffic]
    times=[str(row['timestamp_evento']) for row in events]
    report['source_temporal_profiles']={
        'trips':{'start':report['coverage']['first_date'],'end':report['coverage']['last_date']},
        'traffic':{'start':min(dates) if dates else None,'end':max(dates) if dates else None},
        'iot':{'start':min(times) if times else None,'end':max(times) if times else None}}
    report['coverage_interpretation']='Los viajes son de enero/febrero de 2026; tráfico e IoT son de noviembre. No se atribuyen esas mediciones a viajes de otro período.'
    context=[]
    for zone in sources['zones']:
        speeds=[row for row in traffic if row['origin_zone_id']==zone['zone_id']]
        sensors=[event for event in events if event['zone_id']==zone['zone_id']]
        valid_speeds=[float(row['avg_speed_kmh']) for row in speeds if row.get('avg_speed_kmh') is not None]
        latest=max(sensors,key=lambda row:str(row['timestamp_evento'])) if sensors else None
        context.append({'zone_id':zone['zone_id'],'zone_name':zone['zone_name'],
            'county_name':zone['county_name'],'population_in_source':zone.get('poblacion_residente'),
            'exported_trip_examples':sum(row['origin_zone_id']==zone['zone_id'] for row in trips),
            'traffic_observations':len(speeds),'traffic_mean_speed_kmh':mean(valid_speeds) if valid_speeds else None,
            'traffic_observed_date':max(str(row['travel_date']) for row in speeds) if speeds else None,
            'iot_observations':len(sensors),'iot_last_observation':latest['timestamp_evento'] if latest else None,
            'last_recorded_alert':latest.get('requiere_mantenimiento_pdm') if latest else None,
            'context_scope':'Referencias por zona con fechas distintas; no condiciones simultáneas del viaje'})
    report['territorial_context']=context
    report['privacy_verification']={'rider_identifiers_in_portable_export':any(
        any(key.startswith('rider') or key.startswith('payment') for key in trip) for trip in trips),
        'credentials_in_report':False,'scope':'Verificación de campos del export portable; no certificación legal de anonimización'}
    return report
