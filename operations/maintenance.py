"""Diagnóstico temporal de sensores; no transforma alertas en etiquetas de fallas."""
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean


def assess_sensors(events, now=None):
    now = now or datetime.now(timezone.utc)
    grouped = defaultdict(list)
    for event in events:
        grouped[event['node_id']].append(event)
    result = []
    for node_id, values in sorted(grouped.items()):
        observations = {str(e['timestamp_evento']): e for e in values}
        ordered = sorted(observations.values(), key=lambda e: str(e['timestamp_evento']))
        latest = ordered[-1]
        times = [datetime.fromisoformat(str(e['timestamp_evento']).replace('Z', '+00:00')) for e in ordered]
        span = (max(times)-min(times)).total_seconds()/3600
        age = (now-max(times)).total_seconds()/3600
        slopes = {}
        if len(times) >= 3 and span >= 1:
            hours = [(stamp-min(times)).total_seconds()/3600 for stamp in times]
            denominator = sum((value-mean(hours))**2 for value in hours)
            for metric in ['vibration_mms', 'temperature_c']:
                ys = [float(e[metric]) for e in ordered]
                slopes[metric] = sum((x-mean(hours))*(y-mean(ys)) for x,y in zip(hours,ys))/denominator if denominator else None
        result.append({'node_id': node_id, 'station_id': latest['station_id'], 'zone_id': latest['zone_id'],
                       'observations': len(ordered), 'span_hours': span, 'age_hours': age,
                       'last_observation': max(times).isoformat(), 'latest_vibration_mms': latest['vibration_mms'],
                       'latest_temperature_c': latest['temperature_c'],
                       'recorded_alert': latest.get('requiere_mantenimiento_pdm'),
                       'status': 'stale_sensor' if age > 1 else 'trend_available' if slopes else 'insufficient_history',
                       'slopes_per_hour': slopes, 'failure_probability': None,
                       'failure_model_status': 'requires_observed_failure_labels_and_temporal_validation',
                       'engineering_approved_limits': None,
                       'action': 'Revisar frescura y disponibilidad del sensor' if age > 1 else 'Inspeccionar alerta registrada' if latest.get('requiere_mantenimiento_pdm') is True else 'Reunir historial por activo'})
    return result
