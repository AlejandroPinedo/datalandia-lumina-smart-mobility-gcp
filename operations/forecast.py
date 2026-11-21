"""Evaluación temporal de demanda. Solo usa observaciones anteriores al objetivo."""
from collections import defaultdict
from datetime import date, timedelta
from statistics import mean, median
import math


def predict(history, target, model):
    past = {day: value for day, value in history.items() if day < target}
    if model == 'seasonal_naive_7':
        return past.get(target - timedelta(days=7))
    if model == 'same_weekday_28':
        values = [value for day, value in past.items() if 0 < (target-day).days <= 28 and day.weekday() == target.weekday()]
        return mean(values) if len(values) >= 2 else None
    if model == 'median_recent_7':
        values = [value for day, value in past.items() if 0 < (target-day).days <= 7]
        return median(values) if len(values) >= 5 else None
    raise ValueError('Modelo no reconocido')


def score(pairs):
    if not pairs: return {'n': 0, 'mae': None, 'wape_pct': None, 'bias': None}
    errors = [pred-actual for actual, pred in pairs]
    denominator = sum(actual for actual, _ in pairs)
    return {'n': len(pairs), 'mae': mean(abs(error) for error in errors),
            'wape_pct': sum(abs(error) for error in errors)/denominator*100 if denominator else None,
            'bias': mean(errors)}


def evaluate_series(observations, today=None):
    today = today or date.today()
    history = {date.fromisoformat(item['date']): float(item['trips']) for item in observations}
    if any(value < 0 or not math.isfinite(value) for value in history.values()):
        raise ValueError('Demanda no válida')
    days = sorted(history)
    if len(days) < 35:
        return {'status': 'insufficient_history', 'observed_days': len(days)}
    validation, test = days[-14:-7], days[-7:]
    models = ['seasonal_naive_7', 'same_weekday_28', 'median_recent_7']
    tuning = {}
    # Los mismos días se comparan para todos los candidatos; no se imputan ausencias.
    common = [day for day in validation if all(predict(history, day, model) is not None for model in models)]
    if len(common) < 5:
        return {'status': 'insufficient_validation', 'observed_days': len(days)}
    for model in models:
        tuning[model] = score([(history[day], predict(history, day, model)) for day in common])
    selected = min(models, key=lambda model: tuning[model]['mae'])
    holdout = [(history[day], predict(history, day, selected)) for day in test if predict(history, day, selected) is not None]
    baseline = [(history[day], predict(history, day, 'seasonal_naive_7')) for day in test if predict(history, day, 'seasonal_naive_7') is not None]
    next_day = days[-1] + timedelta(days=1)
    prediction = predict(history, next_day, selected)
    age = (today-days[-1]).days
    return {'status': 'evaluated' if 0 <= age <= 2 else 'historical_evaluation_only',
            'observed_days': len(days), 'first_observation': days[0].isoformat(),
            'last_observation': days[-1].isoformat(), 'data_age_days': age,
            'training_end': days[-15].isoformat(), 'validation_start': validation[0].isoformat(),
            'validation_end': validation[-1].isoformat(), 'test_start': test[0].isoformat(),
            'test_end': test[-1].isoformat(), 'selected_model': selected,
            'validation_metrics': tuning, 'test_metrics': score(holdout),
            'seasonal_baseline_test_metrics': score(baseline),
            'forecast_date': next_day.isoformat(), 'forecast_trips': prediction,
            'operational_forecast_for_tomorrow': prediction if age == 0 else None,
            'resource_assignment': None,
            'resource_reason': 'No hay inventario operativo de flota, turnos y conductores',
            'missing_days_assumed_zero': False,
            'method': 'Selección en validación de 7 días; evaluación separada de 7 días con origen rodante'}


def evaluate_demand(daily_rows, today=None):
    grouped = defaultdict(list)
    for row in daily_rows:
        if row['transport_mode'] == 'PERSONAL_CAR_BENCHMARK': continue
        key = (row['zone_id'], row['transport_mode'])
        grouped[key].append({'date': str(row['date']), 'trips': row['trips']})
    return [{'zone_id': zone, 'transport_mode': mode, **evaluate_series(values, today)}
            for (zone, mode), values in sorted(grouped.items())]
