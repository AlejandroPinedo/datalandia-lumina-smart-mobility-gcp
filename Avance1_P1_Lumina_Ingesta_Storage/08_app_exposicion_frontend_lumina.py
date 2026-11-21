#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""API académica Flask o HTTP estándar.
Expone caché, simulación con supuestos, consultas de lectura y baselines operativos.
La ausencia de datos cloud se informa sin inventar resultados. No despacha vehículos ni procesa pagos."""

from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import html as html_lib
import logging
import math
import os
from pathlib import Path
import secrets
import time
from typing import Any, Dict, Optional

try:
    from flask import Flask, Response, g, jsonify, render_template, request
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False
    Flask = None  # type: ignore

try:
    from google.cloud import bigquery
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False
    bigquery = None  # type: ignore

import config
from olap_service import load_olap_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lumina_smart_mobility_api")

ALLOWED_ZONES = {z["zone_id"]: z for z in config.DATALANDIA_ZONES}
ALLOWED_PERIODS = {"PEAK_MORNING", "PEAK_EVENING", "OFF_PEAK"}


def get_app_secret() -> str:
    env_secret = os.getenv("FLASK_SECRET_KEY")
    if env_secret:
        return env_secret
    return secrets.token_hex(32)


def default_calibrated_artifacts() -> Dict[str, Any]:
    """
    Datos calibrados de respaldo basados en los 4 datasets de DataLandia (Lumina)
    para garantizar respuesta sub-10ms incluso antes de correr los jobs en GCP.
    """
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gcp_project_id": config.GCP_PROJECT_ID,
        "engine_mode": "DEMO SINTETICA: indicadores ilustrativos, sin consulta en vivo",
        "data_origin": "synthetic_demo",
        "headline_source": "synthetic_demo",
        "datasets": {
            "bronze": config.BQ_DATASET_BRONZE,
            "silver": config.BQ_DATASET_SILVER,
            "gold": config.BQ_DATASET_GOLD,
            "audit": config.BQ_DATASET_AUDIT,
        },
        "headline_kpis": {
            "poblacion_total": 9250000,
            "condados_unificados": 8,
            "agencias_integradas": 12,
            "cobertura_oart_8km_pct": 89.0,
            "velocidad_hora_punta_auto_kmh": 23.6,
            "velocidad_lumina_oart_kmh": 41.8,
            "costo_km_lumina_usd": 0.42,
            "costo_km_auto_particular_usd": 1.18,
            "ahorro_costo_km_pct": 64.4,
            "sla_calidad_knowledge_catalog_pct": 99.6,
        },
        "modes_benchmark": [
            {
                "transport_mode": "LUMINA_BIKE_AND_OART",
                "label": "Bicicleta Primera Milla + Tren OART",
                "total_viajes": 68,
                "avg_cost_per_km_usd": 0.378,
                "avg_speed_kmh": 38.4,
                "avg_duration_min": 24.2,
                "avg_occupancy_pax": 3.8,
                "savings_vs_car_pct": 68.0,
            },
            {
                "transport_mode": "LUMINA_BUS_RAPID",
                "label": "Bus Rápido Carril Exclusivo Lumina",
                "total_viajes": 84,
                "avg_cost_per_km_usd": 0.452,
                "avg_speed_kmh": 31.2,
                "avg_duration_min": 29.5,
                "avg_occupancy_pax": 4.4,
                "savings_vs_car_pct": 61.7,
            },
            {
                "transport_mode": "LUMINA_MULTIMODAL_OART",
                "label": "Minivan Eléctrica + Red Ferroviaria OART",
                "total_viajes": 228,
                "avg_cost_per_km_usd": 0.518,
                "avg_speed_kmh": 42.1,
                "avg_duration_min": 21.8,
                "avg_occupancy_pax": 4.2,
                "savings_vs_car_pct": 56.1,
            },
            {
                "transport_mode": "LUMINA_EV_MINIVAN_POOL",
                "label": "Minivan Eléctrica Compartida (Ride-Pooling)",
                "total_viajes": 132,
                "avg_cost_per_km_usd": 0.675,
                "avg_speed_kmh": 33.0,
                "avg_duration_min": 27.6,
                "avg_occupancy_pax": 3.9,
                "savings_vs_car_pct": 42.8,
            },
            {
                "transport_mode": "PERSONAL_CAR_BENCHMARK",
                "label": "Vehículo Particular (Atascado en Hora Punta 24 km/h)",
                "total_viajes": 88,
                "avg_cost_per_km_usd": 1.385,
                "avg_speed_kmh": 23.6,
                "avg_duration_min": 41.4,
                "avg_occupancy_pax": 1.0,
                "savings_vs_car_pct": 0.0,
            },
        ],
        "counties_kpis": [
            {
                "county_id": "CNT-04",
                "county_name": "Condado Valle Joven",
                "zone_id": "ZDL-004",
                "zone_name": "Corredor Universitario Sur",
                "nearest_oart_station_id": "OART-ST-15",
                "zone_type": "SUBURBIO_JOVEN_ESTUDIANTIL",
                "poblacion_residente": 1320000,
                "pct_adulto_mayor_65": 10.5,
                "pct_jovenes_18_34": 44.2,
                "pct_menos_8km_estacion_oart": 91.0,
                "indice_prioridad_primera_milla": 96.5,
                "costo_km_lumina_usd": 0.41,
                "velocidad_media_lumina_kmh": 39.2,
                "velocidad_calle_uber_24kmh": 23.4,
                "ocupacion_oart_pct": 78.0,
                "alertas_mantenimiento_pdm": 1,
                "plan_diario_recursos_recomendado": "DESPLEGAR BIKES + BUS RAPIDO UNIVERSITARIO -> OART",
            },
            {
                "county_id": "CNT-07",
                "county_name": "Condado Comarca del Lago",
                "zone_id": "ZDL-007",
                "zone_name": "Residencial Lago Sereno (Retiro)",
                "nearest_oart_station_id": "OART-ST-33",
                "zone_type": "SUBURBIO_ADULTO_MAYOR",
                "poblacion_residente": 920000,
                "pct_adulto_mayor_65": 33.8,
                "pct_jovenes_18_34": 14.5,
                "pct_menos_8km_estacion_oart": 84.0,
                "indice_prioridad_primera_milla": 95.5,
                "costo_km_lumina_usd": 0.46,
                "velocidad_media_lumina_kmh": 37.5,
                "velocidad_calle_uber_24kmh": 24.1,
                "ocupacion_oart_pct": 62.0,
                "alertas_mantenimiento_pdm": 0,
                "plan_diario_recursos_recomendado": "DESPLEGAR MINIVANS ACCESIBLES (05:00 AM) -> CONEXION OART",
            },
            {
                "county_id": "CNT-03",
                "county_name": "Condado Altos del Este",
                "zone_id": "ZDL-003",
                "zone_name": "Suburbio Residencial Colinas Verdes",
                "nearest_oart_station_id": "OART-ST-09",
                "zone_type": "SUBURBIO_ADULTO_MAYOR",
                "poblacion_residente": 980000,
                "pct_adulto_mayor_65": 29.5,
                "pct_jovenes_18_34": 16.0,
                "pct_menos_8km_estacion_oart": 87.0,
                "indice_prioridad_primera_milla": 94.0,
                "costo_km_lumina_usd": 0.45,
                "velocidad_media_lumina_kmh": 38.0,
                "velocidad_calle_uber_24kmh": 23.8,
                "ocupacion_oart_pct": 64.0,
                "alertas_mantenimiento_pdm": 1,
                "plan_diario_recursos_recomendado": "DESPLEGAR MINIVANS ACCESIBLES (05:00 AM) -> CONEXION OART",
            },
            {
                "county_id": "CNT-01",
                "county_name": "Condado Central (Core)",
                "zone_id": "ZDL-001",
                "zone_name": "Centro Financiero Metropolitano",
                "nearest_oart_station_id": "OART-ST-01",
                "zone_type": "URBANO_DENSO",
                "poblacion_residente": 1450000,
                "pct_adulto_mayor_65": 14.2,
                "pct_jovenes_18_34": 34.5,
                "pct_menos_8km_estacion_oart": 96.5,
                "indice_prioridad_primera_milla": 88.0,
                "costo_km_lumina_usd": 0.42,
                "velocidad_media_lumina_kmh": 42.4,
                "velocidad_calle_uber_24kmh": 22.1,
                "ocupacion_oart_pct": 85.0,
                "alertas_mantenimiento_pdm": 2,
                "plan_diario_recursos_recomendado": "FRECUENCIA ESTANDAR TREN OART + RIDE-POOLING",
            },
            {
                "county_id": "CNT-06",
                "county_name": "Condado Ribera Industrial",
                "zone_id": "ZDL-006",
                "zone_name": "Parque Industrial y Logístico",
                "nearest_oart_station_id": "OART-ST-28",
                "zone_type": "POLO_LABORAL",
                "poblacion_residente": 1190000,
                "pct_adulto_mayor_65": 13.5,
                "pct_jovenes_18_34": 27.5,
                "pct_menos_8km_estacion_oart": 85.0,
                "indice_prioridad_primera_milla": 86.0,
                "costo_km_lumina_usd": 0.44,
                "velocidad_media_lumina_kmh": 39.8,
                "velocidad_calle_uber_24kmh": 23.5,
                "ocupacion_oart_pct": 74.0,
                "alertas_mantenimiento_pdm": 1,
                "plan_diario_recursos_recomendado": "FRECUENCIA ESTANDAR TREN OART + RIDE-POOLING",
            },
            {
                "county_id": "CNT-02",
                "county_name": "Condado Silicon Norte",
                "zone_id": "ZDL-002",
                "zone_name": "Distrito Tecnológico Norte",
                "nearest_oart_station_id": "OART-ST-04",
                "zone_type": "POLO_LABORAL",
                "poblacion_residente": 1280000,
                "pct_adulto_mayor_65": 11.8,
                "pct_jovenes_18_34": 38.0,
                "pct_menos_8km_estacion_oart": 92.0,
                "indice_prioridad_primera_milla": 84.5,
                "costo_km_lumina_usd": 0.43,
                "velocidad_media_lumina_kmh": 41.2,
                "velocidad_calle_uber_24kmh": 23.9,
                "ocupacion_oart_pct": 81.0,
                "alertas_mantenimiento_pdm": 0,
                "plan_diario_recursos_recomendado": "DESPLEGAR BIKES + BUS RAPIDO UNIVERSITARIO -> OART",
            },
            {
                "county_id": "CNT-05",
                "county_name": "Condado Bahía Oeste",
                "zone_id": "ZDL-005",
                "zone_name": "Puerto Multimodal y Ferry Bahía",
                "nearest_oart_station_id": "OART-ST-21",
                "zone_type": "NODO_PORTUARIO",
                "poblacion_residente": 1050000,
                "pct_adulto_mayor_65": 16.0,
                "pct_jovenes_18_34": 29.0,
                "pct_menos_8km_estacion_oart": 94.0,
                "indice_prioridad_primera_milla": 81.0,
                "costo_km_lumina_usd": 0.47,
                "velocidad_media_lumina_kmh": 36.8,
                "velocidad_calle_uber_24kmh": 24.4,
                "ocupacion_oart_pct": 68.0,
                "alertas_mantenimiento_pdm": 0,
                "plan_diario_recursos_recomendado": "FRECUENCIA ESTANDAR TREN OART + RIDE-POOLING",
            },
            {
                "county_id": "CNT-08",
                "county_name": "Condado Mirador Sur",
                "zone_id": "ZDL-008",
                "zone_name": "Mirador Turístico y Globos Aerostáticos",
                "nearest_oart_station_id": "OART-ST-40",
                "zone_type": "ECOTURISMO_Y_PERIFERIA",
                "poblacion_residente": 1060000,
                "pct_adulto_mayor_65": 19.4,
                "pct_jovenes_18_34": 25.0,
                "pct_menos_8km_estacion_oart": 82.5,
                "indice_prioridad_primera_milla": 79.0,
                "costo_km_lumina_usd": 0.49,
                "velocidad_media_lumina_kmh": 35.4,
                "velocidad_calle_uber_24kmh": 24.8,
                "ocupacion_oart_pct": 59.0,
                "alertas_mantenimiento_pdm": 0,
                "plan_diario_recursos_recomendado": "FRECUENCIA ESTANDAR TREN OART + RIDE-POOLING",
            },
        ],
        "recent_iot_telemetry": [
            {
                "event_id": "AOT-9F2A81C04B",
                "node_id": "NODE-ZDL-001-412",
                "zone_id": "ZDL-001",
                "station_id": "OART-ST-01",
                "asset_type": "OART_RAIL_SWITCH",
                "vibration_mms": 11.4,
                "temperature_c": 88.5,
                "sound_db": 94.2,
                "passenger_load_pct": 84,
                "sensor_status": "CRITICAL_PDM",
                "requiere_mantenimiento_pdm": True,
            },
            {
                "event_id": "AOT-3C7E19B20D",
                "node_id": "NODE-ZDL-003-208",
                "zone_id": "ZDL-003",
                "station_id": "OART-ST-09",
                "asset_type": "OART_TRAIN_BOGIE",
                "vibration_mms": 8.9,
                "temperature_c": 82.1,
                "sound_db": 89.5,
                "passenger_load_pct": 68,
                "sensor_status": "CRITICAL_PDM",
                "requiere_mantenimiento_pdm": True,
            },
            {
                "event_id": "AOT-7D4B55E11A",
                "node_id": "NODE-ZDL-004-719",
                "zone_id": "ZDL-004",
                "station_id": "OART-ST-15",
                "asset_type": "LUMINA_EV_MINIVAN_TELEMATICS",
                "vibration_mms": 3.2,
                "temperature_c": 41.0,
                "sound_db": 64.0,
                "passenger_load_pct": 75,
                "sensor_status": "NORMAL",
                "requiere_mantenimiento_pdm": False,
            },
            {
                "event_id": "AOT-5A1C90F32E",
                "node_id": "NODE-ZDL-002-115",
                "zone_id": "ZDL-002",
                "station_id": "OART-ST-04",
                "asset_type": "URBAN_AOT_ENV_NODE",
                "vibration_mms": 2.4,
                "temperature_c": 26.8,
                "sound_db": 61.5,
                "passenger_load_pct": 79,
                "sensor_status": "NORMAL",
                "requiere_mantenimiento_pdm": False,
            },
            {
                "event_id": "AOT-8E6D12A99C",
                "node_id": "NODE-ZDL-007-504",
                "zone_id": "ZDL-007",
                "station_id": "OART-ST-33",
                "asset_type": "OART_RAIL_SWITCH",
                "vibration_mms": 6.1,
                "temperature_c": 69.4,
                "sound_db": 77.0,
                "passenger_load_pct": 58,
                "sensor_status": "WARNING_PDM",
                "requiere_mantenimiento_pdm": False,
            },
        ],
        "dlq_summary": [
            {
                "origen_fuente": "IOT_AOT_STREAM",
                "motivo_error": "FALLO_SENSOR_VIBRACION_FUERA_RANGO (-45.0 mm/s)",
                "total_aislados": 3,
            },
            {
                "origen_fuente": "IOT_AOT_STREAM",
                "motivo_error": "FALLO_TERMOPAR_OART_FUERA_ESCALA (999.9 C)",
                "total_aislados": 2,
            },
            {
                "origen_fuente": "IOT_AOT_STREAM",
                "motivo_error": "SENSOR_SIN_NODE_ID",
                "total_aislados": 1,
            },
        ],
    }


def load_gold_dashboard_data() -> Dict[str, Any]:
    base = default_calibrated_artifacts()
    if config.CACHE_FILE.exists():
        try:
            with open(config.CACHE_FILE, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if "counties_kpis" in cached:
                base["counties_kpis"] = cached["counties_kpis"]
            if "modes_benchmark" in cached:
                base["modes_benchmark"] = cached["modes_benchmark"]
            if "recent_iot_telemetry" in cached:
                base["recent_iot_telemetry"] = cached["recent_iot_telemetry"]
            if "dlq_summary" in cached:
                base["dlq_summary"] = cached["dlq_summary"]
            base["generated_at"] = cached.get("generated_at", base["generated_at"])
            base["engine_mode"] = "CACHE EXPORTADA: no es telemetría en vivo; cabecera ilustrativa"
            base["data_origin"] = "exported_cache"
            base["cache_provenance"] = cached.get("provenance", "legacy_unverified")
        except Exception as exc:
            logger.warning("Usando artefactos base calibrados: %s", exc)
    return base


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(max(4.5, r * c * 1.32), 2)


def compute_route_simulation(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula en tiempo real la comparativa Lumina Multimodal (OART + Primera Milla)
    vs. Auto Particular (24 km/h), consultando BigQuery con ScalarQueryParameter si está activo.
    """
    orig_id = str(body.get("origin_zone_id", "ZDL-003")).strip()
    dest_id = str(body.get("dest_zone_id", "ZDL-001")).strip()
    time_period = str(body.get("time_period", "PEAK_MORNING")).strip()

    if orig_id not in ALLOWED_ZONES:
        orig_id = "ZDL-003"
    if dest_id not in ALLOWED_ZONES or dest_id == orig_id:
        dest_id = "ZDL-001" if orig_id != "ZDL-001" else "ZDL-002"
    if time_period not in ALLOWED_PERIODS:
        time_period = "PEAK_MORNING"

    try:
        passengers = max(1, min(6, int(body.get("passengers", 2))))
    except (TypeError, ValueError):
        passengers = 2

    orig_zone = ALLOWED_ZONES[orig_id]
    dest_zone = ALLOWED_ZONES[dest_id]
    dist_km = haversine_km(orig_zone["lat"], orig_zone["lon"], dest_zone["lat"], dest_zone["lon"])

    if time_period in ("PEAK_MORNING", "PEAK_EVENING"):
        car_speed_kmh = 23.6
        lumina_oart_speed_kmh = 42.4
        congestion_badge = "CONGESTIÓN VIAL SEVERA (23.6 km/h)"
    else:
        car_speed_kmh = 44.5
        lumina_oart_speed_kmh = 48.0
        congestion_badge = "TRÁFICO MODERADO VALLE (44.5 km/h)"

    engine_used = "SIMULACION_CON_SUPUESTOS_NO_OBSERVADOS"
    if HAS_BIGQUERY and os.getenv("ENABLE_LIVE_BQ_ROUTE_QUERY", "false").lower() == "true":
        try:
            bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
            sql_speed = f"""
            SELECT ROUND(AVG(avg_speed_kmh), 1) AS speed_kmh
            FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_SILVER}.{config.BQ_SILVER_UBER}`
            WHERE origin_zone_id = @orig_id
              AND dest_zone_id = @dest_id
              AND time_period = @period
              AND travel_date = @travel_date
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("orig_id", "STRING", orig_id),
                    bigquery.ScalarQueryParameter("dest_id", "STRING", dest_id),
                    bigquery.ScalarQueryParameter("period", "STRING", time_period),
                    bigquery.ScalarQueryParameter("travel_date", "DATE", datetime.now(timezone.utc).date()),
                ]
            )
            rows = list(bq_client.query(sql_speed, job_config=job_config).result(timeout=4.0))
            if rows and rows[0]["speed_kmh"]:
                car_speed_kmh = float(rows[0]["speed_kmh"])
                engine_used = "SIMULACION_CON_TRAFICO_DE_FECHA_ACTUAL_Y_TARIFAS_SUPUESTAS"
        except Exception as exc:
            logger.info("Usando velocidad calibrada en memoria: %s", exc)

    car_duration_min = round((dist_km / car_speed_kmh) * 60.0, 1)
    lumina_duration_min = round((dist_km / lumina_oart_speed_kmh) * 60.0 + 3.5, 1)

    car_total_usd = round(config.CAR_FIXED_COST_USD + (dist_km * config.CAR_VARIABLE_COST_USD_KM), 2)
    car_cost_per_km = round(car_total_usd / dist_km, 3)

    lumina_total_usd = round(1.50 + (dist_km * 0.41 * (1.0 if passengers == 1 else 0.88)), 2)
    lumina_cost_per_km = round(lumina_total_usd / dist_km, 3)

    savings_usd = round(car_total_usd - lumina_total_usd, 2)
    savings_pct = round((savings_usd / car_total_usd) * 100.0, 1) if car_total_usd else None
    time_saved_min = round(car_duration_min - lumina_duration_min, 1)

    if orig_zone["zone_type"] == "SUBURBIO_ADULTO_MAYOR":
        first_mile_mode = f"Minivan Eléctrica Accesible Puerta-a-Puerta ➔ Estación {orig_zone['nearest_oart_station_id']}"
        agent_reason = (
            f"Zona con alta población de Adultos Mayores ({orig_zone['zone_name']}). "
            f"Agent Platform prioriza Minivan accesible hasta {orig_zone['nearest_oart_station_id']} (< 8 km) "
            f"conectando con tren rápido OART hacia {dest_zone['nearest_oart_station_id']}."
        )
    elif orig_zone["zone_type"] == "SUBURBIO_JOVEN_ESTUDIANTIL":
        first_mile_mode = f"Lumina Bike-Share / Bus Rápido ➔ Estación {orig_zone['nearest_oart_station_id']}"
        agent_reason = (
            f"Corredor estudiantil joven ({orig_zone['zone_name']}). "
            f"Agent Platform asigna carril de Micromovilidad + Tren OART hacia {dest_zone['nearest_oart_station_id']}."
        )
    else:
        first_mile_mode = f"Shuttle EV Compartido ➔ Red Ferroviaria OART ({orig_zone['nearest_oart_station_id']} ➔ {dest_zone['nearest_oart_station_id']})"
        agent_reason = (
            f"Evita el cuello de botella vial de {car_speed_kmh} km/h en {orig_zone['county_name']} "
            f"combinando primera milla compartida y riel exclusivo OART a {lumina_oart_speed_kmh} km/h."
        )

    return {
        "status": "ok",
        "engine_source": engine_used,
        "is_simulation": True,
        "assumptions": {"car_fixed_usd": config.CAR_FIXED_COST_USD,
                        "car_variable_usd_km": config.CAR_VARIABLE_COST_USD_KM,
                        "oart_speed_kmh": lumina_oart_speed_kmh,
                        "transfer_time_min": 3.5,
                        "limitation": "No reserva vehículos ni usa horarios, GPS o disponibilidad real. Tarifas supuestas."},
        "route": {
            "origin_zone_id": orig_id,
            "origin_zone_name": orig_zone["zone_name"],
            "origin_county": orig_zone["county_name"],
            "origin_oart_station": orig_zone["nearest_oart_station_id"],
            "dest_zone_id": dest_id,
            "dest_zone_name": dest_zone["zone_name"],
            "dest_county": dest_zone["county_name"],
            "dest_oart_station": dest_zone["nearest_oart_station_id"],
            "distance_km": dist_km,
            "time_period": time_period,
            "congestion_status": congestion_badge,
            "passengers": passengers,
        },
        "comparison": {
            "lumina_multimodal": {
                "itinerary": first_mile_mode,
                "total_cost_usd": lumina_total_usd,
                "cost_per_km_usd": lumina_cost_per_km,
                "duration_min": lumina_duration_min,
                "effective_speed_kmh": lumina_oart_speed_kmh,
            },
            "personal_car_benchmark": {
                "itinerary": "Conducción Individual por Vía Arterial Congestionada",
                "total_cost_usd": car_total_usd,
                "cost_per_km_usd": car_cost_per_km,
                "duration_min": car_duration_min,
                "effective_speed_kmh": car_speed_kmh,
            },
            "savings": {
                "saved_usd": savings_usd,
                "saved_pct": savings_pct,
                "time_saved_min": time_saved_min,
                "co2_avoided_kg": round(dist_km * 0.192, 2),
            },
        },
        "agent_platform_recommendation": agent_reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def execute_bq_sql_safe(sql: str) -> Optional[Dict[str, Any]]:
    """Ejecuta una consulta SQL segura en BigQuery Gold con fallback en caso de error."""
    if not HAS_BIGQUERY:
        return None
    try:
        client = bigquery.Client(project=config.GCP_PROJECT_ID)
        job = client.query(sql)
        for row in job.result():
            return dict(row)
    except Exception as exc:
        logger.warning("Fallo ejecución SQL dinámica en BigQuery: %s", exc)
    return None


def process_agent_query(user_query: str) -> Dict[str, Any]:
    """Consultas de lectura predefinidas. Nunca inventa resultados si BigQuery falla."""
    start = time.monotonic()
    query = user_query.lower()
    prefix = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}"
    source = f"{prefix}.{config.BQ_GOLD_FACT_TRIPS}"
    if any(k in query for k in ("sensor", "iot", "pdm", "mantenim", "alerta")):
        source = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_SILVER}.{config.BQ_SILVER_IOT}"
        sql = f"""WITH latest AS (
          SELECT * FROM `{source}`
          WHERE timestamp_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
          QUALIFY ROW_NUMBER() OVER(PARTITION BY node_id ORDER BY timestamp_evento DESC, event_id DESC) = 1
        ) SELECT COUNT(*) AS sensores_con_datos_ultima_hora,
          COUNTIF(requiere_mantenimiento_pdm) AS activos_con_alerta,
          AVG(passenger_load_pct) AS ocupacion_pct FROM latest"""
    elif any(k in query for k in ("plan", "recurso", "mañana", "demanda")):
        source = f"{prefix}.plan_diario_recursos_gold"
        sql = f"""SELECT COUNT(*) AS zonas, SUM(viajes_previstos) AS viajes_previstos,
          COUNTIF(estado = 'HISTORIA_INSUFICIENTE') AS zonas_sin_historia,
          SUM(vehiculos_equivalentes_requeridos) AS vehiculos_equivalentes FROM `{source}`"""
    else:
        sql = f"""SELECT COUNT(*) AS viajes, SUM(trip_distance_km) AS distancia_km,
          SAFE_DIVIDE(SUM(lumina_total_fare_usd), SUM(trip_distance_km)) AS ingreso_km_usd,
          AVG(lumina_duration_min) AS duracion_media_min,
          AVG(time_saved_min_vs_car) AS ahorro_min_solo_con_trafico_coincidente,
          COUNTIF(traffic_match_found) AS viajes_con_trafico_coincidente
          FROM `{source}` WHERE transport_mode != 'PERSONAL_CAR_BENCHMARK'"""
    row = execute_bq_sql_safe(sql)
    if row is None:
        reply = "Datos no disponibles: no se pudo consultar BigQuery. No se generaron métricas de respaldo."
    else:
        reply = ("<b>Resultado de consulta</b><br>" +
                 "<br>".join(f"{html_lib.escape(k)}: {html_lib.escape(str(v))}" for k, v in row.items()) +
                 "<br><small>Los resultados heredan la procedencia de las tablas. "
                 "La comparación económica utiliza supuestos; no acredita ahorro observado.</small>")
    return {"status": "ok" if row is not None else "unavailable", "query": user_query,
            "reply_html": reply, "sql_executed": " ".join(sql.split()) if row is not None else None,
            "latency_ms": round((time.monotonic() - start) * 1000, 1),
            "metrics": row or {}, "dataset_source": source if row is not None else None}


def load_operational_plan(kind):
    names = {"daily": "plan_diario_recursos_gold", "maintenance": "mantenimiento_proactivo_gold"}
    if kind not in names:
        raise ValueError("Servicio no válido")
    if not HAS_BIGQUERY:
        return {"status": "unavailable", "rows": [], "reason": "BigQuery no disponible"}
    try:
        client = bigquery.Client(project=config.GCP_PROJECT_ID)
        sql = f"SELECT * FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}.{names[kind]}` LIMIT 1000"
        rows = [dict(r) for r in client.query(sql).result(timeout=10)]
        # Normaliza DATE y TIMESTAMP para Flask y HTTP estándar.
        return {"status": "ok", "rows": json.loads(json.dumps(rows, default=str)),
                "source": names[kind], "limitation": "Baseline exploratorio; requiere revisión operativa"}
    except Exception as exc:
        logger.warning("Plan no disponible: %s", exc)
        return {"status": "unavailable", "rows": [], "reason": "Consulta no disponible"}


def render_standalone_dashboard_html(csp_nonce: str = "lumina-static-nonce") -> str:
    """
    Renderiza el HTML completo con mapa interactivo y agente.
    """
    data = load_gold_dashboard_data()
    zones = config.DATALANDIA_ZONES
    template_path = Path(__file__).resolve().parent / "templates" / "lumina_dashboard.html"
    raw = template_path.read_text(encoding="utf-8")

    import re
    raw = raw.replace("{{ project_id }}", config.GCP_PROJECT_ID)
    raw = raw.replace("{{ initial_data.recent_iot_telemetry | tojson | safe }}", json.dumps(data.get("recent_iot_telemetry", []), ensure_ascii=False))
    
    # Render zones select options
    orig_opts = "\n".join(
        f'<option value="{z["zone_id"]}" {"selected" if z["zone_id"] == "ZDL-003" else ""}>'
        f'{z["zone_name"]} ({z["county_name"]})</option>'
        for z in zones
    )
    dest_opts = "\n".join(
        f'<option value="{z["zone_id"]}" {"selected" if z["zone_id"] == "ZDL-001" else ""}>'
        f'{z["zone_name"]} ({z["county_name"]})</option>'
        for z in zones
    )

    raw = re.sub(
        r'<select id="sel-orig"[^>]*>.*?</select>',
        f'<select id="sel-orig" class="form-select">\n{orig_opts}\n</select>',
        raw,
        flags=re.DOTALL
    )
    raw = re.sub(
        r'<select id="sel-dest"[^>]*>.*?</select>',
        f'<select id="sel-dest" class="form-select">\n{dest_opts}\n</select>',
        raw,
        flags=re.DOTALL
    )

    # Zones map JSON
    zones_json = json.dumps({z["zone_id"]: z for z in zones}, ensure_ascii=False)
    raw = re.sub(r'const ZONES_DATA = \{.*?\};', f'const ZONES_DATA = {zones_json};', raw, flags=re.DOTALL)

    # Telemetry list
    telemetry_html = []
    for iot in data.get("recent_iot_telemetry", [])[:8]:
        st_color = "var(--accent-rose)" if iot.get("sensor_status") == "CRITICAL_PDM" else ("var(--accent-amber)" if iot.get("sensor_status") == "WARNING_PDM" else "var(--accent-green)")
        st_label = "🚨 ALERTA PdM" if iot.get("sensor_status") == "CRITICAL_PDM" else ("⚠️ WARNING" if iot.get("sensor_status") == "WARNING_PDM" else "🟢 NORMAL")
        telemetry_html.append(f"""
        <div style="background:rgba(15,23,42,0.6);border:1px solid var(--border-color);border-radius:8px;padding:12px;">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <b style="font-size:12.5px;color:#FFF;">{iot.get("node_id")}</b>
            <span style="font-size:10px;background:rgba(255,255,255,0.1);color:{st_color};padding:2px 6px;border-radius:4px;">{st_label}</span>
          </div>
          <p style="font-size:11.5px;color:var(--text-muted);margin-top:4px;">
            Estación: <code>{iot.get("station_id")}</code> | Componente: {iot.get("asset_type")}
          </p>
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;margin-top:8px;font-size:11px;">
            <div>Vibración: <b style="color:#FFF;">{iot.get("vibration_mms")} mm/s</b></div>
            <div>Temp: <b style="color:#FFF;">{iot.get("temperature_c")} °C</b></div>
            <div>Ocupación: <b style="color:#FFF;">{iot.get("passenger_load_pct")}%</b></div>
          </div>
        </div>
        """)
    
    raw = re.sub(
        r'\{%\s*for iot in initial_data\.recent_iot_telemetry\s*%\}.*?\{%\s*endfor\s*%\}',
        "\n".join(telemetry_html),
        raw,
        flags=re.DOTALL
    )

    # La alternativa sin Flask también debe resolver la tabla de condados.
    county_rows = []
    def visible(row, *keys):
        return next((row[k] for k in keys if row.get(k) is not None), "N/D")
    for county in data.get("counties_kpis", []):
        values = [visible(county, "county_name"), visible(county, "county_id"),
                  visible(county, "poblacion_residente", "poblacion_total"),
                  visible(county, "pct_adulto_mayor_65", "pct_adulto_mayor"),
                  visible(county, "pct_menos_8km_estacion_oart", "cobertura_oart_pct")]
        name, cid, pop, elderly, coverage = [html_lib.escape(str(x)) for x in values]
        county_rows.append(f'<article style="padding:12px;border:1px solid #475569;border-radius:8px">'
                           f'<b>{name} ({cid})</b><p>Población: {pop} hab. · Adultos mayores: {elderly}%</p>'
                           f'<p>Cobertura OART: {coverage}%</p></article>')
    raw = re.sub(r'\{%\s*for c in initial_data\.counties_kpis\s*%\}.*?\{%\s*endfor\s*%\}',
                 lambda _: "\n".join(county_rows), raw, flags=re.DOTALL)

    return raw


if HAS_FLASK:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = get_app_secret()
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    @app.before_request
    def attach_csp_nonce() -> None:
        g.csp_nonce = secrets.token_urlsafe(16)

    @app.after_request
    def set_security_headers(response: Response) -> Response:
        nonce = getattr(g, "csp_nonce", "")
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self' https: data: 'unsafe-inline' 'unsafe-eval'; "
            "script-src 'self' https: 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' https: 'unsafe-inline'; "
            "font-src 'self' https: data:; "
            "img-src 'self' https: data: blob:; "
            "connect-src 'self' https:;"
        )
        return response

    @app.route("/", methods=["GET"])
    def home():
        return p3_dashboard()

    @app.route("/p3", methods=["GET"])
    def p3_dashboard():
        content = (Path(__file__).parent / "templates" / "p3_olap.html").read_text(encoding="utf-8")
        return Response(content, mimetype="text/html")

    @app.route("/api/v1/olap-kpis", methods=["GET"])
    def olap_kpis():
        data = load_olap_report()
        return jsonify(data), 200 if data["status"] == "ok" else 503

    @app.route("/api/v1/health", methods=["GET"])
    def api_health() -> Response:
        return jsonify({
            "status": "healthy",
            "service": "lumina-smart-mobility-bff-api",
            "gcp_project": config.GCP_PROJECT_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    @app.route("/api/v1/daily-plan", methods=["GET"])
    def daily_plan():
        return jsonify(load_operational_plan("daily"))

    @app.route("/api/v1/maintenance-plan", methods=["GET"])
    def maintenance_plan():
        return jsonify(load_operational_plan("maintenance"))

    @app.route("/api/v1/dashboard-summary", methods=["GET"])
    def api_dashboard_summary() -> Response:
        data = load_olap_report()
        return jsonify(data), 200 if data["status"] == "ok" else 503

    @app.route("/api/v1/iot-telemetry", methods=["GET"])
    def api_iot_telemetry() -> Response:
        return jsonify({"status":"unavailable","active_nodes":[],
                        "reason":"No se publican sensores de demostración como mediciones actuales."}), 503

    @app.route("/api/v1/route-simulator", methods=["POST"])
    def api_route_simulator() -> tuple[Response, int]:
        return jsonify({"status":"disabled","reason":"P3 utiliza datos existentes, sin simulación de viajes."}), 410

    @app.route("/api/v1/agent-query", methods=["POST"])
    def api_agent_query() -> tuple[Response, int]:
        body = request.get_json(silent=True) or {}
        q = str(body.get("query", "")).strip()
        return jsonify(process_agent_query(q)), 200
else:
    app = None


class StdlibLuminaHandler(BaseHTTPRequestHandler):
    """Servidor HTTP nativo de respaldo en caso de ejecutarse sin Flask instalado."""

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/p3":
            content = (Path(__file__).parent / "templates" / "p3_olap.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif self.path == "/api/v1/olap-kpis":
            data = load_olap_report()
            self._send_json(data, status=200 if data["status"] == "ok" else 503)
        elif self.path in ("/", "/index.html"):
            html_bytes = (Path(__file__).parent / "templates" / "p3_olap.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html_bytes)))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.end_headers()
            self.wfile.write(html_bytes)
        elif self.path == "/api/v1/health":
            self._send_json({
                "status": "healthy",
                "service": "lumina-smart-mobility-bff-api",
                "gcp_project": config.GCP_PROJECT_ID,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        elif self.path == "/api/v1/daily-plan":
            self._send_json(load_operational_plan("daily"))
        elif self.path == "/api/v1/maintenance-plan":
            self._send_json(load_operational_plan("maintenance"))
        elif self.path == "/api/v1/dashboard-summary":
            data = load_olap_report()
            self._send_json(data, status=200 if data["status"] == "ok" else 503)
        elif self.path == "/api/v1/iot-telemetry":
            self._send_json({"status":"unavailable","active_nodes":[],
                "reason":"No se publican sensores de demostración como mediciones actuales."},status=503)
        else:
            self._send_json({"status": "not_found", "path": self.path}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/api/v1/route-simulator":
            self._send_json({"status":"disabled","reason":"P3 utiliza datos existentes, sin simulación de viajes."},status=410)
        elif self.path == "/api/v1/agent-query":
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            try:
                body = json.loads(raw_body)
            except Exception:
                body = {}
            q = str(body.get("query", "")).strip()
            self._send_json(process_agent_query(q), status=200)
        else:
            self._send_json({"status": "not_found", "path": self.path}, status=404)


if __name__ == "__main__":
    print("==========================================================================")
    print("🚀 LUMINA SMART MOBILITY - FRONT-END & API DE EXPOSICIÓN (ENTREGA 3 - P3)")
    print(f"   🌐 Abre en tu navegador: http://{config.FLASK_HOST}:{config.FLASK_PORT}")
    print(f"   🔌 Endpoint Resumen JSON: http://{config.FLASK_HOST}:{config.FLASK_PORT}/api/v1/dashboard-summary")
    print(f"   🔌 Endpoint IoT & DLQ   : http://{config.FLASK_HOST}:{config.FLASK_PORT}/api/v1/iot-telemetry")
    print("==========================================================================")
    if HAS_FLASK and app is not None:
        app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=False)
    else:
        logger.info("Ejecutando con servidor HTTP nativo de Python (Zero-Dependency Mode)...")
        server = HTTPServer((config.FLASK_HOST, config.FLASK_PORT), StdlibLuminaHandler)
        server.serve_forever()
