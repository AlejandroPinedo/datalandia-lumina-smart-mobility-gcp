#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_ingesta_batch_multifuente.py
Implementa la Capa de Ingesta Batch Multifuente hacia Cloud Storage (Data Lake) y
BigQuery (Capa Bronze) integrando las 3 fuentes históricas/referenciales del caso:
  1. F4 - Censo de Población y Vivienda (INE) -> 9.25 millones de residentes en los
          8 condados de DataLandia, reflejando el 89% a < 8 km de una estación OART,
          adultos mayores en suburbios y jóvenes universitarios.
  2. F2 - SF Caltrain / Uber Movement Speeds -> Tiempos de viaje y velocidades horarias
          entre las zonas de DataLandia, reflejando la congestión crítica de 24 km/h
          (15 mph) en horas punta frente a horarios valle.
  3. F1 - NYC TLC Trip Record Data (adaptado a Lumina) -> Viajes multimodales de las
          12 agencias unificadas (Tren OART + Minivan + Bike) comparados contra viajes
          en automóvil particular (Personal Car Benchmark), calculando el costo por km.

Flujo de Ingesta:
  Generación/Adaptación Local (CSV abiertos) -> Subida a gs://{BUCKET}/landing/batch/
  -> Carga nativa vía `bq load` hacia las tablas particionadas/clusterizadas en BigQuery.
"""

import csv
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import random
import subprocess
import uuid

import config

DATA_DIR = Path(__file__).resolve().parent / "data_landing_local"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd: list[str], desc: str) -> bool:
    env = os.environ.copy()
    env["CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE"] = "false"
    env["CLOUDSDK_BILLING_QUOTA_PROJECT"] = config.GCP_PROJECT_ID
    env["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
    print(f"⚙️  {desc}...")
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"   ✅ {desc}")
        return True
    print(f"   ⚠️  Aviso en {desc}: {(res.stderr or res.stdout).strip()[:280]}")
    return False


def generar_censo_demografia_ine() -> Path:
    """
    Fuente F4: Censo Demográfico INE adaptado a los 8 Condados de DataLandia
    (Total: ~9.25 millones de habitantes, 89% a menos de 8 km de estación OART).
    """
    csv_path = DATA_DIR / "censo_demografia_ine_datalandia.csv"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    perfiles_censales = {
        "ZDL-001": (1450000, 580000, 14.2, 34.5, 5400.0, 42.0, 96.5, 88.0),
        "ZDL-002": (1280000, 490000, 11.8, 38.0, 6200.0, 28.0, 92.0, 84.5),
        "ZDL-003": (980000,  360000, 29.5, 16.0, 4100.0, 14.0, 87.0, 94.0),  # Suburbio Adulto Mayor
        "ZDL-004": (1320000, 470000, 10.5, 44.2, 2850.0, 51.0, 91.0, 96.5),  # Corredor Joven sin vivienda cerca
        "ZDL-005": (1050000, 410000, 16.0, 29.0, 4600.0, 33.0, 94.0, 81.0),
        "ZDL-006": (1190000, 430000, 13.5, 27.5, 3400.0, 24.0, 85.0, 86.0),
        "ZDL-007": (920000,  350000, 33.8, 14.5, 3950.0, 12.0, 84.0, 95.5),  # Suburbio Adulto Mayor
        "ZDL-008": (1060000, 390000, 19.4, 25.0, 3700.0, 19.0, 82.5, 79.0),
    }

    fieldnames = [
        "zone_id", "zone_name", "county_id", "county_name",
        "nearest_oart_station_id", "zone_type", "poblacion_residente",
        "viviendas_totales", "pct_adulto_mayor_65", "pct_jovenes_18_34",
        "ingreso_medio_hogar_usd", "pct_hogares_sin_auto",
        "pct_menos_8km_estacion_oart", "indice_prioridad_primera_milla",
        "center_lat", "center_lon", "ts_ingesta_bq",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for z in config.DATALANDIA_ZONES:
            zid = z["zone_id"]
            pob, viv, p_may, p_jov, ing, sin_auto, cob_oart, prio = perfiles_censales[zid]
            writer.writerow({
                "zone_id": zid,
                "zone_name": z["zone_name"],
                "county_id": z["county_id"],
                "county_name": z["county_name"],
                "nearest_oart_station_id": z["nearest_oart_station_id"],
                "zone_type": z["zone_type"],
                "poblacion_residente": pob,
                "viviendas_totales": viv,
                "pct_adulto_mayor_65": p_may,
                "pct_jovenes_18_34": p_jov,
                "ingreso_medio_hogar_usd": ing,
                "pct_hogares_sin_auto": sin_auto,
                "pct_menos_8km_estacion_oart": cob_oart,
                "indice_prioridad_primera_milla": prio,
                "center_lat": z["lat"],
                "center_lon": z["lon"],
                "ts_ingesta_bq": now_iso,
            })

    print(f"📄 Generado dataset Censo INE (F4): {csv_path.name} (8 condados, 9.25M residentes)")
    return csv_path


def generar_uber_movement_speeds() -> Path:
    """
    Fuente F2: SF Caltrain / Uber Movement Data adaptado a los corredores de DataLandia.
    Modela explícitamente el cuello de botella de 24 km/h (15 mph) en horas punta
    (07:00-09:00 y 17:00-19:00) frente a ~48 km/h en horas valle.
    """
    random.seed(42)
    csv_path = DATA_DIR / "uber_movement_speeds_datalandia.csv"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    base_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    fieldnames = [
        "movement_id", "travel_date", "hod", "time_period",
        "sourceid", "dstid", "origin_zone_id", "dest_zone_id",
        "county_id", "distance_corridor_km", "mean_travel_time_sec",
        "stddev_travel_time_sec", "avg_speed_kmh", "is_severe_congestion",
        "ts_ingesta_bq",
    ]

    rows = []
    for orig in config.DATALANDIA_ZONES:
        for dest in config.DATALANDIA_ZONES:
            if orig["zone_id"] == dest["zone_id"]:
                continue
            dist_km = round(random.uniform(6.5, 28.0), 2)
            for hod in [7, 8, 9, 12, 13, 17, 18, 19, 22]:
                if hod in (7, 8, 9):
                    period = "PEAK_MORNING"
                    # Velocidad promedio ~24 km/h en hora punta (según enunciado DataLandia)
                    speed_kmh = round(random.gauss(23.8, 2.4), 2)
                elif hod in (17, 18, 19):
                    period = "PEAK_EVENING"
                    speed_kmh = round(random.gauss(23.2, 2.6), 2)
                else:
                    period = "OFF_PEAK"
                    speed_kmh = round(random.gauss(46.5, 4.5), 2)

                speed_kmh = max(14.0, min(speed_kmh, 65.0))
                mean_sec = round((dist_km / speed_kmh) * 3600.0, 1)
                stddev_sec = round(mean_sec * random.uniform(0.12, 0.25), 1)
                is_congested = speed_kmh <= 25.0

                rows.append({
                    "movement_id": f"UMV-{orig['zone_id']}-{dest['zone_id']}-H{hod:02d}",
                    "travel_date": str(base_date),
                    "hod": hod,
                    "time_period": period,
                    "sourceid": orig["uber_movement_id"],
                    "dstid": dest["uber_movement_id"],
                    "origin_zone_id": orig["zone_id"],
                    "dest_zone_id": dest["zone_id"],
                    "county_id": orig["county_id"],
                    "distance_corridor_km": dist_km,
                    "mean_travel_time_sec": mean_sec,
                    "stddev_travel_time_sec": stddev_sec,
                    "avg_speed_kmh": speed_kmh,
                    "is_severe_congestion": is_congested,
                    "ts_ingesta_bq": now_iso,
                })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"📄 Generado dataset Uber Movement (F2): {csv_path.name} ({len(rows)} registros O-D horarios)")
    return csv_path


def generar_nyc_tlc_trips_multimodal(num_trips: int = 600) -> Path:
    """
    Fuente F1: NYC TLC Trip Record Data adaptado a los viajes multimodales de Lumina
    y su comparativa directa con la conducción en vehículo personal.
    """
    random.seed(101)
    csv_path = DATA_DIR / "nyc_tlc_trips_lumina_multimodal.csv"
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    base_day = (now_utc - timedelta(days=1)).replace(minute=0, second=0, microsecond=0)

    modos = [
        ("LUMINA_MULTIMODAL_OART", 0.38, 42.0, 0.42),   # Tren OART + Minivan primera milla (rápido y barato)
        ("LUMINA_EV_MINIVAN_POOL", 0.24, 31.5, 0.58),   # Minivan eléctrica compartida
        ("LUMINA_BUS_RAPID",       0.14, 29.0, 0.35),   # Bus rápido integrado
        ("LUMINA_BIKE_AND_OART",   0.09, 36.0, 0.28),   # Bicicleta + Tren OART
        ("PERSONAL_CAR_BENCHMARK", 0.15, 23.6, 1.18),   # Benchmark Auto Particular (24 km/h y alto costo/km)
    ]

    fieldnames = [
        "trip_id", "pickup_datetime", "dropoff_datetime", "hod",
        "pulocation_id", "dolocation_id", "origin_zone_id", "dest_zone_id",
        "county_id", "agency_id", "transport_mode", "connected_oart_station_id",
        "passenger_count", "trip_distance_km", "duration_minutes",
        "effective_speed_kmh", "base_fare_usd", "total_fare_usd",
        "cost_per_km_usd", "payment_type", "ts_ingesta_bq",
    ]

    rows = []
    for i in range(num_trips):
        orig = random.choice(config.DATALANDIA_ZONES)
        dest = random.choice([z for z in config.DATALANDIA_ZONES if z["zone_id"] != orig["zone_id"]])
        hod = random.choice([7, 8, 8, 9, 12, 17, 18, 18, 19, 21])
        pickup_dt = base_day.replace(hour=hod) + timedelta(minutes=random.randint(0, 58))

        modo, _, base_speed, rate_per_km = random.choices(
            modos, weights=[m[1] for m in modos], k=1
        )[0]

        dist_km = round(random.uniform(5.0, 26.0), 2)
        if hod in (7, 8, 9, 17, 18, 19) and modo == "PERSONAL_CAR_BENCHMARK":
            speed = round(random.gauss(23.5, 2.2), 2)  # Atascado a ~24 km/h
        elif hod in (7, 8, 9, 17, 18, 19):
            speed = round(random.gauss(base_speed, 3.0), 2)  # Carril exclusivo / Riel OART
        else:
            speed = round(random.gauss(base_speed + 8.0, 3.5), 2)

        speed = max(15.0, speed)
        duration_min = round((dist_km / speed) * 60.0, 2)
        dropoff_dt = pickup_dt + timedelta(minutes=duration_min)

        base_fare = 1.50 if modo != "PERSONAL_CAR_BENCHMARK" else 3.20
        total_fare = round(base_fare + (dist_km * rate_per_km * random.uniform(0.92, 1.08)), 2)
        cost_per_km = round(total_fare / dist_km, 4)

        agency_num = random.randint(1, 12)
        pax = 1 if modo == "PERSONAL_CAR_BENCHMARK" else random.randint(2, 6)

        rows.append({
            "trip_id": f"TRIP-{uuid.uuid4().hex[:10].upper()}",
            "pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "hod": hod,
            "pulocation_id": orig["tlc_location_id"],
            "dolocation_id": dest["tlc_location_id"],
            "origin_zone_id": orig["zone_id"],
            "dest_zone_id": dest["zone_id"],
            "county_id": orig["county_id"],
            "agency_id": f"AG-{agency_num:02d}",
            "transport_mode": modo,
            "connected_oart_station_id": orig["nearest_oart_station_id"],
            "passenger_count": pax,
            "trip_distance_km": dist_km,
            "duration_minutes": duration_min,
            "effective_speed_kmh": speed,
            "base_fare_usd": base_fare,
            "total_fare_usd": total_fare,
            "cost_per_km_usd": cost_per_km,
            "payment_type": "LUMINA_UNIFIED_PAY" if modo != "PERSONAL_CAR_BENCHMARK" else "EXTERNAL_TOLL_FUEL",
            "ts_ingesta_bq": now_iso,
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"📄 Generado dataset NYC TLC Multimodal (F1): {csv_path.name} ({len(rows)} viajes)")
    return csv_path


def ejecutar_ingesta_batch_gcp():
    config.mostrar_resumen_config()

    csv_censo = generar_censo_demografia_ine()
    csv_uber = generar_uber_movement_speeds()
    csv_tlc = generar_nyc_tlc_trips_multimodal(num_trips=600)

    # 1. Subir archivos crudos al Data Lake en Cloud Storage (Zona Landing Batch)
    archivos_landing = [
        (csv_censo, f"gs://{config.GCS_BUCKET_DATALAKE}/landing/batch/censo_ine/{csv_censo.name}", config.BQ_TABLE_CENSO_INE),
        (csv_uber,  f"gs://{config.GCS_BUCKET_DATALAKE}/landing/batch/uber_movement/{csv_uber.name}", config.BQ_TABLE_UBER_SPEEDS),
        (csv_tlc,   f"gs://{config.GCS_BUCKET_DATALAKE}/landing/batch/tlc_trips/{csv_tlc.name}", config.BQ_TABLE_TLC_TRIPS),
    ]

    for local_file, gcs_uri, bq_table in archivos_landing:
        ok_gcs = run_cmd(
            ["gcloud", "storage", "cp", str(local_file), gcs_uri, f"--project={config.GCP_PROJECT_ID}"],
            f"Subiendo '{local_file.name}' al Data Lake GCS ({gcs_uri})",
        )
        source_to_load = gcs_uri if ok_gcs else str(local_file)
        run_cmd(
            [
                "bq", "load",
                "--replace",
                "--source_format=CSV",
                "--skip_leading_rows=1",
                f"--project_id={config.GCP_PROJECT_ID}",
                f"{config.BQ_DATASET_BRONZE}.{bq_table}",
                source_to_load,
            ],
            f"Cargando '{local_file.name}' en BigQuery Bronze ({config.BQ_DATASET_BRONZE}.{bq_table})",
        )

    print("\n✅ ¡Ingesta Batch Multifuente (NYC TLC + Uber Movement + Censo INE) completada en GCS y BigQuery!")
    print("👉 Siguiente paso: python3 03_simulador_streaming_iot_aot_pubsub.py")


if __name__ == "__main__":
    ejecutar_ingesta_batch_gcp()
