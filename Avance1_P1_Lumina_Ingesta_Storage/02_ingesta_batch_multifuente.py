#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generación de datos sintéticos o carga de CSV canónicos mediante staging y MERGE.
Por defecto solo escribe archivos locales. --upload habilita la escritura en GCP.
No descarga ni homologa automáticamente los datasets externos sugeridos por el taller."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
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
    raise RuntimeError(f"Fallo de ingesta: {desc}")


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

    perfiles_censales = {f"CNT-{i:02d}": profile for i, profile in enumerate(perfiles_censales.values(), 1)}
    # Repartir los totales de cada condado entre sus zonas sin duplicar población.
    reparto = {}
    for county, profile in perfiles_censales.items():
        zones = [z for z in config.DATALANDIA_ZONES if z["county_id"] == county]
        weight = sum(z["population"] for z in zones)
        remaining_pop, remaining_homes = profile[:2]
        for i, zone in enumerate(zones):
            pop = round(profile[0] * zone["population"] / weight) if i < len(zones) - 1 else remaining_pop
            homes = round(profile[1] * zone["population"] / weight) if i < len(zones) - 1 else remaining_homes
            reparto[zone["zone_id"]] = (pop, homes, *profile[2:])
            remaining_pop -= pop
            remaining_homes -= homes

    fieldnames = [
        "zone_id", "zone_name", "county_id", "county_name",
        "nearest_oart_station_id", "zone_type", "poblacion_residente",
        "viviendas_totales", "pct_adulto_mayor_65", "pct_jovenes_18_34",
        "ingreso_medio_hogar_usd", "pct_hogares_sin_auto",
        "pct_menos_8km_estacion_oart", "indice_prioridad_primera_milla",
        "center_lat", "center_lon", "ts_ingesta_bq",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator=chr(10))
        writer.writeheader()
        for z in config.DATALANDIA_ZONES:
            zid = z["zone_id"]
            pob, viv, p_may, p_jov, ing, sin_auto, cob_oart, prio = reparto[zid]
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
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator=chr(10))
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
        "cost_per_km_usd", "payment_type", "rider_id_hash", "payment_token_masked", "ts_ingesta_bq",
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
            "trip_id": "TRIP-" + hashlib.sha256(f"{base_day.date()}:{i}:{num_trips}".encode()).hexdigest()[:24],
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
            "rider_id_hash": hashlib.sha256(f"usr_{orig['zone_id']}_{i}_{agency_num}".encode("utf-8")).hexdigest()[:24],
            "payment_token_masked": f"tok_lumina_{random.randint(1000, 9999)}****" if modo != "PERSONAL_CAR_BENCHMARK" else "ext_direct_cash",
            "ts_ingesta_bq": now_iso,
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator=chr(10))
        writer.writeheader()
        writer.writerows(rows)

    print(f"📄 Generado dataset NYC TLC Multimodal (F1): {csv_path.name} ({len(rows)} viajes)")
    return csv_path


def cargar_csv_sin_reemplazar(local_file, table_name, merge_keys):
    """Carga staging con esquema del destino y MERGE idempotente por llave natural."""
    stage = f"{table_name}_stage_{uuid.uuid4().hex[:12]}"
    target = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{table_name}"
    staging = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}.{stage}"
    digest = hashlib.sha256(local_file.read_bytes()).hexdigest()[:16]
    uri = f"gs://{config.GCS_BUCKET_DATALAKE}/landing/batch/{table_name}/{digest}.csv"
    query_prefix = ["bq", "query", "--use_legacy_sql=false", f"--project_id={config.GCP_PROJECT_ID}",
                    f"--location={config.GCP_LOCATION}"]
    run_cmd(["gcloud", "storage", "cp", str(local_file), uri,
             f"--project={config.GCP_PROJECT_ID}"], "Preservar lote en GCS")
    run_cmd(query_prefix + [f"CREATE TABLE `{staging}` LIKE `{target}`"], "Crear staging")
    try:
        run_cmd(["bq", "load", "--source_format=CSV", "--skip_leading_rows=1",
                 f"--project_id={config.GCP_PROJECT_ID}", f"--location={config.GCP_LOCATION}",
                 staging, uri], "Cargar staging (sin reemplazar Bronze)")
        with local_file.open(encoding="utf-8", newline="") as f:
            columns = next(csv.reader(f))
        # Los nombres provienen del esquema de los CSV admitidos, nunca de SQL libre.
        if not all(c.replace("_", "").isalnum() and not c[0].isdigit() for c in columns):
            raise ValueError("Cabecera CSV no válida")
        on = " AND ".join(f"T.`{k}` = S.`{k}`" for k in merge_keys)
        names = ", ".join(f"`{c}`" for c in columns)
        values = ", ".join(f"S.`{c}`" for c in columns)
        updates = ", ".join(f"T.`{c}` = S.`{c}`" for c in columns if c not in merge_keys)
        partition = ", ".join(f"`{k}`" for k in merge_keys)
        sql = (f"MERGE `{target}` T USING (SELECT * FROM `{staging}` "
               f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY ts_ingesta_bq DESC) = 1) S "
               f"ON {on} WHEN MATCHED THEN UPDATE SET {updates} "
               f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})")
        run_cmd(query_prefix + [sql], "MERGE idempotente a Bronze")
    finally:
        run_cmd(query_prefix + [f"DROP TABLE IF EXISTS `{staging}`"], "Limpiar staging")


def ejecutar_ingesta_batch_gcp(upload=False, input_dir=None, num_trips=600):
    if input_dir:
        directory = Path(input_dir)
        files = [directory / n for n in (
            "censo_demografia_ine_datalandia.csv", "uber_movement_speeds_datalandia.csv",
            "nyc_tlc_trips_lumina_multimodal.csv")]
        if not all(p.is_file() for p in files):
            raise FileNotFoundError("Se requieren los tres CSV con esquema canónico")
    else:
        print("Generando datos SINTÉTICOS para demostración; no son descargas de NYC/Uber/INE.")
        files = [generar_censo_demografia_ine(), generar_uber_movement_speeds(),
                 generar_nyc_tlc_trips_multimodal(num_trips=num_trips)]
    if not upload:
        print("Archivos locales listos. No se escribió en GCP. Usar --upload para cargar mediante MERGE.")
        return
    for path, table, keys in zip(files,
        [config.BQ_TABLE_CENSO_INE, config.BQ_TABLE_UBER_SPEEDS, config.BQ_TABLE_TLC_TRIPS],
        [("zone_id",), ("origin_zone_id", "dest_zone_id", "travel_date", "hod"), ("trip_id",)]):
        cargar_csv_sin_reemplazar(path, table, keys)
    print("Ingesta completada; tablas Bronze preservadas mediante MERGE.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generación sintética local o carga incremental segura")
    parser.add_argument("--upload", action="store_true", help="Cargar en GCP mediante staging + MERGE")
    parser.add_argument("--input-dir", help="Usar tres CSV canónicos existentes, sin generar datos")
    parser.add_argument("--num-trips", type=int, default=600)
    args = parser.parse_args()
    if args.num_trips <= 0:
        parser.error("--num-trips debe ser positivo")
    ejecutar_ingesta_batch_gcp(args.upload, args.input_dir, args.num_trips)
