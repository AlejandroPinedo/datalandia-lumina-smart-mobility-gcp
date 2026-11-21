#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Spark real para Silver Parquet o demo Python explícita.
Gold multifuente se construye mediante el pipeline SQL, no mediante este job."""

import argparse
import os
import sys
import hashlib
from datetime import datetime

# Intentar importar PySpark y verificar si Java Runtime está disponible
PYSPARK_INITIALIZED = False
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, TimestampType, BooleanType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


def inicializar_spark(app_name: str = "Lumina-P2-Spark-Medallion"):
    """Inicializa Spark real. Si falta el runtime retorna None; no activa un fallback."""
    if not PYSPARK_AVAILABLE:
        return None

    try:
        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "20")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(f"🚀 Sesión de Apache Spark inicializada: {spark.version} (App: {app_name})")
        return spark
    except Exception as exc:
        print(f"ℹ️  Aviso de Entorno: Java Runtime no detectado en el host local ({exc}).")
        print("   Spark no se ejecutó. Use --mode=demo-python solo para la demostración explícita.")
        print("   (En Google Cloud Dataproc se ejecuta de forma nativa en el clúster con JVM administrada).")
        return None


# ==============================================================================
# 1. ESQUEMAS DE INGESTA BRONZE
# ==============================================================================

def esquema_tlc_bronze():
    return StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("modo_transporte", StringType(), True),
    ])


# ==============================================================================
# 2. TRANSFORMACIONES SPARK NATIVAS
# ==============================================================================

def transformar_bronze_a_silver_trips(spark, df_bronze_trips):
    print("🧹 [Spark Silver] Limpiando, deduplicando con Window Functions y enriqueciendo...")
    df_parsed = (
        df_bronze_trips
        .withColumn("ts_recogida", F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ts_llegada", F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("distancia_km", F.round(F.col("trip_distance") * 1.60934, 2))
        .withColumn("duracion_min", F.round((F.unix_timestamp("ts_llegada") - F.unix_timestamp("ts_recogida")) / 60.0, 2))
        .withColumn("zona_origen_id", F.col("PULocationID"))
        .withColumn("zona_destino_id", F.col("DOLocationID"))
        .withColumn("costo_total_usd", F.round(F.col("total_amount"), 2))
        .withColumn("modo_transporte", F.coalesce(F.col("modo_transporte"), F.lit("TAXIS_PRIVADOS")))
    )

    df_valid = df_parsed.filter(
        (F.col("ts_recogida").isNotNull()) &
        (F.col("ts_llegada").isNotNull()) &
        (F.col("duracion_min") > 1.0) &
        (F.col("duracion_min") < 240.0) &
        (F.col("distancia_km") > 0.1) &
        (F.col("distancia_km") < 150.0) &
        (F.col("costo_total_usd") > 0.0)
    )

    # Deduplicación por ventana
    dedup_window = Window.partitionBy("zona_origen_id", "zona_destino_id", "ts_recogida", "costo_total_usd").orderBy("ts_llegada")
    df_dedup = df_valid.withColumn("row_num", F.row_number().over(dedup_window)).filter(F.col("row_num") == 1).drop("row_num")

    hora_col = F.hour("ts_recogida")
    periodo_expr = (
        F.when((hora_col >= 7) & (hora_col <= 9), "PICO_AM")
        .when((hora_col >= 17) & (hora_col <= 19), "PICO_PM")
        .when((hora_col >= 22) | (hora_col <= 5), "NOCHE")
        .otherwise("VALLE")
    )

    return (
        df_dedup
        .withColumn("hora_viaje", hora_col)
        .withColumn("fecha_viaje", F.to_date("ts_recogida"))
        .withColumn("periodo_dia", periodo_expr)
        .withColumn("velocidad_efectiva_kmh", F.round(F.col("distancia_km") / (F.col("duracion_min") / 60.0), 2))
        .withColumn("usuario_hash_pseudonimizado", F.sha2(F.concat(F.col("zona_origen_id"), F.lit("_"), F.col("ts_recogida")), 256))
    )


# ==============================================================================
# 3. MODO DE DEMOSTRACIÓN COMPATIBLE LOCAL (PYTHON / SQL)
# ==============================================================================

def ejecutar_simulacion_spark_local(landing_dir: str):
    """Demo Python limitada a 20,000 filas. No reproduce ni acredita ejecución Spark."""
    import csv

    print("\nDEMO PYTHON: no ejecuta Spark, no certifica AQE ni cómputo distribuido.")
    path_tlc = os.path.join(landing_dir, "nyc_tlc_trips_lumina_multimodal.csv")
    path_uber = os.path.join(landing_dir, "uber_movement_speeds_datalandia.csv")
    path_censo = os.path.join(landing_dir, "censo_demografia_ine_datalandia.csv")

    # 1. Censo Bronze
    censo_rows = []
    with open(path_censo, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            censo_rows.append({
                "zone_id": r.get("zone_id", "ZDL-001"),
                "county_id": r.get("county_id", "CNT-01"),
                "county_name": r.get("county_name", "Condado Central"),
                "poblacion_total": int(r.get("poblacion_residente", 1000000)),
                "ingreso_medio_usd": float(r.get("ingreso_medio_hogar_usd", 4500.0)),
                "nearest_oart_station_id": r.get("nearest_oart_station_id", "OART-ST-01")
            })

    # 2. Uber Speeds Bronze
    uber_lookup = {}
    with open(path_uber, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            src = r["origin_zone_id"]
            dst = r["dest_zone_id"]
            hour = int(r.get("hod", r.get("hour", 0)))
            speed = float(r.get("avg_speed_kmh", r.get("speed_kmh_mean", 24.0)))
            uber_lookup[(src, dst, r["travel_date"], hour)] = speed

    # 3. Ingesta, Deduplicación y Enriquecimiento de Viajes
    total_leidos = 0
    viajes_validos = []
    vistos_dedup = set()

    with open(path_tlc, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            total_leidos += 1
            if total_leidos > 20000:  # Muestra para validación inmediata local
                break
            try:
                p_uid = int(r.get("pulocation_id") or r.get("PULocationID", 1))
                d_uid = int(r.get("dolocation_id") or r.get("DOLocationID", 2))
                
                # Distancia en km
                if "trip_distance_km" in r:
                    dist_km = float(r["trip_distance_km"])
                else:
                    dist_km = round(float(r.get("trip_distance", 1.0)) * 1.60934, 2)
                    
                # Costo
                costo = float(r.get("total_fare_usd") or r.get("total_amount", 5.0))
                
                pickup_str = r.get("pickup_datetime") or r.get("tpep_pickup_datetime")
                dropoff_str = r.get("dropoff_datetime") or r.get("tpep_dropoff_datetime")

                # Deduplicación Window row_number() = 1
                dedup_key = (p_uid, d_uid, pickup_str, costo)
                if dedup_key in vistos_dedup:
                    continue
                vistos_dedup.add(dedup_key)

                # Parse de fechas y duración
                t1 = datetime.strptime(pickup_str, "%Y-%m-%d %H:%M:%S")
                t2 = datetime.strptime(dropoff_str, "%Y-%m-%d %H:%M:%S")
                dur_min = round((t2 - t1).total_seconds() / 60.0, 2)

                if dur_min <= 1.0 or dur_min >= 240.0 or dist_km <= 0.1 or costo <= 0.0:
                    continue

                vel_efectiva = round(dist_km / (dur_min / 60.0), 2)
                hora = t1.hour

                # Periodo
                if 7 <= hora <= 9:
                    periodo = "PICO_AM"
                elif 17 <= hora <= 19:
                    periodo = "PICO_PM"
                elif hora >= 22 or hora <= 5:
                    periodo = "NOCHE"
                else:
                    periodo = "VALLE"

                # Cruce con Uber Speeds
                vel_vial = uber_lookup.get((r["origin_zone_id"], r["dest_zone_id"], str(t1.date()), hora))
                tiempo_vial = (dist_km / vel_vial) * 60.0 if vel_vial else None
                ahorro = round(tiempo_vial - dur_min, 2) if tiempo_vial is not None else None
                supera_cuello = vel_efectiva > 24.0
                county_id = r.get("county_id") or f"CNT-{((p_uid - 1) % 8) + 1:02d}"

                # Hash SHA-256 seudonimizado
                user_hash = hashlib.sha256(f"{p_uid}_{pickup_str}".encode()).hexdigest()[:16]

                viajes_validos.append({
                    "county_id": county_id,
                    "periodo_dia": periodo,
                    "distancia_km": dist_km,
                    "duracion_min": dur_min,
                    "costo_usd": costo,
                    "velocidad_efectiva": vel_efectiva,
                    "ahorro_min": ahorro,
                    "supera_cuello": supera_cuello,
                    "user_hash": user_hash
                })
            except Exception:
                continue

    print(f"📥 [Bronze -> Silver] {total_leidos:,} registros evaluados.")
    print(f"   * Registros limpios y deduplicados: {len(viajes_validos):,}")
    print(f"   * Duplicados/anomalías descartados: {total_leidos - len(viajes_validos):,}")

    # 4. Agregación Gold: Data Mart Ejecutivo
    from collections import defaultdict
    kpis = defaultdict(lambda: {"count": 0, "dist": 0.0, "dur": 0.0, "vel": 0.0, "costo": 0.0, "ahorro": 0.0, "supera": 0})

    for v in viajes_validos:
        k = (v["county_id"], v["periodo_dia"])
        kpis[k]["count"] += 1
        kpis[k]["dist"] += v["distancia_km"]
        kpis[k]["dur"] += v["duracion_min"]
        kpis[k]["vel"] += v["velocidad_efectiva"]
        kpis[k]["costo"] += v["costo_usd"]
        kpis[k]["ahorro"] += v["ahorro_min"] or 0.0
        if v["supera_cuello"]:
            kpis[k]["supera"] += 1

    condados_dict = {c["county_id"]: c["county_name"] for c in censo_rows}

    print("\n[DEMO PYTHON] Resumen en memoria; no materializa una tabla Gold:")
    print("-" * 115)
    print(f"{'Condado':<22} | {'Período':<8} | {'Viajes':<8} | {'Dist(km)':<9} | {'Vel(km/h)':<10} | {'Costo/km':<10} | {'Ahorro(h)':<10} | {'% > 24 km/h'}")
    print("-" * 115)

    filas_mostradas = 0
    for (cid, per), stat in sorted(kpis.items()):
        c_name = condados_dict.get(cid, f"Condado {cid}")
        n = stat["count"]
        if n == 0:
            continue
        d_avg = stat["dist"] / n
        v_avg = stat["vel"] / n
        c_per_km = (stat["costo"] / stat["dist"]) if stat["dist"] > 0 else 0.0
        ahorro_h = stat["ahorro"] / 60.0
        pct_supera = (stat["supera"] / n) * 100.0

        print(f"{c_name:<22} | {per:<8} | {n:<8} | {d_avg:<9.2f} | {v_avg:<10.2f} | ${c_per_km:<9.3f} | {ahorro_h:<10.2f} | {pct_supera:.1f}%")
        filas_mostradas += 1
        if filas_mostradas >= 10:
            break
    print("-" * 115)


# ==============================================================================
# 4. PUNTO DE ENTRADA Y COMANDO DATAPROC
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Spark real o demo Python explícita")
    parser.add_argument("--mode", choices=["spark", "demo-python"], default="spark")
    parser.add_argument("--input", help="CSV canónico local o gs://... accesible desde Spark")
    parser.add_argument("--output", help="Directorio Parquet Silver; obligatorio para Spark")
    args = parser.parse_args()
    landing_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_landing_local")
    if args.mode == "demo-python":
        ejecutar_simulacion_spark_local(landing_dir)
        print("Demo Python finalizada. No se ejecutó Spark ni se escribió Gold.")
        return
    if not args.input or not args.output:
        parser.error("Spark requiere --input y --output; no reemplaza tablas BigQuery")
    spark = inicializar_spark()
    if spark is None:
        raise RuntimeError("Spark no disponible. Instale PySpark y Java o ejecute en Dataproc.")
    try:
        df = spark.read.option("header", "true").csv(args.input)
        # Adaptador del CSV canónico a los campos del transformador Spark.
        if "trip_distance_km" in df.columns:
            df = (df.withColumn("trip_distance", F.col("trip_distance_km").cast("double") / F.lit(1.60934))
                  .withColumn("tpep_pickup_datetime", F.col("pickup_datetime"))
                  .withColumn("tpep_dropoff_datetime", F.col("dropoff_datetime"))
                  .withColumn("PULocationID", F.col("pulocation_id"))
                  .withColumn("DOLocationID", F.col("dolocation_id"))
                  .withColumn("total_amount", F.col("total_fare_usd").cast("double"))
                  .withColumn("modo_transporte", F.col("transport_mode")))
        result = transformar_bronze_a_silver_trips(spark, df)
        result.write.mode("errorifexists").partitionBy("fecha_viaje").parquet(args.output)
        print("Spark Silver materializado en Parquet. Gold multifuente se construye con el pipeline SQL.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
