#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
05_verificar_cruce_multifuente_bq.py
Demuestra en vivo en Google BigQuery la estrategia de Integración Multifuente
(Proyecto Parte 1 - P1): une mediante SQL las 4 fuentes heterogéneas cargadas
en la Capa Bronze (`tlc_trips_raw`, `uber_movement_speeds_raw`, `iot_aot_telemetry_raw`,
`censo_demografia_raw`) usando las llaves maestras (`zone_id`, `county_id`, `hod`, `station_id`)
para responder a las 4 Verticales de Negocio de Lumina.
"""

from google.cloud import bigquery
import config


def ejecutar_consulta(bq_client: bigquery.Client, titulo: str, sql: str):
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    RESET = "\033[0m"
    print(f"\n{BLUE}================================================================================{RESET}")
    print(f"{GREEN}📊 {titulo}{RESET}")
    print(f"{BLUE}================================================================================{RESET}")
    job = bq_client.query(sql)
    rows = list(job.result())
    if not rows:
        print("  (Sin resultados)")
        return
    headers = list(rows[0].keys())
    print(" | ".join(f"{h:<24}" for h in headers))
    print("-" * (27 * len(headers)))
    for r in rows:
        vals = [str(r[h]) for h in headers]
        print(" | ".join(f"{v:<24}" for v in vals))


def verificar_cruces_multifuente():
    config.mostrar_resumen_config()
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    ds = f"`{config.GCP_PROJECT_ID}.{config.BQ_DATASET_BRONZE}`"

    # Consulta 1: Cruce F1 (NYC TLC) + F2 (Uber Movement) -> Eficiencia Lumina vs Auto Personal en Hora Punta (24 km/h)
    sql_v3_olap = f"""
    SELECT
      t.transport_mode AS modo_transporte,
      COUNT(t.trip_id) AS total_viajes,
      ROUND(AVG(t.cost_per_km_usd), 3) AS costo_prom_km_usd,
      ROUND(AVG(t.effective_speed_kmh), 1) AS velocidad_lumina_kmh,
      ROUND(AVG(u.avg_speed_kmh), 1) AS velocidad_calle_uber_kmh,
      ROUND(AVG(t.passenger_count), 1) AS ocupacion_prom_pax
    FROM {ds}.{config.BQ_TABLE_TLC_TRIPS} t
    LEFT JOIN {ds}.{config.BQ_TABLE_UBER_SPEEDS} u
      ON t.origin_zone_id = u.origin_zone_id
     AND t.dest_zone_id = u.dest_zone_id
     AND t.hod = u.hod
    GROUP BY t.transport_mode
    ORDER BY costo_prom_km_usd ASC
    """
    ejecutar_consulta(
        bq_client,
        "CRUCE 1 (F1 TLC + F2 Uber Movement): Costo/km y Velocidad Lumina vs. Vehículo Personal (24 km/h)",
        sql_v3_olap,
    )

    # Consulta 2: Cruce F4 (Censo INE) + F1 (Viajes TLC) + F3 (IoT/OART) -> Planes Diarios y Brecha Primera Milla
    sql_v2_demografia = f"""
    WITH viajes_por_zona AS (
      SELECT
        origin_zone_id,
        COUNT(*) AS viajes_originados,
        ROUND(AVG(duration_minutes), 1) AS duracion_prom_min
      FROM {ds}.{config.BQ_TABLE_TLC_TRIPS}
      GROUP BY origin_zone_id
    ),
    iot_por_zona AS (
      SELECT
        zone_id,
        ROUND(AVG(passenger_load_pct), 1) AS ocupacion_oart_pct,
        COUNTIF(requiere_mantenimiento_pdm) AS alertas_pdm_activas
      FROM {ds}.{config.BQ_TABLE_IOT_AOT}
      GROUP BY zone_id
    )
    SELECT
      c.county_name AS condado,
      c.zone_id AS zona,
      c.zone_type AS perfil_demografico,
      c.pct_adulto_mayor_65 AS pct_adulto_mayor,
      c.pct_jovenes_18_34 AS pct_jovenes,
      c.pct_menos_8km_estacion_oart AS cob_oart_8km_pct,
      IFNULL(v.viajes_originados, 0) AS viajes_tlc,
      IFNULL(i.ocupacion_oart_pct, 0.0) AS carga_tren_oart_pct,
      IFNULL(i.alertas_pdm_activas, 0) AS alertas_pdm_oart
    FROM {ds}.{config.BQ_TABLE_CENSO_INE} c
    LEFT JOIN viajes_por_zona v ON c.zone_id = v.origin_zone_id
    LEFT JOIN iot_por_zona i ON c.zone_id = i.zone_id
    ORDER BY c.indice_prioridad_primera_milla DESC
    """
    ejecutar_consulta(
        bq_client,
        "CRUCE 2 (F4 Censo INE + F1 TLC + F3 IoT AoT): Integración de las 4 Fuentes en los 8 Condados",
        sql_v2_demografia,
    )

    # Consulta 3: Auditoría de Resiliencia (Dead-Letter Queue de Sensores OART fallidos)
    sql_dlq = f"""
    SELECT
      origen_fuente,
      motivo_error,
      COUNT(*) AS eventos_aislados
    FROM {ds}.{config.BQ_TABLE_DEADLETTER}
    GROUP BY origen_fuente, motivo_error
    ORDER BY eventos_aislados DESC
    """
    ejecutar_consulta(
        bq_client,
        "AUDITORÍA DE FIABILIDAD (Dead-Letter Queue): Sensores OART Fallidos Aislados sin Caída del Sistema",
        sql_dlq,
    )


if __name__ == "__main__":
    verificar_cruces_multifuente()
