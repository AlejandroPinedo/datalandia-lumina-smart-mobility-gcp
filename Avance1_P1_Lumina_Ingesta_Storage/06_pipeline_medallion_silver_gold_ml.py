"""Silver -> auditoría SQL y bloqueo -> Gold -> caché. No despliega Dataplex ni entrena ML."""
from datetime import datetime, timezone
import json
import config
from pipeline_sql import table, build_silver_sql, build_dq_sql, build_quality_gate_sql, build_gold_sql


def ejecutar_sql(client, titulo, sql):
    print(f"Ejecutando: {titulo}")
    client.query(sql).result()


def construir_capas_silver_y_gold():
    from google.cloud import bigquery
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    for title, builder in (("Bronze -> Silver", build_silver_sql),
                           ("Auditoría SQL", build_dq_sql),
                           ("Compuerta de calidad", build_quality_gate_sql),
                           ("Silver -> Gold", build_gold_sql)):
        ejecutar_sql(client, title, builder())
    exportar_cache_frontend(client)


def exportar_cache_frontend(bq_client):
    p = config.GCP_PROJECT_ID
    g_ds = f"`{p}.{config.BQ_DATASET_GOLD}`"
    s_ds = f"`{p}.{config.BQ_DATASET_SILVER}`"
    b_ds = f"`{p}.{config.BQ_DATASET_BRONZE}`"

    # 1. KPIs por modo de transporte
    q_modos = f"""
    SELECT
      transport_mode,
      COUNT(*) AS total_viajes,
      ROUND(AVG(lumina_cost_per_km_usd), 3) AS avg_cost_per_km_usd,
      ROUND(AVG(lumina_speed_kmh), 1) AS avg_speed_kmh,
      ROUND(AVG(lumina_duration_min), 1) AS avg_duration_min,
      ROUND(AVG(passenger_count), 1) AS avg_occupancy_pax
    FROM {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_FACT_TRIPS)}
    GROUP BY transport_mode
    ORDER BY avg_cost_per_km_usd ASC
    """
    modos_rows = [dict(r) for r in bq_client.query(q_modos).result()]

    # 2. KPIs por Condado (8 Condados)
    q_condados = f"""
    SELECT *
    FROM {table(config.BQ_DATASET_GOLD, config.BQ_GOLD_KPI_CONDADOS)}
    ORDER BY indice_prioridad_primera_milla DESC
    """
    condados_rows = [dict(r) for r in bq_client.query(q_condados).result()]

    # 3. Últimos eventos IoT y alertas PdM
    q_iot = f"""
    SELECT
      event_id,
      CAST(timestamp_evento AS STRING) AS timestamp_evento,
      node_id,
      zone_id,
      county_id,
      station_id,
      asset_type,
      vibration_mms,
      temperature_c,
      sound_db,
      passenger_load_pct,
      sensor_status,
      requiere_mantenimiento_pdm
    FROM {table(config.BQ_DATASET_SILVER, config.BQ_SILVER_IOT)}
    ORDER BY timestamp_evento DESC
    LIMIT 15
    """
    iot_rows = [dict(r) for r in bq_client.query(q_iot).result()]

    # 4. Resumen Dead-Letter Queue (DLQ)
    q_dlq = f"""
    SELECT
      origen_fuente,
      motivo_error,
      COUNT(*) AS total_aislados
    FROM {table(config.BQ_DATASET_BRONZE, config.BQ_TABLE_DEADLETTER)}
    GROUP BY origen_fuente, motivo_error
    ORDER BY total_aislados DESC
    """
    dlq_rows = [dict(r) for r in bq_client.query(q_dlq).result()]

    cache_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": "bigquery_export_source_may_be_synthetic",
        "gcp_project_id": config.GCP_PROJECT_ID,
        "datasets": {
            "bronze": config.BQ_DATASET_BRONZE,
            "silver": config.BQ_DATASET_SILVER,
            "gold": config.BQ_DATASET_GOLD,
            "audit": config.BQ_DATASET_AUDIT,
        },
        "modes_benchmark": modos_rows,
        "counties_kpis": condados_rows,
        "recent_iot_telemetry": iot_rows,
        "dlq_summary": dlq_rows,
    }

    config.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(config.CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_payload, f, indent=2, ensure_ascii=False, default=str)
    print(f"   💾 Caché Gold exportada en: {config.CACHE_FILE}")


if __name__ == "__main__":
    construir_capas_silver_y_gold()
