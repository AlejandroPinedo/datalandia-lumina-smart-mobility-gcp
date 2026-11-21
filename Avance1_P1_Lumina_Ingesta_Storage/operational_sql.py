"""Baselines interpretables para planificación y mantenimiento; requieren validación en GCP.

No asignan vehículos, no emiten órdenes de trabajo ni estiman probabilidades de falla.
"""
import config


def build_operational_sql():
    prefix = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_GOLD}"
    silver = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_SILVER}"
    return f"""
    -- Pronóstico de mañana: promedio del mismo día semanal de las cuatro semanas previas.
    -- Días ausentes NO se imputan como cero: solo cierres diarios completos permiten hacerlo.
    CREATE OR REPLACE TABLE `{prefix}.plan_diario_recursos_gold` AS
    WITH diarios AS (
      SELECT origin_zone_id, DATE(pickup_datetime) AS dia, COUNT(*) AS viajes,
        AVG(passenger_count) AS pasajeros_por_viaje
      FROM `{silver}.{config.BQ_SILVER_TRIPS}`
      WHERE DATE(pickup_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        AND DATE(pickup_datetime) < CURRENT_DATE()
        AND transport_mode != 'PERSONAL_CAR_BENCHMARK'
      GROUP BY origin_zone_id, dia
    ), base AS (
      SELECT origin_zone_id, COUNT(*) AS dias_observados,
        COUNTIF(EXTRACT(DAYOFWEEK FROM dia) = EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))) AS dias_comparables,
        AVG(IF(EXTRACT(DAYOFWEEK FROM dia) = EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), viajes, NULL)) AS demanda_base
      FROM diarios GROUP BY origin_zone_id
    )
    SELECT z.zone_id, z.county_id, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AS fecha_plan,
      'PROMEDIO_ESTACIONAL_4_SEMANAS' AS metodo,
      IFNULL(b.dias_observados, 0) AS dias_observados,
      IFNULL(b.dias_comparables, 0) AS dias_comparables,
      IF(b.dias_comparables >= 2 AND b.dias_observados >= 14,
          CAST(CEIL(b.demanda_base) AS INT64), NULL) AS viajes_previstos,
      IF(b.dias_comparables >= 2 AND b.dias_observados >= 14,
          CAST(CEIL(b.demanda_base / {config.PLANNING_TRIPS_PER_VEHICLE_DAY}) AS INT64), NULL) AS vehiculos_equivalentes_requeridos,
      {config.PLANNING_TRIPS_PER_VEHICLE_DAY} AS supuesto_viajes_por_vehiculo_dia,
      CAST(NULL AS INT64) AS flota_disponible_no_integrada,
      CAST(NULL AS INT64) AS personal_disponible_no_integrado,
      CASE WHEN b.dias_comparables >= 2 AND b.dias_observados >= 14 THEN 'BASELINE_PENDIENTE_VALIDACION'
           ELSE 'HISTORIA_INSUFICIENTE' END AS estado,
      CURRENT_TIMESTAMP() AS generado_en
    FROM `{silver}.{config.BQ_SILVER_CENSO}` z LEFT JOIN base b ON z.zone_id = b.origin_zone_id;

    -- Tendencia lineal por activo en 24 horas; extrapolación exploratoria hasta umbral.
    CREATE OR REPLACE TABLE `{prefix}.mantenimiento_proactivo_gold` AS
    WITH muestras AS (
      SELECT *, UNIX_SECONDS(timestamp_evento) / 3600.0 AS hora
      FROM `{silver}.{config.BQ_SILVER_IOT}`
      WHERE timestamp_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND timestamp_evento <= CURRENT_TIMESTAMP()
    ), tendencias AS (
      SELECT node_id, COUNT(DISTINCT timestamp_evento) AS muestras,
        TIMESTAMP_DIFF(MAX(timestamp_evento), MIN(timestamp_evento), MINUTE) AS minutos_observados,
        SAFE_DIVIDE(COVAR_SAMP(hora, vibration_mms), VAR_SAMP(hora)) AS pendiente_vibracion_hora,
        SAFE_DIVIDE(COVAR_SAMP(hora, temperature_c), VAR_SAMP(hora)) AS pendiente_temperatura_hora
      FROM muestras GROUP BY node_id
    ), ultimos AS (
      SELECT * FROM muestras
      QUALIFY ROW_NUMBER() OVER(PARTITION BY node_id ORDER BY timestamp_evento DESC, event_id DESC) = 1
    ), proyeccion AS (
      SELECT u.node_id, u.station_id, u.zone_id, u.timestamp_evento, u.vibration_mms, u.temperature_c,
        t.muestras, t.minutos_observados, t.pendiente_vibracion_hora, t.pendiente_temperatura_hora,
        CASE WHEN u.vibration_mms >= 7.5 OR u.temperature_c >= 80 THEN 0.0
          WHEN t.muestras >= 3 AND t.minutos_observados >= 60 THEN
            (SELECT MIN(horas) FROM UNNEST([
              IF(t.pendiente_vibracion_hora > 0, SAFE_DIVIDE(7.5 - u.vibration_mms, t.pendiente_vibracion_hora), NULL),
              IF(t.pendiente_temperatura_hora > 0, SAFE_DIVIDE(80 - u.temperature_c, t.pendiente_temperatura_hora), NULL)
            ]) AS horas WHERE horas >= 0)
          ELSE NULL END AS horas_hasta_umbral
      FROM ultimos u JOIN tendencias t USING(node_id)
    )
    SELECT *, 'EXTRAPOLACION_LINEAL_NO_PROBABILIDAD_DE_FALLA' AS metodo,
      CASE WHEN timestamp_evento < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 'SENSOR_DESACTUALIZADO'
           WHEN horas_hasta_umbral = 0 THEN 'INSPECCION_PRIORITARIA'
           WHEN horas_hasta_umbral <= 24 THEN 'PROPONER_INSPECCION_EN_24H'
           WHEN muestras < 3 OR minutos_observados < 60 THEN 'HISTORIA_INSUFICIENTE'
           ELSE 'SEGUIMIENTO' END AS recomendacion,
      CURRENT_TIMESTAMP() AS generado_en
    FROM proyeccion;
    """
