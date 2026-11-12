<div style="background: linear-gradient(135deg, #E8F0FE 0%, #FCE8E6 35%, #FEF7E0 70%, #CEEAD6 100%); padding: 24px; border-radius: 16px; border-left: 8px solid #4285F4;">
  <h1 style="color: #1A73E8; margin: 0;">🏙️ Proyecto Final: Arquitectura Big Data — DataLandia Metropolitan (Lumina Smart Mobility)</h1>
  <p style="color: #3C4043; font-size: 15px; margin-top: 8px; margin-bottom: 0;">
    <b>Curso:</b> Big Data &nbsp;|&nbsp;
    <b>Universidad:</b> UTEC — Maestría en Ciencia de Datos e Inteligencia Artificial &nbsp;|&nbsp;
    <b>Grupo 7:</b> DataLandia<br>
    <b>Integrantes:</b> Herles Pinedo · David Jimenez · José Nomberto<br>
    <b>Nube:</b> Google Cloud Platform (proyecto <code>bigdata-herles-pinedo</code>, <code>us-central1</code>) &nbsp;|&nbsp;
    <b>Estándares:</b> Open Source (Apache Beam, Spark, Parquet, Iceberg, Airflow)
  </p>
</div>

---

## 📌 Descripción general

Plataforma analítica End-to-End en **Google Cloud Platform** para la **Autoridad de Tránsito Unificada "Lumina"** de **DataLandia** (9.25 millones de habitantes, 8 condados, 12 agencias de transporte).

La arquitectura integra y cruza **4 fuentes heterogéneas** (Streaming IoT + Batch histórico/demográfico) para superar el cuello de botella vial de **24 km/h** en hora punta y demostrar que el transporte multimodal conectado a la red ferroviaria **OART** es más rápido y más barato por kilómetro que el auto particular.

| Fuente | Tipo | Contenido |
| :--- | :---: | :--- |
| F1 · Viajes multimodales (NYC TLC adaptado) | Batch | ~5M viajes (84 shards Parquet en `raw/tlc_trips_5m/`) |
| F2 · Uber Movement Speeds | Batch | Velocidades por corredor y hora (cuello de 24 km/h) |
| F3 · Array of Things + OART | Streaming | Telemetría IoT vía Cloud Pub/Sub |
| F4 · Censo INE DataLandia | Batch | Demografía de los 8 condados |

---

## 📂 Entregables

| Entregable | Hito | Descripción |
| :--- | :---: | :--- |
| ⭐ **[`00_DataLandia_Lumina_Entrega2_Procesamiento_Transformacion.html`](./00_DataLandia_Lumina_Entrega2_Procesamiento_Transformacion.html)** | **Entrega 2 (P2 · 25%)** | **Presentable oficial de la Entrega 2. Un solo archivo autocontenido**, sin dependencias externas: carátula del grupo, matriz de cumplimiento de la rúbrica, **3 diagramas Archify interactivos embebidos** (arquitectura híbrida, pipeline multifuente y ERD), código fuente completo de los 6 scripts, diccionario de datos, fórmulas de negocio, auditoría de calidad DQ y **5 capturas reales de GCP** en Base64. |
| 📄 [`Avance2_P2_Procesamiento_Transformacion_Modelado_DataLandia.md`](./Avance2_P2_Procesamiento_Transformacion_Modelado_DataLandia.md) | Entrega 2 (P2) | Informe técnico P2: procesamiento híbrido, deduplicación `QUALIFY`, enriquecimiento geoespacial, cruce multifuente, Star Schema Gold y orquestación. |
| 🛠️ [`build_entrega2_html.py`](./build_entrega2_html.py) | Entrega 2 (P2) | Generador del HTML autocontenido (incrusta diagramas, scripts y capturas). |
| ⭐ [`PROYECTO_MASTER_DATALANDIA_LUMINA.html`](./PROYECTO_MASTER_DATALANDIA_LUMINA.html) | Entrega 1 (P1 + E) | Presentable interactivo del blueprint de ingesta, storage y examen de sustentación. |
| 📄 [`Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md`](./Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md) | Entrega 1 (P1) | Blueprint de ingesta y storage: dimensionamiento, DDL y DLQ. |
| 📄 [`Avance1_Examen_Sustentacion_Tecnica_Economica_Resiliencia.md`](./Avance1_Examen_Sustentacion_Tecnica_Economica_Resiliencia.md) | Examen (E) | Sustentación técnica, económica (FinOps) y de resiliencia (DRP). |
| 📘 [`GUIA_PASO_A_PASO_IMPLEMENTACION_LUMINA.md`](./GUIA_PASO_A_PASO_IMPLEMENTACION_LUMINA.md) | Soporte | Guía paso a paso de implementación en GCP. |
| 🖼️ [`Capturas de pantalla/`](./Capturas%20de%20pantalla/) | Evidencias | C1 (BigQuery), C2 (DQ), C3 (Pub/Sub), G4 (Cloud Storage Medallion), G5 (ejecución PySpark). |
| 🧭 [`Visualizaciones_Archify/`](./Visualizaciones_Archify/) | Diagramas | Visores interactivos originales de arquitectura, dataflow y ERD. |

---

## ✅ Entrega 2 (P2): Procesamiento, Transformación y Modelado

| Criterio de la rúbrica | Implementación | Scripts |
| :--- | :--- | :--- |
| **1. Pipelines de cómputo / transformación** | BigQuery SQL (Medallion Bronze ➔ Silver ➔ Gold), Apache Spark / PySpark (Dataproc, Window Functions + AQE) y Apache Beam (Dataflow) | `06_…`, `06b_…`, `04_…` |
| **2. Procesamiento híbrido** | Streaming de baja latencia (Pub/Sub ➔ Beam ➔ BigQuery + DLQ) y batch masivo (5M viajes) orquestado con Airflow / Cloud Composer | `03_…`, `04_…`, `07_…` |
| **3. Modelado para analítica** | Esquema estrella en `lumina_gold_governed`: `fact_movilidad_multimodal_gold` (4,998,926 filas, partición diaria por `travel_date`, cluster `county_id, origin_zone_id, transport_mode`), `dim_zonas_condados_gold` y `kpi_ejecutivo_condados_gold` | `06_…`, `05_…` |

**Resultados verificados en GCP:** 4,998,926 viajes en Silver/Gold · 59 particiones · 1.80 GB lógicos / 321.44 MB físicos · 100% de cumplimiento en las 3 reglas de calidad (SLA ≥ 95%) · Data Lake Medallion en `gs://bigdata-herles-pinedo-lumina-datalake/` (`landing/`, `raw/`, `bronze/`, `silver/`, `gold/` en Parquet).

---

## 💻 Código fuente (`Avance1_P1_Lumina_Ingesta_Storage/`)

| Script | Función |
| :--- | :--- |
| `01_setup_infraestructura_lumina.py` | Aprovisiona GCS, Pub/Sub (+DLQ), datasets y tablas de BigQuery |
| `02_ingesta_batch_multifuente.py` | Ingesta batch de TLC, Uber Movement y Censo INE a GCS y BigQuery |
| `03_simulador_streaming_iot_aot_pubsub.py` | Productor de telemetría IoT/OART hacia Pub/Sub |
| `04_pipeline_streaming_beam_bq.py` | Pipeline Apache Beam con validación física y Dead-Letter Queue |
| `05_verificar_cruce_multifuente_bq.py` | Verificación analítica del cruce de las 4 fuentes |
| `06_pipeline_medallion_silver_gold_ml.py` | Pipeline Medallion en BigQuery (Silver, DQ, Gold Star Schema, BQML) |
| `06b_pipeline_pyspark_dataproc_silver_gold.py` | Pipeline distribuido en PySpark para Dataproc Serverless |
| `07_dag_managed_airflow_composer.py` | DAG de orquestación en Cloud Composer / Airflow |
| `08_app_exposicion_frontend_lumina.py` | Demo web (Flask) de exposición de KPIs Gold |

---

## 🚀 Ejecución rápida

```bash
cd Avance1_P1_Lumina_Ingesta_Storage

# 1. Entorno e instalación de dependencias
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Infraestructura (GCS, Pub/Sub con DLQ, datasets y tablas BigQuery)
python3 01_setup_infraestructura_lumina.py

# 3. Ingesta batch (NYC TLC + Uber Movement 24 km/h + Censo INE)
python3 02_ingesta_batch_multifuente.py

# 4. Streaming IoT (Array of Things + OART) hacia Pub/Sub
python3 03_simulador_streaming_iot_aot_pubsub.py --max-messages 50 --interval 0.3

# 5. Pipeline streaming Beam ➔ BigQuery Bronze (+ DLQ)
python3 04_pipeline_streaming_beam_bq.py

# 6. Entrega 2: Medallion Bronze ➔ Silver ➔ Gold en BigQuery
python3 06_pipeline_medallion_silver_gold_ml.py

# 7. Entrega 2: pipeline distribuido PySpark (local o Dataproc Serverless)
python3 06b_pipeline_pyspark_dataproc_silver_gold.py
# gcloud dataproc batches submit pyspark 06b_pipeline_pyspark_dataproc_silver_gold.py --region=us-central1

# 8. Verificación del cruce de las 4 fuentes
python3 05_verificar_cruce_multifuente_bq.py

# 9. Demo web de KPIs (http://127.0.0.1:8080)
python3 08_app_exposicion_frontend_lumina.py
```

Para regenerar el presentable de la Entrega 2 (desde la raíz del repo):

```bash
python3 build_entrega2_html.py
```
