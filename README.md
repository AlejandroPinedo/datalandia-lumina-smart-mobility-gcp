<div style="background: linear-gradient(135deg, #E8F0FE 0%, #FCE8E6 35%, #FEF7E0 70%, #CEEAD6 100%); padding: 24px; border-radius: 16px; border-left: 8px solid #4285F4;">
  <h1 style="color: #1A73E8; margin: 0;">🏙️ Proyecto Final: Arquitectura Big Data — DataLandia Metropolitan (Lumina Smart Mobility)</h1>
  <p style="color: #3C4043; font-size: 15px; margin-top: 8px; margin-bottom: 0;">
    <b>Maestría en Ciencia de Datos / Big Data (UTEC)</b> &nbsp;|&nbsp;
    <b>Nube:</b> Google Cloud Platform (GCP) &nbsp;|&nbsp;
    <b>Estándares:</b> Open Source (Apache Beam, Iceberg, Parquet)
  </p>
</div>

---

## 📌 Descripción General del Proyecto

Diseño e implementación de la plataforma analítica End-to-End en **Google Cloud Platform (GCP)** para la **Autoridad de Tránsito Unificada "Lumina"** en la región metropolitana de **DataLandia** (9.25 millones de habitantes, 8 condados y 12 agencias de transporte unificadas).

La arquitectura integra y cruza **4 fuentes de datos heterogéneas** (Streaming IoT + Batch histórico/demográfico) para superar el cuello de botella vial de **24 km/h (15 mph)** en hora punta y demostrar que el transporte multimodal conectado a la red ferroviaria **OART** (con cobertura del 89% a menos de 8 km) es **más rápido y económico por kilómetro que conducir un vehículo particular**.

---

## 📂 Estructura del Repositorio

| Ruta / Entregable | Descripción |
| :--- | :--- |
| 📄 **[`Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md`](./Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md)** | **Documento Maestro — Entrega 1 (Proyecto Parte 1 - P1):** Estrategia de integración multifuente, modelo Entidad-Relación y llaves canónicas (`zone_id`, `county_id`, `station_id`, `hod`), diagrama de arquitectura End-to-End en GCP y diseño físico de Ingesta y Storage (Lakehouse Medallion + NoSQL operacional). |
| 💻 **[`Avance1_P1_Lumina_Ingesta_Storage/`](./Avance1_P1_Lumina_Ingesta_Storage/)** | **Implementación en GCP (Python + Cloud Pub/Sub + Cloud Storage + BigQuery):** Scripts automatizados para aprovisionar infraestructura, ingestar las 3 fuentes Batch (*NYC TLC*, *Uber Movement 24 km/h*, *Censo INE*), simular y procesar en Streaming los sensores *Array of Things (AoT) + OART* con aislamiento en *Dead-Letter Queue (DLQ)*, y consultar el cruce (`JOIN`) de las 4 fuentes en BigQuery. |
| 🎨 **[`Visualizaciones_Archify/`](./Visualizaciones_Archify/)** | **Diagramas Interactivos Standalone HTML (generados con Archify v3.2):**<br/>• [`00_Lumina_Avance1_P1_Explainer_Interactivo.html`](./Visualizaciones_Archify/00_Lumina_Avance1_P1_Explainer_Interactivo.html) *(One-Page Explainer Ejecutivo)*<br/>• [`01_lumina_arquitectura_end_to_end.html`](./Visualizaciones_Archify/01_lumina_arquitectura_end_to_end.html) *(Arquitectura End-to-End en GCP)*<br/>• [`02_lumina_dataflow_multifuente.html`](./Visualizaciones_Archify/02_lumina_dataflow_multifuente.html) *(Pipeline Dataflow & Linaje Multifuente)*<br/>• [`03_lumina_erd_modelo_datos.html`](./Visualizaciones_Archify/03_lumina_erd_modelo_datos.html) *(Modelo Físico ERD y Llaves de Cruce en BigQuery)* |
| 📋 **[`Taller de Arquitectura BigDAta DataLandia  - Transporte y Movilidad Urbana (Smart Mobility).docx`](./Taller%20de%20Arquitectura%20BigDAta%20DataLandia%20%20-%20Transporte%20y%20Movilidad%20Urbana%20(Smart%20Mobility).docx)** | Caso de estudio oficial de DataLandia y requerimientos del RFP de Lumina. |
| 📑 **[`UTEC - Desarrollo de Proyecto - Seccion 1.pdf`](./UTEC%20-%20Desarrollo%20de%20Proyecto%20-%20Seccion%201.pdf)** | Rúbrica y cronograma de las 3 entregas progresivas del curso. |

---

## 🚀 Ejecución Rápida de la Capa de Ingesta y Storage (Avance 1 - P1)

```bash
cd Avance1_P1_Lumina_Ingesta_Storage

# 1. Verificar parámetros del proyecto GCP y catálogo de los 8 condados
python3 config.py

# 2. Aprovisionar tópicos Pub/Sub, bucket GCS Data Lake y tablas particionadas en BigQuery
python3 01_setup_infraestructura_lumina.py

# 3. Ejecutar Ingesta Batch Multifuente (NYC TLC + Uber Movement + Censo INE) -> GCS & BigQuery
python3 02_ingesta_batch_multifuente.py

# 4. Publicar telemetría IoT en tiempo real (Array of Things + Trenes OART) en Cloud Pub/Sub
python3 03_simulador_streaming_iot_aot_pubsub.py --max-messages 30 --interval 0.2

# 5. Consumir Streaming hacia BigQuery Bronze + aislamiento de sensores rotos en Dead-Letter Queue
python3 04_pipeline_streaming_beam_bq.py

# 6. Ejecutar el cruce SQL (JOIN) de las 4 fuentes en BigQuery
python3 05_verificar_cruce_multifuente_bq.py
```
