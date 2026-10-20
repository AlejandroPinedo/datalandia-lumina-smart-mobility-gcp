# 🏙️ Proyecto DataLandia (Lumina): Capa de Ingesta y Storage en GCP (P1)

Este directorio contiene la implementación práctica del **Proyecto Parte 1 (P1): Blueprint y Diseño de Ingesta / Almacenamiento** en **Google Cloud Platform (GCP)** para la Autoridad de Tránsito Unificada **Lumina**.

📄 **Documento de Diseño y Arquitectura Completo:**
👉 [Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md](../Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md)

---

## 📂 Estructura de Archivos

| Archivo | Propósito |
| :--- | :--- |
| [`.env`](./.env) | Configuración central (Proyecto GCP, Tópicos Pub/Sub, Bucket GCS y Tablas Medallion en BigQuery). |
| [`config.py`](./config.py) | Catálogo maestro de los **8 Condados de DataLandia**, estaciones **OART** y esquemas de las 4 fuentes + DLQ. |
| [`01_setup_infraestructura_lumina.py`](./01_setup_infraestructura_lumina.py) | Crea automáticamente el Tópico/Suscripción en **Cloud Pub/Sub**, el Bucket en **Cloud Storage** y los 4 Datasets Medallion + 5 Tablas Particionadas/Clusterizadas en **BigQuery**. |
| [`02_ingesta_batch_multifuente.py`](./02_ingesta_batch_multifuente.py) | Genera y sube al Data Lake GCS + BigQuery Bronze las 3 fuentes batch homologadas: **NYC TLC Trips (F1)**, **SF Uber Movement 24 km/h (F2)** y **Censo Demográfico INE (F4)**. |
| [`03_simulador_streaming_iot_aot_pubsub.py`](./03_simulador_streaming_iot_aot_pubsub.py) | Simula en tiempo real los sensores urbanos **Array of Things (AoT)** y la telemetría ferroviaria **OART (F3)** enviando eventos JSON a **Cloud Pub/Sub** (con inyección de fallas de sensores). |
| [`04_pipeline_streaming_beam_bq.py`](./04_pipeline_streaming_beam_bq.py) | Consume desde **Cloud Pub/Sub**, valida rangos físicos, escribe eventos válidos en `iot_aot_telemetry_raw` y aísla sensores defectuosos en `ingesta_deadletter_raw` (DLQ). |
| [`05_verificar_cruce_multifuente_bq.py`](./05_verificar_cruce_multifuente_bq.py) | Ejecuta en **BigQuery** los cruces (`JOIN`) de las 4 fuentes para responder a las 4 verticales de Lumina. |
| [`data_landing_local/`](./data_landing_local/) | Datasets CSV pre-generados listos para subir a GCS / BigQuery (`censo_demografia_ine_datalandia.csv`, `uber_movement_speeds_datalandia.csv`, `nyc_tlc_trips_lumina_multimodal.csv`). |

---

## 🚀 Orden de Ejecución en Consola / Cloud Shell

```bash
# 1. Revisar configuración GCP
python3 config.py

# 2. Crear infraestructura en GCP (Pub/Sub, GCS Data Lake y BigQuery Bronze/Silver/Gold)
python3 01_setup_infraestructura_lumina.py

# 3. Ejecutar Ingesta Batch Multifuente (NYC TLC + Uber Movement + Censo INE)
python3 02_ingesta_batch_multifuente.py

# 4. Publicar eventos Streaming IoT (Array of Things + Trenes OART) en Cloud Pub/Sub
python3 03_simulador_streaming_iot_aot_pubsub.py --max-messages 30 --interval 0.2

# 5. Consumir Streaming y cargar en BigQuery + Dead-Letter Queue
python3 04_pipeline_streaming_beam_bq.py

# 6. Consultar el cruce de las 4 fuentes en BigQuery
python3 05_verificar_cruce_multifuente_bq.py
```
