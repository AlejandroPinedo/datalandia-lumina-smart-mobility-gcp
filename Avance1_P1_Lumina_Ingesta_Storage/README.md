# 🏙️ Plataforma Big Data E2E "Lumina" — DataLandia Metropolitan (P1, P2 y P3)

**Curso:** Big Data — Maestría en Ciencia de Datos e Inteligencia Artificial (UTEC) — Sección 1 (Grupo 7)  
**Caso de Negocio:** Autoridad de Tránsito Unificada **Lumina** (8 Condados, 12 Agencias de Transporte, 9.25 Millones de Residentes, Red Ferroviaria OART).

---

## 🛡️ Plan de Protección de Presupuesto GCP ($250 USD en Créditos)

Toda la implementación ha sido auditada y diseñada bajo una estrategia **100% Serverless / Scale-to-Zero** para que el despliegue completo y la demostración en vivo consuman **menos de $0.50 USD** de tus $250 USD de crédito:

| Componente | Servicio en Arquitectura | Modo de Ejecución Económico (< $0.50 USD) | Ahorro frente a Cluster 24/7 |
| :--- | :--- | :--- | :--- |
| **Data Lake** | Cloud Storage (`Standard`) | Lotes CSV/JSON (~15 MB) en capa gratuita regional | Evita costos de transferencia multi-región |
| **Streaming Bus** | Cloud Pub/Sub | Pull/Push bajo demanda (< 10 MB de mensajes IoT) | $0.00 USD (Free Tier 10 GB/mes) |
| **Stream Processing** | Dataflow (Apache Beam) | Soporta `--mode=pull` ($0.00 instantáneo) o `DirectRunner` / `DataflowRunner` por 5 min | Evita dejar VMs `n1-standard` encendidas 24/7 |
| **Lakehouse (Bronze/Silver/Gold)** | BigQuery | Tablas particionadas y clusterizadas (`< 50 MB` escaneados) | $0.00 USD (Free Tier 1 TB consulta / 10 GB storage) |
| **Orquestación Medallion** | Managed Airflow | Incluye DAG oficial de Airflow 2.x + **Ejecutor Directo Serverless** (`python3 07_...py`) | **Ahorra ~$300/mes** de cluster GKE de Cloud Composer |
| **Front-End & API BFF (P3)** | Cloud Run / Local | `deploy_cloud_run.sh` configurado con `--min-instances 0 --max-instances 2` | $0.00 USD en reposo (Scale-to-Zero) |

---

## 📂 Estructura de Scripts Python (Entregas P1, P2 y P3)

```text
Avance1_P1_Lumina_Ingesta_Storage/
├── config.py                                    # Configuración centralizada (Bronze, Silver, Gold, Audit, Zonas)
├── requirements.txt                             # Dependencias GCP + Apache Beam + Flask + Gunicorn
├── Dockerfile                                   # Contenedor ligero Python 3.11 para Cloud Run (P3)
├── deploy_cloud_run.sh                          # Despliegue Serverless en Cloud Run (--min-instances 0)
├── 01_setup_infraestructura_lumina.py           # [P1] Crea Bucket GCS, Topics/DLQ Pub/Sub y 4 Datasets BigQuery
├── 02_ingesta_batch_multifuente.py              # [P1] Genera y carga TLC Trips, Uber Movement (24 km/h) y Censo INE
├── 03_simulador_streaming_iot_aot_pubsub.py     # [P1] Publica telemetría IoT Array of Things + OART en Pub/Sub
├── 04_pipeline_streaming_beam_bq.py             # [P1/P2] Pipeline Streaming (Pull Serverless o Apache Beam DoFn + DLQ)
├── 05_verificar_cruce_multifuente_bq.py         # [P1] Validación de volumetría y cruce inicial de las 4 fuentes
├── 06_pipeline_medallion_silver_gold_ml.py      # [P2] Transformación Bronze->Silver (QUALIFY, GIS, DLP) y Silver->Gold (Estrella)
├── 07_dag_managed_airflow_composer.py           # [P2] DAG Oficial de Managed Airflow + Ejecutor Local Serverless ($0.00)
├── 08_app_exposicion_frontend_lumina.py         # [P3] Servidor Web y API REST BFF (Simulador de Rutas + Telemetría + DLQ)
├── dashboard_operativo_lumina_en_vivo.html      # [P3] Versión standalone interactiva del Centro de Control Lumina
└── templates/
    └── lumina_dashboard.html                    # [P3] Plantilla HTML5/JS con paleta Google Cloud + Pasteles
```

---

## 🚀 Ejecución Paso a Paso (End-to-End)

### 1. Configurar Credenciales y Variables (`.env`)
Verifica tu archivo `.env` con tu `GCP_PROJECT_ID` (ej. `gobiernodatos`) y autentícate:
```bash
gcloud auth application-default login
```

### 2. Ejecutar Entrega 1 (P1: Ingesta Batch + Streaming + Almacenamiento Bronze)
```bash
python3 01_setup_infraestructura_lumina.py
python3 02_ingesta_batch_multifuente.py
python3 03_simulador_streaming_iot_aot_pubsub.py
python3 04_pipeline_streaming_beam_bq.py --mode=pull
python3 05_verificar_cruce_multifuente_bq.py
```

### 3. Ejecutar Entrega 2 (P2: Transformación Medallion Silver/Gold, Knowledge Catalog DQ y Orquestación Managed Airflow)
```bash
# Ejecuta Bronze -> Silver (Deduplicación QUALIFY, GIS ST_GEOGPOINT, DLP SHA256),
# valida SLA >= 95% (Knowledge Catalog) y construye el Esquema Estrella en Gold:
python3 07_dag_managed_airflow_composer.py
```

### 4. Ejecutar Entrega 3 (P3: Front-End Operativo, Simulador de Rutas 24 km/h y API REST)
```bash
# Levanta el servidor local en http://127.0.0.1:8080 (funciona con Flask o con el servidor nativo de Python):
python3 08_app_exposicion_frontend_lumina.py
```
O abre directamente [`dashboard_operativo_lumina_en_vivo.html`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/dashboard_operativo_lumina_en_vivo.html) en tu navegador, o despliega en Cloud Run con `./deploy_cloud_run.sh`.
