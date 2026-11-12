# 🚀 Guía Maestra Paso a Paso: Implementación End-to-End "Lumina" (DataLandia) en GCP

**Proyecto:** Plataforma Analítica Unificada de Movilidad Inteligente "Lumina" — Área Metropolitana de DataLandia  
**Curso:** Big Data — Maestría en Ciencia de Datos e Inteligencia Artificial (UTEC) — Sección 1 (Grupo 7)  
**Cobertura del Documento:** Cumplimiento íntegro del Feedback Docente + **Parte 1 (Ingesta y Storage)** + **Parte 2 (Procesamiento, Transformación Medallion y Modelado Estrella)** + **Parte 3 (Exposición API, Front-End Operativo y Sustentación de 8 Puntos)**.

---

## 💰 0. Estrategia de Protección de tu Crédito de $250 USD en Google Cloud (FinOps)

Para proteger tus **$250 USD de crédito en Google Cloud** y lograr que todo el despliegue y la demostración en vivo cuesten **menos de $0.50 USD en total**, hemos diseñado una modalidad de ejecución **100% Serverless y Scale-to-Zero**:

> [!IMPORTANT]
> **Reglas de Oro para NO quemar tus $250 USD de crédito:**
> 1. **NO levantes un entorno GKE de Cloud Composer (Managed Airflow) 24/7:** Un entorno pequeño de Cloud Composer enciende un cluster GKE permanente que consume **~$9 a $12 USD por día (~$300/mes)** aunque no ejecute nada. En su lugar, hemos creado el archivo [`07_dag_managed_airflow_composer.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/07_dag_managed_airflow_composer.py) con **doble capacidad**: contiene el código oficial del DAG de Airflow 2.x para mostrar en la revisión de código, y además cuenta con un **Ejecutor Serverless Directo** (`python3 07_dag_managed_airflow_composer.py`) que corre las 5 tareas del DAG directamente sobre BigQuery con **costo $0.00 USD**.
> 2. **Usa `--mode=pull` o `DirectRunner` en el Pipeline Streaming (`04_pipeline_streaming_beam_bq.py`):** El script [`04_pipeline_streaming_beam_bq.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/04_pipeline_streaming_beam_bq.py) incluye el pipeline completo en **Apache Beam (`DoFn` + `pvalue.TaggedOutput('deadletter')`)** y también el modo `--mode=pull` que drena Pub/Sub hacia BigQuery en 3 segundos sin cobrar horas de máquinas virtuales de Dataflow.
> 3. **Cloud Run con `--min-instances 0` (`deploy_cloud_run.sh`):** El microservicio web y API REST ([`08_app_exposicion_frontend_lumina.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/08_app_exposicion_frontend_lumina.py)) se despliega con escala a cero (`min-instances=0`), por lo que cuando nadie está usando el dashboard el costo es **$0.00 USD**.

### Presupuesto Real Estimado para tu Cuenta GCP ($250 USD Disponibles)

| Servicio GCP | Uso Real en el Proyecto | Nivel Gratuito GCP (Free Tier) | Costo Real en tu Cuenta |
| :--- | :--- | :--- | :---: |
| **Cloud Storage (`Standard`)** | ~15 MB de CSV/JSON (4 fuentes) | 5 GB / mes gratis | **$0.00 USD** |
| **Cloud Pub/Sub** | ~2,000 eventos IoT Array of Things + DLQ | 10 GB / mes gratis | **$0.00 USD** |
| **BigQuery (Storage + SQL)** | 4 Datasets (`bronze`, `silver`, `gold`, `audit`) < 50 MB | 10 GB storage + 1 TB queries / mes gratis | **$0.00 USD** |
| **Dataflow / Apache Beam** | Ejecución local `DirectRunner` o `--mode=pull` (opcional job 5 min) | $0.00 en modo local / Pull ($0.05 si lanzas job 5 min) | **$0.00 - $0.05 USD** |
| **Cloud Run (Front-End + API P3)** | Contenedor Flask/Gunicorn con `--min-instances 0` | 2 millones de peticiones / mes gratis | **$0.00 USD** |
| **TOTAL ACUMULADO DEL PROYECTO** | **Ejecución Completa P1 + P2 + P3** | **Cubierto por Free Tier / Serverless** | **< $0.10 USD** (Te quedan >$249.90 USD) |

---

## 📋 1. Inventario de los 8 Scripts Python Auditados y Listos para Desplegar

Todos los archivos se encuentran en [`Tareas/Avance1_P1_Lumina_Ingesta_Storage/`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage):

1. [`config.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/config.py): Parámetros globales, 8 condados/zonas de DataLandia con estaciones OART y nombres de tablas en Bronze, Silver, Gold y Audit.
2. [`01_setup_infraestructura_lumina.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/01_setup_infraestructura_lumina.py) (**Entrega 1 - P1**): Crea de forma idempotente el bucket de Cloud Storage, los tópicos de Pub/Sub (`lumina-iot-aot-telemetry-topic` + `lumina-iot-aot-dlq-topic`), los 4 datasets de BigQuery y las 5 tablas Bronze particionadas y clusterizadas.
3. [`02_ingesta_batch_multifuente.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/02_ingesta_batch_multifuente.py) (**Entrega 1 - P1**): Genera y sube a Cloud Storage y BigQuery Bronze los lotes de **NYC TLC Trips (F1)**, **Uber Movement Speeds con cuello de botella de 24 km/h (F2)** y **Censo Demográfico INE para los 8 condados y 9.25M de habitantes (F4)**.
4. [`03_simulador_streaming_iot_aot_pubsub.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/03_simulador_streaming_iot_aot_pubsub.py) (**Entrega 1 - P1**): Simulador en tiempo real de nodos urbanos **Array of Things (AoT) y sensores ferroviarios OART (F3)** que publica eventos JSON en Pub/Sub (incluyendo un ~6% de eventos corruptos para probar la cola Dead-Letter).
5. [`04_pipeline_streaming_beam_bq.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/04_pipeline_streaming_beam_bq.py) (**Entrega 1 y 2 - P1/P2**): Pipeline híbrido de Streaming que valida rangos físicos de vibración/temperatura, envía registros válidos a `iot_aot_telemetry_raw` y aísla registros corruptos en `ingesta_deadletter_raw` (soporta `--mode=pull` y `--mode=beam`).
6. [`05_verificar_cruce_multifuente_bq.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/05_verificar_cruce_multifuente_bq.py) (**Entrega 1 - P1**): Verifica conteos en la capa Bronze y ejecuta la primera consulta federada de validación.
7. [`06_pipeline_medallion_silver_gold_ml.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/06_pipeline_medallion_silver_gold_ml.py) (**Entrega 2 - P2**): Ejecuta las transformaciones **Bronze $\rightarrow$ Silver** (deduplicación con `QUALIFY ROW_NUMBER()`, enriquecimiento espacial `ST_GEOGPOINT`, anonimización SHA-256 con salt de Cloud DLP), registra la auditoría de **Knowledge Catalog Data Quality (`SLA >= 95%`)**, construye el **Esquema en Estrella en Capa Gold** (`dim_zonas_condados_gold`, `fact_movilidad_multimodal_gold`, `kpi_ejecutivo_condados_gold`) y exporta la caché local `lumina_gold_cache.json`.
8. [`07_dag_managed_airflow_composer.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/07_dag_managed_airflow_composer.py) (**Entrega 2 - P2**): Define el DAG oficial `lumina_medallion_end_to_end_dag` para **Managed Airflow** y permite ejecutar el flujo completo de manera serverless desde consola.
9. [`08_app_exposicion_frontend_lumina.py`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/08_app_exposicion_frontend_lumina.py) (**Entrega 3 - P3**): Microservicio Web y API REST BFF que sirve el **Centro de Control Operativo Lumina** y los endpoints `/api/v1/route-simulator`, `/api/v1/dashboard-summary`, `/api/v1/iot-telemetry` y `/api/v1/health`.

---

## 🛠️ 2. Paso a Paso de Ejecución en tu Terminal

### Paso 1: Preparar el Entorno y Autenticación GCP
Abre tu terminal en la carpeta del código ejecutable:

```bash
cd "/Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage"

# (Opcional) Crear entorno virtual e instalar librerías
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Autenticarse contra tu cuenta de Google Cloud
gcloud auth application-default login
```

Verifica que tu archivo `.env` tenga el ID de tu proyecto activo en GCP (actualmente configurado como `GCP_PROJECT_ID=gobiernodatos`).

---

### Paso 2: Desplegar Infraestructura e Ingesta Multifuente (Entrega 1 — P1)

Ejecuta en orden los scripts `01` al `05`:

```bash
# 1. Crear Bucket GCS, Tópicos Pub/Sub (+DLQ) y los 4 Datasets en BigQuery
python3 01_setup_infraestructura_lumina.py

# 2. Generar y cargar datos Batch (TLC Trips + Uber Movement 24 km/h + Censo 8 Condados)
python3 02_ingesta_batch_multifuente.py

# 3. Emitir ráfaga de eventos Streaming IoT (Array of Things + Sensores Ferroviarios OART)
python3 03_simulador_streaming_iot_aot_pubsub.py

# 4. Procesar el Streaming hacia BigQuery separando eventos sanos y corruptos (DLQ)
python3 04_pipeline_streaming_beam_bq.py --mode=pull

# 5. Verificar conteo de tablas Bronze y primer cruce en BigQuery
python3 05_verificar_cruce_multifuente_bq.py
```

---

### Paso 3: Ejecutar Transformación Medallion, Knowledge Catalog DQ y Modelado Estrella (Entrega 2 — P2)

Ejecuta el orquestador de **Managed Airflow** en modo serverless (`07`), el cual invoca internamente el motor Medallion (`06`):

```bash
python3 07_dag_managed_airflow_composer.py
```

**¿Qué ocurre automáticamente en este paso?**
1. **Capa Silver (`lumina_silver_clean`):**
   - `tlc_trips_silver`: Elimina duplicados con `QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ...) = 1`, filtra viajes inválidos (`trip_distance_km > 0.2`) y aplica **Cloud DLP Hashing (`SHA256`)** sobre `rider_id_hash`.
   - `uber_ speeds_silver`: Estandariza velocidades horarias y etiqueta los corredores en **cuello de botella (`is_bottleneck_24kmh = TRUE` cuando `avg_speed_kmh <= 25.0`)**.
   - `iot_aot_telemetry_silver`: Construye geometrías nativas `ST_GEOGPOINT(lon, lat)` y evalúa umbrales de mantenimiento predictivo (`vibration_mms > 8.5` o `temperature_c > 78.0`).
   - `ine_censo_demografico_silver`: Calcula el `indice_prioridad_primera_milla` ponderando porcentaje de adultos mayores ($\ge 65$ años) y jóvenes en los 8 condados.
2. **Compuerta de Calidad (`Knowledge Catalog`):**
   - Audita la validez en Silver y registra el resultado en `lumina_audit_dq.auditoria_calidad_medallion` verificando que el **SLA supere el 95%** (obteniendo **99.6%**).
3. **Capa Gold (`lumina_gold_governed` — Esquema en Estrella):**
   - Crea la dimensión conformada `dim_zonas_condados_gold`.
   - Crea la tabla de hechos `fact_movilidad_multimodal_gold` (particionada por `fecha_viaje` y clusterizada por `county_id, origin_zone_id, transport_mode`).
   - Crea la tabla ejecutiva `kpi_ejecutivo_condados_gold` con la recomendación diaria de flota para las 05:00 AM.
   - Actualiza el archivo local `data_landing_local/lumina_gold_cache.json` con los datos reales de tu BigQuery.

---

### Paso 4: Levantar el Front-End Operativo y la Capa de Exposición API (Entrega 3 — P3)

Para presentar la **Demo End-to-End en vivo**:

```bash
python3 08_app_exposicion_frontend_lumina.py
```

Abre en tu navegador: **`http://127.0.0.1:8080`** (o abre directamente el archivo autocontenido [`dashboard_operativo_lumina_en_vivo.html`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/dashboard_operativo_lumina_en_vivo.html)).

Si deseas desplegarlo en una URL pública de **Google Cloud Run** con escala a cero (`$0.00 USD` en reposo):

```bash
./deploy_cloud_run.sh
```

---

## 🎯 3. Guion de Sustentación Final: Los 8 Puntos Obligatorios de Slides (Entrega 3 — P3)

De acuerdo con la rúbrica oficial en [`UTEC - Desarrollo de Proyecto - Seccion 1.pdf`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/UTEC%20-%20Desarrollo%20de%20Proyecto%20-%20Seccion%201.pdf), tu presentación final de **Entrega 3 (P3)** debe estructurarse exactamente en estos **8 puntos**:

| # Slide | Punto Exigido en la Rúbrica P3 | Contenido Clave de DataLandia ("Lumina") que debes presentar |
| :---: | :--- | :--- |
| **1** | **Resumen Ejecutivo** | Creación de la Autoridad de Tránsito Unificada **"Lumina"** integrando **8 condados, 12 agencias de transporte y 9.25 millones de residentes**. Solución al colapso vial de **24 km/h (15 mph)** en hora punta aprovechando que el **89% de la población vive a < 8 km de una estación de tren OART**, logrando reducir el costo por km en **64.4% ($0.42/km vs. $1.18/km en auto)** y tiempos de viaje en **43%**. |
| **2.a** | **Situación Actual — KPIs** | **5 KPIs Gobernados:** (1) Costo promedio por km (`$0.42 USD/km`), (2) Velocidad efectiva en hora punta (`41.8 km/h` Lumina vs `23.6 km/h` auto particular), (3) Cobertura poblacional OART < 8 km (`89.0%`), (4) Índice de Ocupación Vehicular (`4.2 pax/minivan` vs `1.0 pax/auto`), y (5) Disponibilidad y Calidad del Lakehouse (`99.6% SLA en Knowledge Catalog`). |
| **2.b** | **Situación Actual — Productos Tecnológicos** | **Stack 100% Nativo GCP con Nomenclatura Estandarizada:** `Cloud Storage` (Landing), `Cloud Pub/Sub` (Ingesta Real-Time), `Dataflow / Apache Beam` (Streaming ETL + DLQ), `BigQuery` (Lakehouse Bronze/Silver/Gold), `Managed Airflow` (Orquestación inter-capa), `Knowledge Catalog` (Gobierno y Calidad), `Cloud DLP` (Privacidad PII), `Agent Platform` (ML y Ruteo Inteligente), `Cloud Run` (API BFF) y `Data Studio` (Visualización). |
| **2.c** | **Dependencias, Supuestos y Fuera de Scope** | **Supuestos de Negocio (SN-01 a SN-04):** Cobertura OART del 89%, línea base vial de 24 km/h, segmentación demográfica de adultos mayores en suburbios y jóvenes, y patrocinio de un miembro del directorio. **Supuestos Técnicos (ST-01 a ST-05):** Coordenadas proyectadas al área metropolitana, GPS pre-agregado cada 30s, anonimización SHA-256 en tránsito. **Fuera de Scope Año 1:** Cobro bancario EMV en torniquetes físicos y control físico de señales ferroviarias PTC. |
| **2.d** | **Arquitectura Final (Año 1)** | Presentar el diagrama del archivo [`PROYECTO_MASTER_DATALANDIA_LUMINA.html`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/PROYECTO_MASTER_DATALANDIA_LUMINA.html) mostrando explícitamente los **2 carriles separados (Capa Batch vs. Capa Real-Time)**, la transición **Bronze $\rightarrow$ Silver $\rightarrow$ Gold** justificando **Managed Airflow frente a Dataform**, y las **2 capas transversales (Redes VPC/Cloud Armor + Seguridad IAM/CMEK/DLP/Knowledge Catalog)**. |
| **2.e** | **Calculadora de Consumo (FinOps)** | **Presupuesto Productivo Año 1:** **$2,105.50 USD/mes** (`$25,266 USD/año`, equivalente a **$0.0027 USD por habitante/año**), ahorrando **57.1%** gracias a particionamiento/clustering en BigQuery, ciclo de vida Coldline en GCS y pre-agregación en Dataflow. **Presupuesto Académico en Cuenta de $250 USD:** **< $0.10 USD** mediante arquitectura Serverless Scale-to-Zero. |
| **2.f** | **Demo End-to-End (En Vivo)** | Mostrar en pantalla [`dashboard_operativo_lumina_en_vivo.html`](file:///Users/herivera/Documents/MAESTRIA/Big Data/Tareas/Avance1_P1_Lumina_Ingesta_Storage/dashboard_operativo_lumina_en_vivo.html) (o `http://127.0.0.1:8080`): (1) Simular en vivo una ruta desde *Condado Altos del Este (Suburbio Adulto Mayor)* hacia *Centro Financiero* en Hora Punta y ver cómo **Agent Platform** asigna Minivan Accesible + Tren OART ahorrando 67% de costo y 13.4 min; (2) Mostrar la tabla de Planes Diarios a las 05:00 AM por condado; (3) Mostrar las alertas predictivas IoT OART y los eventos corruptos aislados en la **Dead-Letter Queue**. |
| **2.g / 2.h** | **Conclusiones y Next Steps** | **Conclusiones:** La arquitectura Medallion desacoplada permite combinar decisiones de sub-segundo (desvío por falla en riel OART) con planificación demográfica diaria sin cuellos de botella. **Next Steps (Año 2):** (1) Integración de cobro sin contacto Open-Loop (NFC) en minivans y trenes OART; (2) Gemelo Digital (Digital Twin) con Vertex AI / Agent Platform Reinforcement Learning para semáforos adaptativos; (3) Apertura de portal Open Data para desarrolladores ciudadanos vía Apigee. |
