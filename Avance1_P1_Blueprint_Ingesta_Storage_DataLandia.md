# Arquitectura de ingesta y almacenamiento de Lumina

Lumina integra las doce agencias de DataLandia para conectar primera y última milla con OART.
El caso exige soportar decisiones en tiempo real, planificación diaria, informes y mantenimiento.
Este blueprint separa los componentes incluidos en el repositorio de la arquitectura objetivo.

## Fuentes y contratos canónicos

| Fuente | Naturaleza | Llaves y uso |
| --- | --- | --- |
| F1 viajes | Batch | trip_id, origen, destino, pickup_datetime UTC; tarifas, distancia y duración |
| F2 tráfico | Batch horario | origen, destino, travel_date UTC, hod; velocidad y tiempo |
| F3 IoT | Streaming | event_id, node_id, station_id, zone_id, timestamp_evento con zona; carga y estado físico |
| F4 censo | Referencia batch | zone_id y county_id; demografía y primera milla |

Las referencias NYC TLC, Uber Movement, Array of Things y Censo INE son ejemplos sugeridos
por el taller. Los CSV locales actuales son sintéticos: 600 viajes, 4,968 velocidades y 24 filas censales en la muestra regenerada.
No se presenta su generación como descarga, homologación real de ciudades ni prueba de representatividad.
El catálogo de código contiene 24 zonas en ocho condados; el generador censal cubre las 24 zonas y reparte exactamente 9.25 millones de habitantes.
Las 260 zonas y 85 estaciones son dimensionamiento objetivo, no cobertura implementada.

## Ingesta implementada

El script 02 genera localmente por defecto. --upload carga cada lote en GCS, crea staging
con el esquema del destino, carga CSV y hace MERGE por llave natural. Nunca usa --replace en Bronze.
Los viajes sintéticos tienen IDs reproducibles por fecha, índice y tamaño de muestra;
el censo se actualiza por zone_id y el tráfico por origen/destino/fecha/hora.
Los lotes existentes pueden suministrarse mediante --input-dir con el esquema canónico.
La conservación del histórico de cambios del censo (SCD2) sigue pendiente: este MERGE mantiene estado actual.

IoT se publica en Pub/Sub. El modo pull procesa un lote acotado; --mode=beam solicita
un pipeline continuo. Beam empaqueta telemetry.py mediante setup.py. No se afirma que
el consumidor pull sea un servicio streaming continuo ni se prometen costos cero.

## Almacenamiento y procesamiento

BigQuery conserva tablas nativas Bronze, Silver y Gold. Los datasets y tablas se crean en
01_setup_infraestructura_lumina.py. Las tablas temporales de ingesta se eliminan tras el MERGE.
Los archivos GCS se identifican por hash del contenido y preservan los lotes enviados.
Silver deduplica; Gold cruza fecha/hora UTC y estación. Los datos faltantes permanecen NULL.

La evidencia G4 muestra un bucket regional us-central1 y archivos Parquet; no demuestra
un catálogo o tabla Iceberg, transacciones de lakehouse ni réplica multirregional.
La implementación actual es un warehouse Medallion con archivos en un data lake.

## Arquitectura objetivo y cuatro servicios

1. Tiempo real: solicitudes y GPS -> broker -> validación/estado de flota -> optimizador -> API de rutas.
2. Plan diario: viajes/censo/eventos/flota -> pronóstico -> restricciones de capacidad/personal -> plan aprobado.
3. OLAP: Silver/Gold -> APIs y cuadros de mando con cobertura, frescura y procedencia.
4. Mantenimiento: sensores -> detección y tendencias -> validación por ingeniería -> programación de inspección.

La versión actual incluye telemetría, simulador, consultas OLAP y baselines exploratorios para 2 y 4.
El frontend del RFP se integra mediante REST. Para asignación real faltan contratos de solicitudes,
vehículos/conductores, horarios OART, mapas/eventos y preferencias de accesibilidad.
Las respuestas deben incluir observed_at, valid_until, provenance y errores por fuente no disponible;
la falta de disponibilidad no debe resolverse inventando vehículos, horarios o mediciones.

## Código abierto y propiedad

Python, Flask, Beam, Spark y Parquet son componentes/formatos abiertos utilizados o previstos.
BigQuery, Pub/Sub, GCS y Dataflow son servicios gestionados dependientes de GCP. Los clientes cloud
y el SQL específico impiden afirmar portabilidad sin cambios. La priorización estricta de código
abierto del taller se cumple parcialmente; se propone Kafka, Beam/Flink, Iceberg/Parquet,
Trino/Spark y almacenamiento compatible para una ruta portable, aún no implementada.
Lumina es propietaria de los datos; la IP de los pipelines corresponde al proveedor según el caso.

## Seguridad y calidad

El repositorio usa datos de demo seudonimizados; un hash no acredita anonimización legal ni Cloud DLP.
No se almacenan credenciales en Git. IAM de mínimo privilegio, secretos gestionados, políticas de acceso,
retención, cifrado con claves del cliente, redes privadas y auditoría requieren configuración y evidencias.
La auditoría implementada es SQL, no un despliegue de Dataplex. Ver el informe P2 para sus reglas y límites.

## Crecimiento y fiabilidad

Para escalar a otra metrópoli se deben parametrizar territorio, estaciones, agencias, zona horaria,
tarifas y segregación de datos, además del proyecto y datasets que ya son configurables.
El escenario del desfile central requiere un ensayo con tasa de entrada, backlog, p95/p99,
errores de escritura, reentregas y recuperación. No hay evidencia de 120,000 eventos/s ni latencia de 18 ms.
Ver la sustentación para el dimensionamiento consistente y el DRP propuesto.
