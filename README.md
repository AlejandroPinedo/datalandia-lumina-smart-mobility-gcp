# DataLandia Lumina Smart Mobility

Proyecto académico de arquitectura Big Data para la autoridad Lumina, UTEC, Grupo 7.
Integrantes: Herles Pinedo, David Jimenez y José Nomberto.

La solución integra viajes, velocidades viales, telemetría y demografía. El código incluye
ingesta incremental, procesamiento Medallion, auditoría SQL, APIs y simulación de viajes.
La P3 vigente consulta el Gold que ya estaba disponible en BigQuery: 4,998,926 registros.
No genera ni carga viajes. La API utiliza la consulta identificada de esa fuente; no se presenta
como un despliegue de Cloud Run ni como validación de todos los componentes P1/P2.

## Estado y evidencia

| Componente | Estado verificable |
| --- | --- |
| BigQuery Gold histórico | C1 muestra 4,998,926 filas, 59 particiones y 1.80 GB lógicos. No acredita la ejecución del código corregido. |
| Calidad histórica | C2 muestra tres reglas SQL; no prueba un servicio Dataplex desplegado. 4,998,918 de 4,998,926 registros financieros son válidos: el 100.0% mostrado está redondeado. |
| Pub/Sub | C3 muestra tópico y suscripción. No acredita continuidad, throughput ni latencia. |
| Data Lake | G4 muestra Parquet y capas; el bucket es regional us-central1. No prueba Iceberg ni réplica dual. |
| Spark | G5 muestra fallo de Java seguido de demo Python de 600 registros. No acredita Spark ni Dataproc. |
| Componentes P1/P2 | Las pruebas locales pasan; la validación completa de esos pipelines y de Beam, Spark y Airflow en cloud sigue pendiente. La demo P3 se acredita por separado. |
| P3 con datos existentes | Consulta de solo lectura a fact_movilidad_multimodal_gold: 4,998,926 registros. API contrastada con la consulta. La prueba sintética anterior está archivada y no se usa. |

## Entregables

- **[Entrega 3 P3: API, demo OLAP y sustentación](P3/README.md)** · ejecutar `python P3/demo_e2e.py --serve` y abrir `/p3`.
- **[Evidencia P3 de los datos existentes](P3/evidencias/datos_existentes_latest.json)** · tabla, esquema, consulta y job ID de solo lectura.
- [Calculadora del primer año](P3/calculadora_consumo.py) y [contenido de sustentación](P3/diapositivas.json).

- [Informe de procesamiento y trazabilidad](Avance2_P2_Procesamiento_Transformacion_Modelado_DataLandia.md)
- [Diseño de ingesta y almacenamiento](Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md)
- [Justificación, costos y resiliencia](Avance1_Examen_Sustentacion_Tecnica_Economica_Resiliencia.md)
- [Guía de ejecución](GUIA_PASO_A_PASO_IMPLEMENTACION_LUMINA.md)
- [Presentable actualizado P2](00_DataLandia_Lumina_Entrega2_Procesamiento_Transformacion.html)
- [Presentable de arquitectura](PROYECTO_MASTER_DATALANDIA_LUMINA.html)

## Ejecutar y verificar

Desde la raíz del repositorio:

```bash
python -m unittest discover -s tests -v
python P3/verificar_evidencia.py
python build_entrega3_html.py
python P3/demo_e2e.py --serve
```

Abrir http://127.0.0.1:8080/p3. El tablero muestra la exportación identificada de la consulta
al Gold existente. Si no hay una consulta válida disponible, responde datos no disponibles;
no utiliza la antigua muestra sintética como respaldo. Para consultar de nuevo la fuente,
ejecutar `P3/usar_datos_existentes.ps1` con las credenciales ya guardadas.

Los scripts de P1/P2 se conservan como material de esas fases. Sus generadores no forman
parte de la P3 vigente. El recorrido actual utiliza las tablas existentes y solo emite
consultas SELECT; no ejecuta el setup, no recarga Bronze ni reconstruye Silver/Gold.

## Cobertura pendiente del taller

La arquitectura todavía requiere fuentes operativas de solicitudes, GPS, flota/personal, horarios,
eventos y accesibilidad para asignación real de viajes. Los planes y el mantenimiento tienen baselines
exploratorios; faltan backtesting, validación de operaciones y datos de fallas. No se implementan pagos.
La portabilidad estricta de código abierto, el DRP multirregión, IAM/CMEK y pruebas de carga son diseño objetivo.
