# Resumen ejecutivo y alcance de la implementación

**Pregunta del pasajero.** ¿Qué modos conectaron mi origen y mi destino, cuánto tardaron y qué tarifa registraron?

Lumina reúne cuatro fuentes del proyecto en BigQuery. El pasajero compara alternativas históricas y el analista verifica cómo se obtuvo el indicador. El proceso de negocio demostrado de extremo a extremo es el análisis de viajes por zona y modo.

# Situación actual de los datos

| Fuente disponible | Registros | Cobertura observada |
|---|---:|---|
| Viajes | 5.000.000 | Enero y febrero de 2026 |
| Tráfico | 504 | 10 de noviembre de 2026 |
| Telemetría | 11 | 11 de noviembre de 2026 |
| Censo | 8 | Zonas del proyecto |

::: notes
Las fechas permiten comparar alternativas históricas. El tráfico de noviembre sirve como contexto territorial para cada zona.
:::

# Criterios de éxito y KPI de rendimiento

| Indicador | Criterio de aceptación | Resultado |
|---|---|---|
| Fidelidad de carga | Conteo y huella iguales | 4 de 4 tablas |
| Calidad de Silver | Cada regla ≥ 95 % | Mínimo 99,92018 % |
| Unicidad de Gold | Una fila por viaje | 4.996.009 de 4.996.009 |
| Reentrega IoT | Conservar el estado | 11 duplicados detectados |
| Latencia HTTP local | Medición exploratoria | p95 de 148,52 ms |
| Errores HTTP locales | 0 errores | 0 de 48 con concurrencia 6 |

# Productos tecnológicos y función

| Producto | Uso en la solución |
|---|---|
| Cloud Storage y Parquet | Conservar archivos por fuente y volver a cargarlos |
| BigQuery y GoogleSQL | Depurar viajes, aplicar reglas y agregar por modo |
| Python y SQLite | Reproducir los eventos y restaurar el estado por sensor |
| Node.js y React | Servir el histórico y comparar alternativas en la web |
| Pub/Sub y Cloud Run | Tópico de telemetría configurado y alojamiento previsto de la API, respectivamente |

# Dependencias, supuestos y exclusiones

**Datos.** El despacho requiere flota, turnos y solicitudes vigentes. Estimar probabilidad de falla requiere series por activo y fallas etiquetadas.

**Acceso.** Repetir la ingesta requiere permisos en BigQuery y en el bucket del proyecto. La demo local lee las exportaciones guardadas.

**Presupuesto.** El escenario supone consumo mensual constante durante doce meses. El streaming continuo, el almacenamiento de Parquet en GCS y la recuperación multirregional quedan fuera de la cifra.

# Arquitectura final y consumo del primer año

| Etapa | Diseño y alcance económico |
|---|---|
| Ingesta | Las cuatro fuentes pasan por Parquet y llegan a Bronze. GCS queda fuera del subtotal. |
| Transformación | BigQuery construye Silver, audita calidad y publica Gold. Se presupuestan 2 TiB de consultas y 100 GiB de almacenamiento lógico al mes. |
| Exposición | Node.js sirve los resultados a React. El escenario en Cloud Run contempla 10.000 peticiones mensuales. |
| Eventos | El consumidor procesa telemetría incrementalmente. El replay y la recuperación se probaron en SQLite. |

# Calculadora de consumo del piloto OLAP

| Partida | Consumo por mes | USD/mes |
|---|---:|---:|
| Consultas BigQuery | 2 TiB | 12,50 |
| Almacenamiento BigQuery | 100 GiB | 2,30 |
| CPU de Cloud Run | 10.000 vCPU-s | 0,24 |
| Memoria de Cloud Run | 5.000 GiB-s | 0,0125 |
| Peticiones de Cloud Run | 10.000 | 0,004 |

# Presupuesto del primer año

**Primer año:** USD 15,0565 × 12 × 1,20 = **USD 216,81**. Para recalcular, edite `P3/costos.json` y ejecute `python P3/calculadora_consumo.py`.

# Demo de extremo a extremo · De Bronze a Gold

1. Abra el manifiesto de ingesta. Compare origen y destino de las cuatro tablas. Los conteos y las huellas coinciden.
2. En **Validación integral**, revise las cuatro reglas y reconcilie las 5.000.000 de filas de entrada con 3.991 exclusiones.
3. Compruebe el resultado OLAP. De los 4.996.009 viajes únicos, 1.897.728 corresponden al modo multimodal OART.

::: notes
Con el servidor iniciado, abra http://127.0.0.1:3000/p3#operations. El manifiesto se encuentra en P3/evidencias/ingesta_demostrada_latest.json. En P3/evidencias/validacion_fondo_latest.json, la etapa Consumo OLAP del Gold nuevo identifica el trabajo que agrega por modo. La vista utiliza la exportación guardada de esa ejecución.
:::

# Demo de extremo a extremo · Consulta del pasajero

1. En **Viajar**, seleccione origen y destino. Compare los tiempos y tarifas registrados para cada alternativa.
2. Abra el detalle de una alternativa y guárdela. Compruebe que aparece en **Guardados**.
3. En **Registros**, filtre por modo y abra un viaje. La vista dispone de 960 ejemplos de la tabla Gold histórica.

La vista del pasajero consulta la exportación histórica de 4.998.926 filas. **Validación integral** presenta la ejecución depurada de 4.996.009 viajes.

::: notes
Desde frontend, use npm ci, npm run build y npm start. Abra http://127.0.0.1:3000/p3?origen=ZDL-001&destino=ZDL-002#viajar. La web muestra alternativas observadas. El guardado se mantiene en este navegador.
:::

# Conclusiones

- El piloto permite consultar un trayecto y rastrear el agregado hasta su ejecución en BigQuery. Cada viaje validado tiene una única fila y una zona censal asociada.
- Se evaluaron retrospectivamente 32 series de demanda. Los viajes y la telemetría pertenecen a meses distintos, lo que explica las cero coincidencias temporales.
- El consumidor reconoció las 11 reentregas y restauró su estado desde un respaldo. La evidencia de recuperación corresponde al entorno local.

# Next Steps · Próximos pasos

| Prioridad | Acción | Cómo comprobar el avance |
|---|---|---|
| 1. Actualizar fuentes | Recibir viajes, tráfico y sensores del mismo período | Medir coincidencias temporales por zona |
| 2. Validar operación | Integrar turnos, flota y solicitudes por ciclo | Medir solicitudes atendidas y tiempos de asignación |
| 3. Evaluar mantenimiento | Vincular historial de sensores y fallas por activo | Medir precisión y anticipación en un período posterior |
| 4. Dimensionar producción | Ensayar la API en nube y restaurar un respaldo regional | Comparar p95, tiempo de recuperación y costo con sus objetivos |
