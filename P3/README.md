# Entrega 3 Exposición de los datos existentes de Lumina

P3 consulta exclusivamente la tabla que ya está disponible en el proyecto:
`bigdata-herles-pinedo.lumina_gold_governed.fact_movilidad_multimodal_gold`.
El recorrido es Gold existente → SELECT OLAP → API REST → tablero consumidor.
No genera viajes, no inserta anomalías y no crea ni reemplaza tablas en Google Cloud.

## Resultado verificado de la fuente existente

La consulta del 20/11/2026 a las 23:00 America/Lima devolvió:

| Dato de la fuente | Resultado |
| --- | --- |
| Registros consultados | 4,998,926 |
| Distancia sumada | 45,585,485.89 km |
| Importes de tarifa, excluyendo PERSONAL_CAR_BENCHMARK | USD 82,534,392.08 |
| Última modificación registrada de la tabla | 11/11/2026 04:27:08 UTC |
| Job de consulta | e03f867a-64fa-44b7-90dd-752b97c3546c |
| Job del desglose multidimensional | bdf94307-1c83-4735-833c-c2b5288666c9 |
| Modificación de la fuente por este proceso | Ninguna |
| API | Coincide con los agregados de esa consulta |

Los importes anteriores son sumas de las columnas de la tabla, no facturación auditada
ni una demostración de ahorro real. Se usa el histórico que el equipo ya tenía; la
procedencia externa no queda certificada solo por estar almacenado en BigQuery.
La descripción de la tabla está vacía. Las columnas de cobertura explícita de los cruces
no están presentes; su valor se conserva como NULL, sin inventar porcentajes.

## Evidencia vigente

- `evidencias/datos_existentes_latest.json`: fuente, esquema, fecha, SQL, job ID y resultado.
- `evidencias/esquema_fuente_existente.json`: metadatos consultados antes de agregar.
- `verificar_evidencia.py`: contrasta el SQL y los totales de esa evidencia.
- `evidencias/archivo_pruebas_sinteticas/`: pruebas anteriores retiradas de la entrega vigente.

La antigua prueba de 600 viajes no se utiliza como fuente, como respaldo de la API
ni como evidencia de procesamiento del histórico del proyecto. Se conserva archivada
para no borrar el rastro de lo que se había ejecutado.

## Consultar de nuevo sin iniciar sesión otra vez

Con las credenciales ya guardadas, ejecutar en PowerShell:

```powershell
.\P3\usar_datos_existentes.ps1
```

La consulta lee todos los registros de la tabla elegida, sin LIMIT ni tamaño de muestra fijo.
Registra el esquema, agrega únicamente columnas disponibles y verifica la respuesta HTTP.
Tiene un máximo de 5 GiB facturables por consulta. No ejecuta ingesta ni recalcula las
capas Bronze/Silver/Gold del proyecto; utiliza el Gold preexistente como fuente del informe.

La sesión de Codex puede leer la exportación aunque su conexión de red esté restringida.
Las credenciales permanecen en .runtime y están excluidas de Git.

## Mostrar el tablero con la consulta ya exportada

La web vuelve a incluir una entrada para el pasajero. En `/p3` se abre el buscador
de trayectos; **Análisis** conserva el tablero de indicadores. Para habilitar origen,
destino y registros individuales ejecutar una vez `P3/recuperar_web_pasajero.ps1`.
Lee el mismo Gold y exporta alternativas históricas y hasta los tres últimos
registros de cada grupo origen/destino/condado/modo. La interfaz declara este alcance;
no lo presenta como historial personal, reservas ni disponibilidad en vivo.

La web del pasajero incluye comparación por duración/tarifa/distancia, conexiones
OART de la fuente, consultas calculadas del trayecto, guardados en el navegador y
detalle/CSV de registros. `/api/v1/passenger-data` sirve la evidencia identificada
`evidencias/movilidad_pasajero_latest.json`. No hay coordenadas o rutas inventadas.

La consulta del pasajero del 21/11/2026 a las 00:03 America/Lima fue verificada:
8 zonas de origen/destino, 320 grupos de trayecto por modo y condado, 960 registros
individuales de detalle y 4.998.926 registros agregados. Job de BigQuery:
`065beb66-6272-4132-9338-57f0709fda24`. Las API Node (3000) y Python (8080)
devuelven la misma exportación; los enlaces de trayecto con origen/destino responden
HTTP 200. Pasaron las 33 pruebas Python y 15 pruebas Node, sin omisiones.

El centro de control conserva la identidad visual de Lumina y permite:

- Combinar fecha inicial/final, condado, hora y selección de modos.
- Explorar registros, distancia, duración y tarifa/km, con fichas por modo.
- Seleccionar barras de fecha, hora o condado para profundizar en el resultado.
- Comparar dos modos con los mismos filtros territoriales y temporales.
- Ordenar y buscar en la tabla; descargar CSV con filtros, fuente y job ID.
- Resolver consultas guiadas de resumen, viajes, duración, tarifa y calidad.
- Revisar nulos, distancias negativas, esquema y SQL ejecutado.

El desglose contiene 56.220 grupos observados, 59 fechas, 8 condados, 5 modos y
24 horas. Sus recuentos suman los 4.998.926 registros de la consulta de resumen.
Las medias filtradas se calculan mediante SUM/COUNT de valores no nulos; los
importes excluyen el auto de referencia en las tarjetas de totales. El costo/km
es el cociente de sumas, no una media de cocientes.

Las interacciones operan sobre la exportación identificada de BigQuery.
«Actualizar datos» vuelve a leer la API; para consultar nuevamente BigQuery se
ejecuta `P3/usar_datos_existentes.ps1`. No hay telemetría en vivo, coordenadas
observadas ni un motor de rutas conectado. No se simulan esas funcionalidades.

La interfaz vigente usa React y Node.js; sus recursos editables están en
`frontend/src/`. Instalar dependencias una vez con `npm.cmd --prefix frontend install`.
Después de modificarlos ejecutar `python P3/build_web.py` para compilar React y
actualizar la plantilla autocontenida. Abrir el
tablero mediante el servidor permite acceder a la API; abrir el HTML como archivo
no establece esa conexión.

También se puede ejecutar `npm.cmd --prefix frontend start` después de compilar,
y abrir http://127.0.0.1:3000/p3. El servidor Node lee la misma evidencia existente
y expone la API en el mismo origen; no requiere ejecutar Python para servir esta
exportación. El código anterior de `P3/web/dashboard.*` queda como referencia;
la compilación vigente usa exclusivamente `frontend/src`.

```bash
python P3/demo_e2e.py --serve
```

Abrir http://127.0.0.1:8080/p3 y http://127.0.0.1:8080/api/v1/olap-kpis.
El modo `project_export` sirve el resultado identificado de la consulta guardada, con tabla,
job ID y fecha de consulta. No se presenta como una consulta remota en vivo.
Si falta esa evidencia o corresponde a datos sintéticos, la API responde unavailable;
el tablero muestra datos no disponibles, sin cifras de respaldo.

Para ejecutar una consulta nueva y después servirla desde una terminal con red:

```bash
python P3/usar_datos_existentes.py --serve
```

## Matriz de la rúbrica UTEC

La implementación y evidencia nuevas del fondo están en [operations/README.md](../operations/README.md).
`P3/completar_validacion_fondo.ps1` ejecuta el pipeline completo sobre Bronze existente,
en un dataset aislado. `P3/verificar_fondo.py` reconcilia sus resultados y restauración.
La web en `#operations` consume esos resultados por `/api/v1/operations`.
El Gold nuevo contiene 4.996.009 registros válidos; el histórico de 4.998.926 que usa
el explorador del pasajero se conserva y se identifica por separado.

| Requisito P3 | Entregable y alcance |
| --- | --- |
| Exposición mediante consulta o API | GET /api/v1/olap-kpis expone agregados de la tabla existente y su trazabilidad. |
| Consumidor | / y /p3 sirven el tablero conectado a la API. |
| Caso de uso OLAP | Consulta y agregación de todo el Gold existente, exposición HTTP y consumo. No reejecuta la ingesta histórica. |
| Repositorio | Código, pruebas y evidencia guardados localmente; push pendiente de confirmar. |
| Slides | Fuente de 12 diapositivas actualizada con los registros existentes; falta guardar y revisar la presentación final. |
| Defensa individual y grupal | Debe realizarse en clase; los archivos no acreditan esa participación. |

Fuente: UTEC - Desarrollo de Proyecto - Seccion 1.pdf, página 3. La rúbrica permite
demostrar un proceso de informes OLAP; no exige introducir datos simulados ni un modelo ML.

## Costos y validación

`python P3/calculadora_consumo.py` mantiene una estimación editable del primer año
del piloto. Sus consumos son supuestos de planificación, no mediciones ni facturación
de los viajes de la fuente. Las tarifas están enlazadas a las páginas oficiales en costos.json.

```bash
python -m unittest discover -s tests -v
python P3/verificar_evidencia.py
python build_entrega3_html.py
```

Las pruebas verifican, entre otros casos, que P3 no importe generadores, no muestree la
fuente, no emita SQL de escritura y rechace los manifiestos sintéticos como fuente vigente.
La comprobación HTTP no equivale a una inspección visual del navegador.
