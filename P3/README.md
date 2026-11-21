# Entrega 3 Exposición de los datos existentes de Lumina

P3 consulta exclusivamente la tabla que ya está disponible en el proyecto:
`bigdata-herles-pinedo.lumina_gold_governed.fact_movilidad_multimodal_gold`.
El recorrido es Gold existente → SELECT OLAP → API REST → tablero consumidor.
No genera viajes, no inserta anomalías y no crea ni reemplaza tablas en Google Cloud.

## Resultado verificado de la fuente existente

La consulta del 20/11/2026 a las 22:31 America/Lima devolvió:

| Dato de la fuente | Resultado |
| --- | --- |
| Registros consultados | 4,998,926 |
| Distancia sumada | 45,585,485.89 km |
| Importes de tarifa, excluyendo PERSONAL_CAR_BENCHMARK | USD 82,534,392.08 |
| Última modificación registrada de la tabla | 11/11/2026 04:27:08 UTC |
| Job de consulta | 47635f4c-2483-4d2d-840c-1499dfeba6f5 |
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
