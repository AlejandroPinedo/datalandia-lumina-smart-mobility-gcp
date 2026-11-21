# Lumina · Interfaz React

Aplicación de análisis de movilidad: React, Vite y Node.js. El diseño usa una
paleta clara, tipografía del sistema, iconos Lucide y gráficos interactivos.
Funciona sin fuentes, librerías o servicios externos cargados desde un CDN.

## Ejecutar

Desde `frontend`:

```powershell
npm.cmd install
npm.cmd run build
npm.cmd start
```

Abrir http://127.0.0.1:3000/p3. El servidor Node sirve React y la API de lectura
`/api/v1/olap-kpis` en el mismo origen. Lee exclusivamente la exportación existente
`P3/evidencias/datos_existentes_latest.json`. No necesita credenciales de Google
en el navegador ni una segunda ejecución de Python.

La compilación también empaqueta React en
`Avance1_P1_Lumina_Ingesta_Storage/templates/p3_olap.html`. Así la URL anterior
http://127.0.0.1:8080/p3 sigue mostrando la nueva interfaz cuando se ejecuta
`python P3/demo_e2e.py --serve` desde la raíz del repositorio.

Para desarrollar con recarga automática, ejecutar `npm.cmd run dev`. Vite usa
http://127.0.0.1:5173 y reenvía `/api` al servidor Python del puerto 8080.

## Datos e interacciones

La página principal abre ahora la **web del pasajero**:

- `#viajar`: elegir origen/destino, invertirlos, comparar duración, tarifa y
  distancia medias observadas, inspeccionar conexiones OART y consultar el trayecto.
- `#registros`: buscar ID, fecha, zonas y transporte; paginar, abrir el detalle y
  descargar los registros exportados con su fuente y job ID.
- `#guardados`: guardar una alternativa y volver a consultarla desde este navegador.
- `#red`: explorar conexiones observadas y abrir sus alternativas.

El enlace **Análisis** abre el tablero administrativo anterior en `#overview`.
El enlace **Web del pasajero** permite volver. Las funciones del panel analítico
se conservan:

- Resumen: evolución diaria, distribución por modo, hora y condado.
- Explorador: búsqueda, ordenación y ficha de cada modo.
- Comparación: dos modos con los mismos filtros temporales y territoriales.
- Consultas: respuestas calculadas sobre los indicadores de la selección.
- Fuente: calidad, tabla, fechas, jobs, SQL y esquema.

El CSV conserva códigos originales, filtros y job ID de la consulta.
Los nombres más breves de los modos son etiquetas de presentación.
Se conserva la lógica compartida de agregación de `P3/web/analytics.js`.
Las medias utilizan SUM/COUNT; se mantienen los valores nulos y la exclusión del
auto de referencia en los importes totales. Los filtros no generan registros.

«Actualizar datos» vuelve a leer el resultado exportado de la API. Para ejecutar
una consulta nueva en BigQuery usar `P3/usar_datos_existentes.ps1` desde la raíz.

## Fuente del pasajero

Ejecutar una vez desde PowerShell con las credenciales del proyecto ya guardadas:

```powershell
.\P3\recuperar_web_pasajero.ps1
```

Este script se ejecuta desde la raíz del repositorio, no desde `frontend`. Consulta
exclusivamente el Gold existente, con máximo de 5 GiB facturables, y guarda
`P3/evidencias/movilidad_pasajero_latest.json`. No modifica tablas, no ejecuta
generadores ni exporta el identificador del pasajero.

`GET /api/v1/passenger-data` está disponible tanto en Node como en Python.
Las alternativas se agregan sobre todos los registros de cada trayecto. El detalle
exportado incluye **hasta 3 últimos viajes por origen, destino, condado y modo**;
no es una copia de los millones de viajes ni un historial personal.
Las tarifas son medias históricas de la columna de Gold; no son cotizaciones.
Las estaciones OART son asociaciones de la fuente, sin orden de paradas verificado.
La visualización es un esquema, sin posiciones geográficas inventadas.

Los nombres de destino se vinculan por el mismo ID a los nombres de origen
observados en Gold. Si falta una correspondencia única se muestra el ID de zona.
No se utilizan listas prefabricadas de zonas o estaciones como respaldo.

Los trayectos guardados utilizan almacenamiento local del navegador. No crean
reservas, pagos ni una cuenta personal. Compartir copia el enlace con los IDs del
trayecto; quien lo abra necesita acceso al mismo servidor.

## Validar

```powershell
npm.cmd run build
npm.cmd test
```

Las pruebas contrastan filtros, totales, consultas y CSV con la evidencia existente,
y verifican la API Node y la ausencia de datos suplentes cuando falta la evidencia.

`P3/web/dashboard.html`, `.css` y `.js` corresponden a la interfaz anterior;
la fuente vigente de la interfaz es `frontend/src`.
