# Entrega 3 Exposición y demo de Lumina

La P3 demuestra un proceso OLAP completo: generar y cargar datos, validar telemetría,
limpiar/deduplicar, cruzar cuatro fuentes, materializar Gold y consultar sus indicadores desde
una API y un tablero consumidor. El modo local y el modo BigQuery se ejecutaron y verificaron.
La ejecución cloud del 20/11/2026 a las 22:14 America/Lima dejó un manifiesto con 16 job IDs,
600 viajes únicos y la API consultando las tablas de esa misma ejecución.

## Evidencia de ejecución en BigQuery

| Verificación | Resultado del ensayo |
| --- | --- |
| Dataset aislado | bigdata-herles-pinedo.p3_20261121_031258_1e7263 |
| Entrada de viajes | 602 filas: 600 de muestra, un duplicado y una distancia negativa |
| Gold | 600 filas y 600 trip_id únicos |
| Cobertura de tráfico | 540 de 600 viajes (90%) |
| Cobertura IoT | 600 de 600 viajes (100%) |
| Eventos inválidos | 2 aislados en deadletter |
| API | status=ok, backend=bigquery, run_id coincidente |
| Tiempo de ejecución | 62.982 segundos en este ensayo; no es un SLA |
| Alcance | Datos sintéticos; API local contra BigQuery, sin despliegue Cloud Run |

Manifiesto: evidencias/bigquery_latest.json. Incluye hashes de los lotes, huella del SQL,
job IDs y respuesta de la API. La comprobación HTTP del consumidor no sustituye una
inspección visual del navegador, que sigue declarada como no realizada.
Las tablas de la demo tienen expiración de siete días; no son almacenamiento permanente de la entrega.

## Matriz de la rúbrica UTEC

| Requisito P3 | Entregable y alcance |
| --- | --- |
| Consulta, endpoint o API | GET /api/v1/olap-kpis devuelve datos de Gold, procedencia, run_id y fecha de procesamiento. |
| Aplicación consumidora | GET /p3 sirve un tablero que consulta ese endpoint. No usa cifras fijas como resultados. |
| Demo de un proceso end-to-end | demo_e2e.py recorre las capas con datos de esa ejecución y comprueba la lectura HTTP. |
| Repositorio estructurado | Código, pruebas, SQL, calculadora y manifiestos en P3/. La publicación requiere push confirmado. |
| Sustentación | diapositivas.json contiene 12 diapositivas y notas con los diez apartados de la rúbrica. La presentación final y el enlace se registran cuando se guarden. |
| Participación individual y grupal | El equipo debe ensayar y sustentar en clase; los archivos no acreditan participación realizada. |

Fuente de requisitos: UTEC - Desarrollo de Proyecto - Seccion 1.pdf, página 3.
La rúbrica permite demostrar al menos un proceso clave, incluidos informes OLAP; no obliga a ML.

## Demo local reproducible

Desde la raíz del repositorio, con Python 3.11 o posterior:

```bash
python -m unittest discover -s tests -v
python P3/demo_e2e.py --serve
```

Abrir http://127.0.0.1:8080/p3 y http://127.0.0.1:8080/api/v1/olap-kpis.
El proceso queda sirviendo hasta Ctrl+C. Sin --serve termina tras verificar y escribir el manifiesto.
No requiere Flask, Google Cloud, Docker ni credenciales. No modifica el Gold histórico ni los CSV
originales: crea una base y datos de trabajo nuevos en .runtime/p3 por ejecución.

## Resultado esperado y verificaciones

La entrada contiene 602 viajes: 600 de muestra, un duplicado y una distancia negativa.
Se generan 24 zonas censales, 4,968 velocidades, 240 eventos IoT válidos y dos anomalías.
Gold debe conservar 600 viajes únicos. El tráfico coincide en 540 (90%); los sensores en 600.
Las 60 faltantes de tráfico se conservan como ausencia de observación, no velocidad supuesta.
El ingreso de Lumina excluye el benchmark del automóvil particular.

evidencias/sqlite_latest.json contiene la ejecución real más reciente: hashes, conteos,
run_id, duración de ensayo y respuesta de API. Es evidencia de ejecución local con datos
sintéticos, no una medición de movilidad real ni un SLA cloud. La verificación HTTP comprueba
la conexión del consumidor; la inspección visual del navegador se declara aparte en el manifiesto.

## Ejecutar la demo en GCP

Para minimizar la intervencion, ejecutar `P3/conectar_y_validar_gcp.ps1`: abre un unico
inicio de sesion, prepara gcloud y ADC con `--update-adc`, instala el cliente en .runtime
y ejecuta automaticamente la demo BigQuery. Al terminar, Codex puede leer el manifiesto local.
Este flujo fijo se ejecuta desde la terminal del usuario con red; no habilita acceso de red
a una sesion de Codex que lo tenga restringido. Las credenciales permanecen excluidas de Git.

La sesión del navegador no equivale a autenticar los clientes locales. Ejecutar personalmente:

```powershell
.\P3\iniciar_sesion_gcp.ps1
.\P3\ejecutar_demo_gcp.ps1
```

Seleccionar una cuenta con permisos para crear datasets y jobs en bigdata-herles-pinedo.
El segundo script instala el cliente BigQuery en .runtime/python-gcp, carga datos sintéticos
en un dataset p3_fecha_id nuevo, ejecuta el SQL y verifica la API contra las tablas cloud.
Las tablas expiran en siete días; los datos históricos de Lumina no se reemplazan.
Las consultas del pipeline tienen máximo de 1 GiB facturable por job. La ejecución genera
evidencias/bigquery_latest.json con dataset y job IDs solo si termina correctamente.
No equivale a desplegar Cloud Run: la API y el consumidor se sirven localmente contra BigQuery.

No pegar contraseñas, tokens ni códigos en el chat. .runtime/ está excluido de Git.
Los accesos requieren red disponible desde la terminal que ejecute los comandos.

## Calculadora de consumo del primer año

```bash
python P3/calculadora_consumo.py
```

Los supuestos editables están en costos.json: 2 TiB consultados al mes, 100 GiB lógicos
activos, 10,000 solicitudes de API y tiempos facturables declarados. El resultado incluye
20% de contingencia y no descuenta cuotas gratuitas compartidas. Estima USD 216.81
para el primer año del piloto; no dimensiona toda la metrópoli ni un carril IoT continuo.
Fuentes oficiales de tarifas consultadas el 20/11/2026:
https://cloud.google.com/bigquery/pricing y https://cloud.google.com/run/pricing.
Excluye impuestos, personal, egreso, logs excedentes, builds, streaming continuo y DRP.
No es una factura ni un presupuesto aprobado. Revisar tarifas antes de usar el cálculo.

## Guion de sustentación sugerido

1. Contexto y alcance: explicar por qué se eligió OLAP para una demo completa.
2. Arquitectura y KPI: mostrar el grano de viaje y los cruces espaciotemporales.
3. Demo: iniciar el pipeline; mostrar limpieza, Gold, API y tablero con el mismo run_id.
4. Economía: editar un consumo en costos.json y recalcular el primer año.
5. Conclusiones: distinguir lo ejecutado, lo supuesto y lo pendiente; distribuir las preguntas entre el equipo.

La defensa en vivo sigue a cargo del grupo. No se atribuyen contribuciones personales no verificadas.

## Publicación del repositorio

Comprobar que git status no incluya credenciales ni .runtime. Las correcciones deben estar
en un commit y el push debe confirmarse contra el remoto conocido:
https://github.com/AlejandroPinedo/datalandia-lumina-smart-mobility-gcp.
Si el acceso de GitHub ha caducado, el propietario/colaborador debe autenticarlo personalmente.
No usar force-push. El manifiesto de entrega registra el commit y el estado real de publicación.
