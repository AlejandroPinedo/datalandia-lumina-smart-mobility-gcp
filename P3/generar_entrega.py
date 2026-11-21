"""Compilar el informe y la sustentación con Pandoc, desde la evidencia del proyecto."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from urllib.parse import quote
from zipfile import ZipFile

P3 = Path(__file__).resolve().parent
ROOT = P3.parent
OUT = P3 / "entrega"


def load(name):
    return json.loads((P3 / "evidencias" / (name + ".json")).read_text(encoding="utf-8"))


def table(headers, rows):
    return "\n".join("| " + " | ".join(map(str, row)) + " |" for row in [headers, ["---"] * len(headers), *rows])


def number(value):
    return f"{value:,}".replace(",", ".")


def image(path, caption, width):
    return f"![{caption}]({quote(path.relative_to(ROOT).as_posix())}){{width={width}in}}"


def main():
    report = load("validacion_fondo_latest")
    ingest = load("ingesta_demostrada_latest")
    if ingest["status"] != "passed" or report["status"] != "validated_olap_pipeline":
        raise ValueError("Revise las evidencias antes de generar los materiales.")
    slides, document = [], []
    def add(title, brief, detail="", old=None):
        slides.append("# " + title + "\n\n" + brief)
        block = "## " + title + "\n\n" + brief + "\n\n" + detail
        if old:
            block += "\n\n" + image(ROOT / "Capturas de pantalla" / (old[0] + ".png"), old[1], 5.8)
        document.append(block)

    add("¿Qué alternativa conviene para un trayecto?", "El pasajero compara duración, distancia y tarifa observadas. El analista puede seguir el indicador hasta el trabajo de BigQuery que lo produjo.", "La demostración desarrolla el proceso OLAP de extremo a extremo. Los datos proceden de las tablas disponibles en el proyecto de Google Cloud. La consulta de viajes cubre enero y febrero de 2026.")
    add("De cuatro fuentes a una consulta", "**Batch:** fuentes del proyecto → Parquet en Cloud Storage → Bronze aislado → Silver → auditoría → Gold → API → React.\n\n**Eventos:** telemetría → consumidor incremental → estado durable por sensor → diagnóstico y recuperación.", "BigQuery procesa el volumen completo, y el formato Parquet permite transferirlo. La API Node.js consume exportaciones identificadas para reproducir la demostración sin repetir consultas facturables. SQLite conserva el estado de los eventos y permite comprobar un cruce portable sobre 960 registros.")
    add("Reingesta verificada en cuatro tablas", table(["Fuente", "Origen", "Carga"], [(t["source"].split(".")[-1], number(t["source_rows"]), number(t["loaded_rows"])) for t in ingest["tables"]]), "Cada tabla se exportó como Parquet y se cargó en un conjunto aislado. El conteo y una huella agregada de los campos coinciden para las cuatro fuentes. El manifiesto registra los trabajos de exportación, carga y comprobación. Ejecución: " + ingest["run_id"] + ".", ("G4", "Captura histórica del proyecto. Objetos Parquet en el bucket de Cloud Storage."))
    add("De 5 millones de filas a 4.996.009 viajes únicos", "La depuración excluye 3.991 filas. Las cuatro reglas superan el umbral de publicación de 95 %. La regla financiera y temporal alcanza 99,92018 %.", table(["Regla", "Evaluados", "Válidos"], [(q["dimension_calidad"], number(q["registros_evaluados"]), number(q["registros_validos"])) for q in report["quality"]]), ("C2", "Auditoría de BigQuery en la entrega previa. La tabla superior corresponde a las cuatro reglas del cierre."))
    add("El modo multimodal OART concentra el mayor volumen", table(["Modo", "Viajes validados"], [(m["transport_mode"].replace("LUMINA_", "").replace("_", " ").title(), number(m["trips"])) for m in report["olap"]]), "Gold conserva una fila por identificador. El resumen de la validación utiliza este universo depurado. La web de pasajeros usa la exportación de la tabla Gold anterior, con 4.998.926 filas.", ("C1", "Tabla Gold anterior en BigQuery, utilizada para la exploración histórica del pasajero."))
    add("Qué fuentes coinciden con los viajes", "Los 4.996.009 viajes encuentran su zona censal. Tráfico e IoT tienen cero coincidencias temporales: los viajes son de enero y febrero, y ambas fuentes son de noviembre.", "La vista territorial conserva las fechas propias de cada fuente. Para 4.945.660 viajes hay una referencia de automóvil observada en el mismo origen, destino, fecha y hora.")
    add("Alcance de las cuatro verticales", "**OLAP:** consulta funcional por zona y modo.\n\n**Plan diario:** 32 series evaluadas retrospectivamente.\n\n**Tiempo real:** asignación implementada, condicionada a solicitudes y flota vigentes.\n\n**Mantenimiento:** diagnóstico de antigüedad de 11 sensores.", "El modelo se seleccionó con siete días de validación y se evaluó con otros siete días. Para ZDL-001 y bicicleta/OART, la referencia estacional semanal obtuvo un error absoluto medio de 50 viajes/día y WAPE de 9,75 % en prueba. Para estimar fallas hacen falta observaciones sucesivas y etiquetas.")
    add("El consumidor reconoce las reentregas", "El replay procesó 11 eventos históricos y detectó los 11 duplicados al reenviarlos. La restauración del respaldo y la comprobación de integridad pasaron.", "El consumidor registra cada evento antes de confirmar recepción y conserva el último estado por sensor. La prueba de recuperación corresponde al almacenamiento local SQLite.", ("C3", "Tópico de telemetría de Pub/Sub configurado en el proyecto."))
    document.append("## Recorrido de la demostración\n\nDesde la raíz del repositorio ejecute `cd frontend`, `npm ci`, `npm run build` y `npm start`. Abra <http://127.0.0.1:3000/p3>.")
    for file, title, body in [
        ("01-viajes", "Buscar un viaje", "En Viajar, elija origen y destino. Compare las alternativas históricas por tiempo o tarifa. Abra el detalle de una alternativa y guárdela para volver a consultarla."),
        ("02-registros", "Consultar registros", "Abra Registros, aplique un filtro y examine un viaje. Hay hasta tres registros por combinación de origen, destino, condado y modo: 960 ejemplos históricos en total."),
        ("03-tablero", "Explorar el tablero", "Seleccione el período y los modos. Observe el volumen filtrado y las medias ponderadas. La exportación de la tabla Gold previa alimenta esta vista."),
        ("04-validacion", "Reconciliar la validación", "En Validación integral, contraste los viajes únicos, revise el resultado de las cuatro reglas y las fechas de las fuentes. Los identificadores de trabajo permiten rastrear la ejecución en BigQuery.")]:
        pic = OUT / "capturas" / (file + ".png")
        document.append("### " + title + "\n\n" + body + ("\n\n" + image(pic, title + " en el prototipo.", 5.8) if pic.exists() else ""))
        if file in ("01-viajes", "04-validacion"):
            slides.append("# Demo · " + title + "\n\n" + (image(pic, title, 6.4) if pic.exists() else body))
    add("USD 216,81 para el primer año del piloto OLAP", "El escenario supone 2 TiB consultados, 100 GiB almacenados y 10.000 solicitudes al mes. El subtotal mensual es USD 15,06. El cálculo anual incorpora 20 % de contingencia.", "La calculadora está en P3/calculadora_consumo.py y los parámetros en P3/costos.json. La estimación excluye egreso, impuestos, operación continua de IoT y despliegue metropolitano.")
    add("Conclusiones y siguientes pasos", "La consulta histórica tiene conteos conciliados y trabajos trazables. La prueba local atendió 48 solicitudes, con concurrencia 6, cero errores y p95 de 148,52 ms.\n\nPara ampliar el piloto se requieren datos contemporáneos, dimensionamiento bajo carga y pruebas de recuperación regional.", "La evaluación operativa requiere flota, turnos y solicitudes recientes. Para mantenimiento predictivo se necesita historial de fallas por activo. La latencia reportada pertenece a la API local.")
    document.append("## Trazabilidad\n\nEjecución de cierre: " + report["run_id"] + ". Consulte los manifiestos de ingesta y validación en P3/evidencias.\n\n" + table(["Etapa", "Trabajo BigQuery"], [(s["name"], s["job_id"]) for s in report["stages"][:8]]) + "\n\nRepositorio: <https://github.com/AlejandroPinedo/datalandia-lumina-smart-mobility-gcp>")
    pandoc = shutil.which("pandoc") or str(Path(os.environ["LOCALAPPDATA"]) / "Programs/Quarto/bin/tools/pandoc.exe")
    OUT.mkdir(exist_ok=True)
    header = "---\ntitle: 'Lumina | Movilidad urbana en DataLandia'\nsubtitle: 'UTEC · Big Data · Entrega 3'\nauthor: 'Herles Pinedo · David Jimenez · José Nomberto'\n---\n\n"
    with tempfile.TemporaryDirectory(dir=OUT) as tmp:
        for ext, sections in [("docx", document), ("pptx", slides)]:
            source = Path(tmp) / (ext + ".md")
            source.write_text(header + "\n\n".join(sections), encoding="utf-8")
            target = Path(tmp) / ("Lumina-P3." + ext)
            subprocess.run([pandoc, str(source), "--slide-level=1", "-o", str(target)], check=True, cwd=ROOT)
            with ZipFile(target) as package:
                if package.testzip():
                    raise ValueError("Paquete dañado")
            target.replace(OUT / target.name)


if __name__ == "__main__":
    main()
