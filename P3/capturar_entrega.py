"""Captura el prototipo local en un perfil efímero de Chrome y recompila la entrega."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.request import urlopen

from generar_entrega import OUT, ROOT, main as generate


def available(url):
    try:
        with urlopen(url, timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def main():
    chrome = next((Path(os.environ[key]) / "Google/Chrome/Application/chrome.exe"
                   for key in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA")
                   if key in os.environ and (Path(os.environ[key]) / "Google/Chrome/Application/chrome.exe").is_file()), None)
    if chrome is None:
        raise RuntimeError("Falta Google Chrome.")
    base = "http://127.0.0.1:3000"
    server = None
    try:
        if not available(base + "/p3"):
            server = subprocess.Popen(["node", "server.mjs"], cwd=ROOT / "frontend")
            for _ in range(30):
                if available(base + "/p3"):
                    break
                time.sleep(0.25)
        with urlopen(base + "/api/v1/passenger-data", timeout=8) as response:
            routes = json.load(response)["routes"]
        route = next(r for r in routes if r["origin_zone_id"] != r["dest_zone_id"])
        trip = f'?origen={route["origin_zone_id"]}&destino={route["dest_zone_id"]}#viajar'
        views = [("01-viajes", trip, "RESULTADOS DEL TRAYECTO"),
                 ("02-registros", "#registros", "Viajes registrados"),
                 ("03-tablero", "#overview", "Resumen de movilidad"),
                 ("04-validacion", "#operations", "Calidad y cobertura")]
        (OUT / "capturas").mkdir(exist_ok=True, parents=True)
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            for name, suffix, marker in views:
                picture = Path(tmp) / (name + ".png")
                args = [str(chrome), "--headless=new", "--disable-extensions", "--no-first-run",
                        "--disable-background-networking", "--no-proxy-server",
                        "--user-data-dir=" + str(Path(tmp) / "profile"), "--window-size=1440,1050",
                        "--virtual-time-budget=12000"]
                page = base + "/p3" + suffix
                print("Capturando", name, flush=True)
                dom = subprocess.run([*args, "--dump-dom", page], capture_output=True, timeout=45, check=True)
                if marker not in dom.stdout.decode("utf-8", errors="replace"):
                    raise RuntimeError("El prototipo no presentó el contenido esperado para " + name)
                subprocess.run([*args, "--screenshot=" + str(picture), page], capture_output=True, timeout=45, check=True)
                content = picture.read_bytes()
                if not content.startswith(b"\x89PNG\r\n\x1a\n") or len(content) < 18000:
                    raise RuntimeError("Captura incompleta: " + name)
            for name, _, _ in views:
                shutil.copyfile(Path(tmp) / (name + ".png"), OUT / "capturas" / (name + ".png"))
        generate()
        print("Documento y diapositivas actualizados en", OUT)
    finally:
        if server:
            server.terminate()
            server.wait(timeout=10)


if __name__ == "__main__":
    main()
