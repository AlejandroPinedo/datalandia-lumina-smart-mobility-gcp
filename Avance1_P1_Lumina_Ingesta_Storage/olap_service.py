"""Servicio de lectura P3: devuelve datos materializados, nunca métricas de ejemplo."""
from datetime import datetime, timezone
from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3
import time


def load_olap_report():
    start = time.monotonic()
    backend = os.getenv("LUMINA_DATA_BACKEND", "sqlite")
    try:
        if backend == "sqlite":
            path = Path(os.environ["LUMINA_SQLITE_PATH"]).resolve()
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
                connection.row_factory = sqlite3.Row
                rows = [dict(row) for row in connection.execute("SELECT * FROM p3_gold_kpi ORDER BY transport_mode")]
                meta = dict(connection.execute("SELECT key, value FROM p3_run_metadata").fetchall())
            source = "sqlite:p3_gold_kpi"
        elif backend == "bigquery":
            import config
            from google.cloud import bigquery
            dataset = os.environ["LUMINA_P3_DATASET"]
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dataset):
                raise ValueError("Dataset no válido")
            client = bigquery.Client(project=config.GCP_PROJECT_ID)
            prefix = f"{config.GCP_PROJECT_ID}.{dataset}"
            rows = [dict(row) for row in client.query(f"SELECT * FROM `{prefix}.p3_gold_kpi` ORDER BY transport_mode").result(timeout=20)]
            meta = {row["key"]: row["value"] for row in client.query(f"SELECT key,value FROM `{prefix}.p3_run_metadata`").result(timeout=20)}
            source = f"bigquery:{prefix}.p3_gold_kpi"
        else:
            raise ValueError("Backend no válido")
        if not rows:
            raise ValueError("Gold no contiene resultados")
        return {"status": "ok", "backend": backend, "source": source,
                "data_origin": meta.get("data_origin", "unknown"),
                "run_id": meta.get("run_id"), "processed_at": meta.get("processed_at"),
                "queried_at": datetime.now(timezone.utc).isoformat(),
                "summary": {"total_trips": sum(int(row["total_trips"]) for row in rows),
                            "total_km": round(sum(float(row["total_km"]) for row in rows), 2),
                            "total_revenue_usd": round(sum(float(row["total_revenue_usd"]) for row in rows
                                if row["transport_mode"] != "PERSONAL_CAR_BENCHMARK"), 2)},
                "by_mode": json.loads(json.dumps(rows, default=str)),
                "latency_ms": round((time.monotonic() - start) * 1000, 2)}
    except (Exception,) as exc:
        # No se exponen rutas, tokens ni detalles internos al consumidor.
        import logging
        logging.getLogger("lumina.olap").warning("Consulta OLAP no disponible (%s)", type(exc).__name__)
        return {"status": "unavailable", "backend": backend,
                "reason": "Ejecuta primero el pipeline P3 y configura el backend correspondiente.",
                "by_mode": [], "summary": None}
