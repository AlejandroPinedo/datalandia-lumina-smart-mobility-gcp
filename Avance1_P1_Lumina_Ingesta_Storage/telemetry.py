"""Validación compartida por el consumidor pull y Apache Beam, sin dependencias cloud."""
from datetime import datetime, timezone
import hashlib
import json
import math


def validar_y_enriquecer_evento(raw_payload: str) -> tuple[bool, dict]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        data = json.loads(raw_payload)
        if not isinstance(data, dict):
            raise ValueError("El evento debe ser un objeto JSON")
        for key in ("event_id", "node_id", "zone_id", "county_id", "station_id", "asset_type"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                raise ValueError(f"Identificador requerido: {key}")
        if not isinstance(data.get("timestamp_evento"), str):
            raise ValueError("timestamp_evento debe ser texto ISO 8601 con zona")
        timestamp = datetime.fromisoformat(data["timestamp_evento"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("timestamp_evento debe incluir zona horaria")
        numeric = {}
        for key in ("vibration_mms", "temperature_c", "lat", "lon", "sound_db", "pm25_ugm3", "passenger_load_pct"):
            if isinstance(data.get(key), bool):
                raise ValueError(f"Valor booleano en {key}")
            numeric[key] = float(data[key])
            if not math.isfinite(numeric[key]):
                raise ValueError(f"Valor no finito en {key}")
        vib, temp = numeric["vibration_mms"], numeric["temperature_c"]
        if not 0 < vib <= 100 or not -40 <= temp <= 180:
            raise ValueError("Vibración o temperatura fuera de rango físico")
        if not -90 <= numeric["lat"] <= 90 or not -180 <= numeric["lon"] <= 180:
            raise ValueError("Coordenadas fuera de rango")
        load = numeric["passenger_load_pct"]
        if not load.is_integer() or not 0 <= load <= 100:
            raise ValueError("Ocupación debe ser un entero entre 0 y 100")
        if not 0 <= numeric["sound_db"] <= 200 or numeric["pm25_ugm3"] < 0:
            raise ValueError("Sonido o partículas fuera de rango")
        status = "CRITICAL_PDM" if vib >= 7.5 or temp >= 80 else (
            "WARNING_PDM" if vib >= 5.5 or temp >= 65 else "NORMAL")
        record = {k: data[k].strip() for k in (
            "event_id", "node_id", "zone_id", "county_id", "station_id", "asset_type")}
        record.update(numeric)
        record.update(timestamp_evento=timestamp.astimezone(timezone.utc).isoformat(),
                      passenger_load_pct=int(load), sensor_status=status,
                      requiere_mantenimiento_pdm=status == "CRITICAL_PDM", ts_ingesta_bq=now)
        return True, record
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        return False, {"payload_crudo": raw_payload[:1000], "origen_fuente": "IOT_AOT_STREAM",
                       "motivo_error": f"EVENTO_INVALIDO: {exc}", "ts_error": now}


def persistir_y_confirmar(messages, bq_client, subscriber, subscription, table_iot, table_dlq):
    """Confirma solo filas persistidas. Una excepción deja el lote sin ACK para reintento.

    insertId estable reduce duplicados en reentregas; la deduplicación definitiva
    se realiza por event_id en Silver (entrega al menos una vez).
    """
    groups = {True: [], False: []}
    for received in messages:
        raw = received.message.data.decode("utf-8", errors="replace")
        valid, row = validar_y_enriquecer_evento(raw)
        insert_id = row["event_id"] if valid else hashlib.sha256(received.message.data).hexdigest()
        groups[valid].append((received.ack_id, row, insert_id))
    ack_ids = []
    failures = 0
    for valid, table in ((True, table_iot), (False, table_dlq)):
        batch = groups[valid]
        if not batch:
            continue
        errors = bq_client.insert_rows_json(table, [x[1] for x in batch], row_ids=[x[2] for x in batch])
        failed = {int(error["index"]) for error in errors}
        failures += len(failed)
        ack_ids.extend(x[0] for i, x in enumerate(batch) if i not in failed)
    if ack_ids:
        subscriber.acknowledge(request={"subscription": subscription, "ack_ids": ack_ids})
    if failures:
        raise RuntimeError(f"{failures} filas sin persistir; se dejan sin ACK para reentrega")
    return len(ack_ids)
