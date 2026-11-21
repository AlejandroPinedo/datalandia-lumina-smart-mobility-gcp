"""Procesador incremental durable: event time, deduplicación, DLQ y recuperación SQLite."""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3


class EventStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, node_id TEXT NOT NULL,
            event_time TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS deadletter(digest TEXT PRIMARY KEY, reason TEXT NOT NULL,
            recorded_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS latest(node_id TEXT PRIMARY KEY,event_id TEXT,event_time TEXT,payload TEXT);
        ''')

    def close(self): self.db.close()

    def process(self, payload):
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str, separators=(',', ':'))
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        try:
            if not isinstance(payload, dict): raise ValueError('El evento debe ser un objeto')
            for key in ['event_id', 'node_id', 'zone_id', 'station_id']:
                if not isinstance(payload.get(key), str) or not payload[key].strip(): raise ValueError('Falta '+key)
            stamp = datetime.fromisoformat(str(payload['timestamp_evento']).replace('Z', '+00:00'))
            if stamp.utcoffset() is None: raise ValueError('Timestamp sin zona horaria')
            timestamp = stamp.astimezone(timezone.utc).isoformat()
            for key, low, high in [('vibration_mms', 0, 100), ('temperature_c', -40, 180), ('passenger_load_pct', 0, 100)]:
                value = payload.get(key)
                if isinstance(value, bool) or value is None: raise ValueError('Medición no válida: '+key)
                value = float(value)
                if not math.isfinite(value) or not low <= value <= high: raise ValueError('Fuera de rango: '+key)
            with self.db:
                previous = self.db.execute('SELECT digest FROM events WHERE event_id=?', (payload['event_id'],)).fetchone()
                if previous:
                    if previous[0] != digest: raise ValueError('event_id reutilizado con otro contenido')
                    return 'duplicate'
                self.db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (payload['event_id'], payload['node_id'], timestamp, digest, serialized))
                self.db.execute('''INSERT INTO latest VALUES(?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET
                  event_id=excluded.event_id,event_time=excluded.event_time,payload=excluded.payload
                  WHERE excluded.event_time > latest.event_time OR
                    (excluded.event_time=latest.event_time AND excluded.event_id > latest.event_id)''',
                  (payload['node_id'], payload['event_id'], timestamp, serialized))
            return 'accepted'
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO deadletter VALUES(?,?,?)', (digest, str(exc), datetime.now(timezone.utc).isoformat()))
            return 'deadletter'

    def counts(self):
        return {name: self.db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in ['events', 'deadletter', 'latest']}

    def latest(self):
        return [json.loads(row[0]) for row in self.db.execute('SELECT payload FROM latest ORDER BY node_id')]

    def backup(self, target):
        other = sqlite3.connect(target)
        try: self.db.backup(other)
        finally: other.close()

    def integrity(self): return self.db.execute('PRAGMA integrity_check').fetchone()[0]
