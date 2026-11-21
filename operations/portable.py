"""Cruce multifuente portable en SQLite sobre los registros existentes exportados."""
import json
import sqlite3
from pathlib import Path


def verify_portable_join(path, trips, zones, traffic, telemetry):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    try:
        db.executescript('''DROP TABLE IF EXISTS trips;DROP TABLE IF EXISTS zones;
          DROP TABLE IF EXISTS traffic;DROP TABLE IF EXISTS telemetry;
          CREATE TABLE trips(trip_id TEXT,origin TEXT,destination TEXT,day TEXT,hour INTEGER,station TEXT);
          CREATE TABLE zones(zone TEXT PRIMARY KEY);
          CREATE TABLE traffic(origin TEXT,destination TEXT,day TEXT,hour INTEGER,
            PRIMARY KEY(origin,destination,day,hour));
          CREATE TABLE telemetry(zone TEXT,station TEXT,day TEXT,hour INTEGER,
            PRIMARY KEY(zone,station,day,hour));''')
        db.executemany('INSERT INTO trips VALUES(?,?,?,?,?,?)', [
            (r['trip_id'],r['origin_zone_id'],r['dest_zone_id'],str(r['pickup_datetime'])[:10],
             int(str(r['pickup_datetime'])[11:13]),r.get('connected_oart_station_id')) for r in trips])
        db.executemany('INSERT OR IGNORE INTO zones VALUES(?)', [(r['zone_id'],) for r in zones])
        db.executemany('INSERT OR IGNORE INTO traffic VALUES(?,?,?,?)', [
            (r['origin_zone_id'],r['dest_zone_id'],str(r['travel_date']),r['hod']) for r in traffic])
        db.executemany('INSERT OR IGNORE INTO telemetry VALUES(?,?,?,?)', [
            (r['zone_id'],r['station_id'],str(r['timestamp_evento'])[:10],int(str(r['timestamp_evento'])[11:13])) for r in telemetry])
        db.commit()
        row = db.execute('''SELECT COUNT(*),COUNT(z.zone),COUNT(f.origin),COUNT(i.zone)
          FROM trips t LEFT JOIN zones z ON t.origin=z.zone
          LEFT JOIN traffic f ON t.origin=f.origin AND t.destination=f.destination AND t.day=f.day AND t.hour=f.hour
          LEFT JOIN telemetry i ON t.origin=i.zone AND t.station=i.station AND t.day=i.day AND t.hour=i.hour''').fetchone()
        if row[0] != len(trips): raise ValueError('El cruce portable multiplica registros')
        return {'status':'passed', 'engine':'SQLite', 'input_records':len(trips),
                'output_records':row[0], 'census_matches':row[1], 'traffic_matches':row[2],
                'iot_matches':row[3], 'scope':'Registros existentes exportados; no prueba de escala metropolitana',
                'integrity_check':db.execute('PRAGMA integrity_check').fetchone()[0]}
    finally: db.close()
