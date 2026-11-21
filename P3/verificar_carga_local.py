"""Carga de lectura local sobre datos existentes; no simula viajes ni tráfico de producción."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:3000')
    parser.add_argument('--requests',type=int,default=48)
    parser.add_argument('--concurrency',type=int,default=6)
    args=parser.parse_args()
    if not 1<=args.requests<=200 or not 1<=args.concurrency<=16:raise ValueError('Carga local acotada requerida')
    if args.base_url not in ['http://127.0.0.1:3000','http://127.0.0.1:8080']:raise ValueError('Solo se permite el servidor local del proyecto')
    def read(_):
        started=time.perf_counter()
        with urllib.request.urlopen(args.base_url+'/api/v1/passenger-data',timeout=15) as response:
            data=json.load(response)
            if data['status']!='ok' or data['data_origin']!='existing_project_data':raise ValueError('Respuesta sin fuente válida')
            return {'latency_ms':(time.perf_counter()-started)*1000,'job_id':data['job_id'],'status':response.status}
    started=time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:results=list(executor.map(read,range(args.requests)))
    elapsed=time.perf_counter()-started;latencies=sorted(r['latency_ms'] for r in results)
    percentile=lambda p:latencies[min(len(latencies)-1,max(0,int((len(latencies)-1)*p)))]
    report={'status':'passed','measured_at':datetime.now(timezone.utc).isoformat(),
        'scope':'Lectura HTTP local; no acredita carga metropolitana ni SLA de GCP',
        'requests':args.requests,'concurrency':args.concurrency,'errors':0,
        'elapsed_seconds':elapsed,'requests_per_second':len(results)/elapsed,
        'p50_ms':percentile(.5),'p95_ms':percentile(.95),'p99_ms':percentile(.99),
        'source_jobs':sorted({r['job_id'] for r in results}),'generated_business_records':0}
    (ROOT/'P3/evidencias/carga_local_latest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
