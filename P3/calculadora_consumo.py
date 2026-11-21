"""Estimación editable del primer año para el piloto OLAP, sin prometer costos de producción."""
import argparse
import json
import math
from pathlib import Path


def calculate(config):
    usage, prices = config['usage_monthly'], config['prices_usd']
    values = list(usage.values()) + list(prices.values()) + [config['months'],config['contingency_pct']]
    if any(not isinstance(v,(float,int)) or not math.isfinite(v) or v<0 for v in values):
        raise ValueError('Los consumos y precios deben ser números finitos no negativos')
    if config.get('free_tier_applied'):
        raise ValueError('Este escenario conservador no descuenta cuotas gratuitas compartidas')
    lines = {
        'bigquery_queries':usage['bigquery_scanned_tib']*prices['bigquery_per_tib'],
        'bigquery_storage':usage['bigquery_logical_storage_gib']*prices['bigquery_storage_per_gib_month'],
        'cloud_run_cpu':usage['cloud_run_vcpu_seconds']*prices['cloud_run_per_vcpu_second'],
        'cloud_run_memory':usage['cloud_run_gib_seconds']*prices['cloud_run_per_gib_second'],
        'cloud_run_requests':usage['cloud_run_requests']/1e6*prices['cloud_run_per_million_requests'],
    }
    subtotal=sum(lines.values())
    monthly=subtotal*(1+config['contingency_pct']/100)
    return {'scenario':config['scenario'],'currency':config['currency'],
            'monthly_lines':{k:round(v,6) for k,v in lines.items()},'monthly_subtotal':round(subtotal,4),
            'contingency_pct':config['contingency_pct'],'monthly_with_contingency':round(monthly,4),
            'first_year_with_contingency':round(monthly*config['months'],2),
            'sources':config['sources'],'excluded':config['excluded'],
            'limitation':'Estimación del piloto con supuestos; no es factura, presupuesto aprobado ni TCO metropolitano.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs',type=Path,default=Path(__file__).with_name('costos.json'))
    args=parser.parse_args()
    result=calculate(json.loads(args.inputs.read_text(encoding='utf-8')))
    output=Path(__file__).parent/'evidencias'/'estimacion_anual.json'
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))
