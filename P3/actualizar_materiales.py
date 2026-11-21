"""Actualiza la fuente de la sustentación con la consulta existente, sin producir datos."""
import json
from pathlib import Path

BASE=Path(__file__).resolve().parent


def main():
    report=json.loads((BASE/'evidencias/datos_existentes_latest.json').read_text(encoding='utf-8'))
    if report.get('data_origin')!='existing_project_data' or report.get('status')!='ok':
        raise ValueError('Solo se admiten resultados de las tablas existentes')
    path=BASE/'diapositivas.json'
    deck=json.loads(path.read_text(encoding='utf-8'))
    s=deck['slides']; summary=report['summary']; rows=summary['total_trips']
    s[3]['title']=f'La consulta utiliza los {rows:,} registros existentes'
    s[3]['table']={'headers':['Indicador','Resultado consultado','Fuente'], 'rows':[
        ['Registros',f'{rows:,}','COUNT(*) del Gold existente'],
        ['Distancia',f'{summary["total_km"]:,.2f} km','SUM(trip_distance_km)'],
        ['Importes Lumina',f'USD {summary["total_revenue_usd"]:,.2f}','Suma de tarifas; excluye benchmark'],
        ['Desglose',f'{len(report["by_mode"])} modos en la fuente','transport_mode'],
        ['Cobertura explícita de cruces','No disponible en el esquema','No se inventan porcentajes']]}
    s[3]['notes']=f'Consulta de solo lectura. Tabla: {report["source_table"]["table_id"]}. Job: {report["job_id"]}. Consulta: {report["queried_at"]}. Los importes son agregados de las columnas existentes, no facturación auditada. La procedencia externa sigue sin verificación independiente.'
    s[9]['body']=['Consultar con P3/usar_datos_existentes.ps1 sin generar ni cargar registros',
                 f'Mostrar el Gold existente: {rows:,} filas y su esquema',
                 'Abrir /api/v1/olap-kpis y contrastar tabla fuente, fecha y job ID',
                 'Abrir /p3: filtrar fecha, condado, hora y modos; inspeccionar una barra',
                 'Comparar dos modos, consultar la selección y exportar el CSV con trazabilidad']
    s[9]['notes']=f'Evidencia vigente: P3/evidencias/datos_existentes_latest.json. API y consumidor verificados. La fuente no fue modificada por este proceso. Job {report["job_id"]}. La exportación local se identifica y no se confunde con una consulta remota en vivo.'
    s[10]['body']=['P3 consulta los registros que ya estaban disponibles en el proyecto',
                  f'La API y el tablero usan los agregados de {rows:,} registros',
                  'No se generan viajes ni se rellenan indicadores ausentes con cifras de ejemplo',
                  'La evidencia anterior de 600 viajes está archivada y no se usa en la entrega vigente']
    s[11]['body'][0]='Conservar tabla fuente, esquema, fecha y job ID de la consulta existente ya verificada'
    path.write_text(json.dumps(deck,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Materiales actualizados con {rows:,} registros de la fuente existente')


if __name__=='__main__': main()
