"""Informe autocontenido P3; no sustituye la presentación ni la defensa en vivo."""
from presentable import build


if __name__=='__main__':
    build('00_DataLandia_Lumina_Entrega3_Demo_OLAP.html',
          'Lumina Entrega 3 Exposición y demo OLAP', ['P3/README.md'],
          include_evidence=False, include_diagrams=False,
          extra_files=['P3/demo_e2e.py','P3/olap_sql.py','P3/calculadora_consumo.py',
                       'P3/costos.json','P3/diapositivas.json','P3/evidencias/sqlite_latest.json',
                       'P3/evidencias/estimacion_anual.json'])
