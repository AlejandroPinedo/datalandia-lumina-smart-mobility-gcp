"""Informe autocontenido P3; no sustituye la presentación ni la defensa en vivo."""
from presentable import build


if __name__=='__main__':
    build('00_DataLandia_Lumina_Entrega3_Demo_OLAP.html',
          'Lumina Entrega 3 Exposición y demo OLAP', ['P3/README.md'],
          include_evidence=False, include_diagrams=False, cloud_verified=True,
          code_files=['existing_project_data.py','olap_service.py'],
          status_notice='P3 utiliza la tabla Gold que ya existía en el proyecto: 4,998,926 registros consultados. No se generan ni cargan viajes. La API sirve el resultado identificado de esa consulta; no se certifica por ello la procedencia externa de todos los datos.',
          extra_files=['P3/demo_e2e.py','P3/usar_datos_existentes.py','P3/verificar_evidencia.py',
                       'P3/calculadora_consumo.py','P3/costos.json','P3/diapositivas.json',
                       'P3/evidencias/estimacion_anual.json','P3/evidencias/datos_existentes_latest.json'])
