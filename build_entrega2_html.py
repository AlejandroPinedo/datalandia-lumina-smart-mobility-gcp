"""Regenera P2 con documentación y código vigentes, sin rutas personales."""
from presentable import build


if __name__ == "__main__":
    build("00_DataLandia_Lumina_Entrega2_Procesamiento_Transformacion.html",
          "Lumina Procesamiento y modelado de movilidad",
          ["Avance2_P2_Procesamiento_Transformacion_Modelado_DataLandia.md",
           "GUIA_PASO_A_PASO_IMPLEMENTACION_LUMINA.md"])
