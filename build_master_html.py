"""Regenera la presentación de arquitectura desde los documentos actuales."""
from presentable import build


if __name__ == "__main__":
    build("PROYECTO_MASTER_DATALANDIA_LUMINA.html", "Lumina Arquitectura de datos",
          ["Avance1_P1_Blueprint_Ingesta_Storage_DataLandia.md",
           "Avance1_Examen_Sustentacion_Tecnica_Economica_Resiliencia.md"])
