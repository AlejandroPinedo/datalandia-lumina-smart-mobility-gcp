"""Diseño editable de la sustentación y vistas previas con la misma composición."""
import io
import json
import math
from pathlib import Path
import re
import shutil
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape
from zipfile import ZipFile, ZIP_DEFLATED

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "entrega"
PREVIEW = ROOT.parent / ".runtime" / "revision" / "slides"
CYAN, INK, GRAY, LIGHT, WHITE = "00B5CD", "171B22", "56636C", "EAF7F9", "FFFFFF"
W, H = 1600, 900
P = "http://schemas.openxmlformats.org/presentationml/2006/main"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
EMU = 7620  # 1600 px = 13 1/3 pulgadas.


def num(value):
    return f"{value:,}".replace(",", ".")


class Slide:
    def __init__(self, title, source="", dark=False):
        self.xml = []
        self.rels = []
        self.pic = Image.new("RGB", (W, H), "#" + (INK if dark else WHITE))
        self.draw = ImageDraw.Draw(self.pic)
        self.fg = WHITE if dark else INK
        self.rect(64, 65, 70, 7, CYAN)
        self.text(title, 64, 95, 1450, 135, 51, self.fg, True)
        if source:
            self.text(source, 64, 843, 1460, 34, 21, "A8BBC4" if dark else GRAY)

    def rect(self, x, y, w, h, color, shape="rect"):
        n = len(self.xml) + 2
        self.xml.append(f'<p:sp><p:nvSpPr><p:cNvPr id="{n}" name="Forma {n}"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr><p:spPr><a:xfrm><a:off x="{int(x*EMU)}" y="{int(y*EMU)}"/><a:ext cx="{int(w*EMU)}" cy="{int(h*EMU)}"/></a:xfrm><a:prstGeom prst="{shape}"><a:avLst/></a:prstGeom><a:solidFill><a:srgbClr val="{color}"/></a:solidFill><a:ln><a:noFill/></a:ln></p:spPr></p:sp>')
        if shape == "downArrow":
            self.draw.polygon([(x+w*.25,y),(x+w*.75,y),(x+w*.75,y+h*.6),(x+w,y+h*.6),(x+w*.5,y+h),(x,y+h*.6),(x+w*.25,y+h*.6)], fill="#"+color)
        elif shape == "rightArrow":
            self.draw.polygon([(x,y+h*.25),(x+w*.6,y+h*.25),(x+w*.6,y),(x+w,y+h*.5),(x+w*.6,y+h),(x+w*.6,y+h*.75),(x,y+h*.75)], fill="#"+color)
        else:
            self.draw.rectangle((x,y,x+w,y+h), fill="#"+color)

    def text(self, content, x, y, w, h, size=31, color=INK, bold=False):
        font = ImageFont.truetype("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf", size)
        lines = []
        for paragraph in content.split("\n"):
            line = ""
            for word in paragraph.split():
                candidate = (line + " " + word).strip()
                if font.getlength(candidate) > w and line:
                    lines.append(line)
                    line = word
                else:
                    line = candidate
            lines.append(line)
        line_height = size * 1.23
        if len(lines) * line_height > h + 3:
            raise ValueError(f"Texto desbordado: {content[:65]} ({len(lines)*line_height} > {h})")
        n = len(self.xml) + 2
        paragraphs = []
        for line in lines:
            paragraphs.append(f'<a:p><a:pPr><a:lnSpc><a:spcPts val="{int(line_height*60)}"/></a:lnSpc></a:pPr><a:r><a:rPr lang="es-PE" sz="{int(size*60)}" b="{int(bold)}"><a:solidFill><a:srgbClr val="{color}"/></a:solidFill><a:latin typeface="Arial"/></a:rPr><a:t xml:space="preserve">{escape(line)}</a:t></a:r><a:endParaRPr sz="{int(size*60)}"/></a:p>')
        self.xml.append(f'<p:sp><p:nvSpPr><p:cNvPr id="{n}" name="Texto {n}"/><p:cNvSpPr txBox="1"/><p:nvPr/></p:nvSpPr><p:spPr><a:xfrm><a:off x="{int(x*EMU)}" y="{int(y*EMU)}"/><a:ext cx="{int(w*EMU)}" cy="{int(h*EMU)}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:noFill/><a:ln><a:noFill/></a:ln></p:spPr><p:txBody><a:bodyPr wrap="none" lIns="0" tIns="0" rIns="0" bIns="0"/><a:lstStyle/>{"".join(paragraphs)}</p:txBody></p:sp>')
        for i, line in enumerate(lines):
            self.draw.text((x,y+i*line_height), line, font=font, fill="#"+color)

    def picture(self, name, x, y, w, h):
        path = OUT / "capturas" / (name + ".png")
        source = Image.open(path).convert("RGB")
        scale = min(w/source.width, h/source.height)
        width, height = round(source.width*scale), round(source.height*scale)
        x, y = x+(w-width)/2, y+(h-height)/2
        self.pic.paste(source.resize((width,height), Image.Resampling.LANCZOS), (round(x),round(y)))
        rid = "rIdPicture" + str(len(self.rels))
        self.rels.append((rid, name))
        n = len(self.xml) + 2
        self.xml.append(f'<p:pic><p:nvPicPr><p:cNvPr id="{n}" name="{name}"/><p:cNvPicPr><a:picLocks noChangeAspect="1"/></p:cNvPicPr><p:nvPr/></p:nvPicPr><p:blipFill><a:blip r:embed="{rid}"/><a:stretch><a:fillRect/></a:stretch></p:blipFill><p:spPr><a:xfrm><a:off x="{int(x*EMU)}" y="{int(y*EMU)}"/><a:ext cx="{int(width*EMU)}" cy="{int(height*EMU)}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></p:spPr></p:pic>')

    def node(self, title, body, x, y, w=260):
        self.rect(x,y,w,170,LIGHT)
        self.rect(x,y,6,170,CYAN)
        self.text(title,x+22,y+20,w-40,52,29,INK,True)
        self.text(body,x+22,y+77,w-40,86,25,GRAY)

    def arrow(self, x, y, w=47):
        self.rect(x,y,w,30,CYAN,"rightArrow")

    def export(self, index):
        self.pic.save(PREVIEW / f"{index:02}.png")
        bg = self.pic.getpixel((0,0))
        color = "%02X%02X%02X" % bg
        return f'<?xml version="1.0" encoding="UTF-8"?><p:sld xmlns:p="{P}" xmlns:a="{A}" xmlns:r="{R}"><p:cSld><p:bg><p:bgPr><a:solidFill><a:srgbClr val="{color}"/></a:solidFill><a:effectLst/></p:bgPr></p:bg><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>{"".join(self.xml)}</p:spTree></p:cSld><p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr></p:sld>'.encode()


def design():
    report = json.loads((ROOT/"evidencias/validacion_fondo_latest.json").read_text("utf-8"))
    slides = []
    def new(title, source="", dark=False):
        s=Slide(title,source,dark)
        slides.append(s)
        return s
    s=new("Lumina\nMovilidad urbana en DataLandia",dark=True)
    s.text("Sustentación de la implementación",65,300,1000,70,40,CYAN)
    s.text("UTEC · Big Data · Entrega 3",65,400,1000,60,31,WHITE)
    s.text("Herles Pinedo\nDavid Jimenez\nJosé Nomberto",65,590,800,160,30,WHITE)
    s.rect(1180,280,300,390,CYAN)
    s.text("4,996 M",1210,330,250,100,58,INK,True)
    s.text("viajes únicos\nvalidados",1210,450,250,140,35,INK)

    s=new("Resumen ejecutivo\nUn trayecto, varias alternativas")
    s.text("La pregunta del pasajero",65,265,650,65,32,GRAY)
    s.text("¿Cómo conecto mi origen y mi destino?",65,335,650,160,48,INK,True)
    s.text("Comparar tiempos y tarifas históricos por modo.",65,545,640,120,33,GRAY)
    s.node("Integrar","Cuatro fuentes disponibles",830,270,530)
    s.rect(1070,455,36,42,CYAN,"downArrow")
    s.node("Consultar","Alternativas por origen y destino",830,510,530)
    s.text("Alcance implementado · análisis histórico de movilidad (OLAP)",65,750,1450,65,31,INK,True)

    s=new("Situación actual\nLas fuentes tienen fechas distintas","Fuente · manifiestos de ingesta y validación, 21/11/2026.")
    for i,(n,v,d) in enumerate([("Viajes","5.000.000","Enero y febrero"),("Tráfico","504","10 de noviembre"),("Sensores","11","11 de noviembre"),("Censo","8 zonas","Referencia territorial")]):
        x=65+i*380
        s.text(v,x,280,350,85,57,CYAN,True)
        s.text(n,x,385,350,60,33,INK,True)
        s.text(d,x,445,345,85,28,GRAY)
    s.rect(65,590,1420,6,"D3E2E6")
    s.rect(65,580,250,26,CYAN)
    s.rect(1295,580,190,26,INK)
    s.text("ENE — FEB\nViajes",65,635,450,110,30)
    s.text("NOVIEMBRE\nTráfico y telemetría",1080,635,410,110,30)

    s=new("Criterios de éxito\nCalidad, integridad y rendimiento","Fuente · validacion_fondo_latest.json y carga_local_latest.json.")
    rows=[("Carga fiel","4 de 4 tablas","Conteo y huella coinciden"),("Calidad","99,92018 %","Mínimo observado · umbral 95 %"),("Unicidad","100 %","4.996.009 IDs / 4.996.009 filas"),("API local","148,52 ms","p95 · 48 solicitudes · 0 errores")]
    for i,(label,value,body) in enumerate(rows):
        y=260+i*135
        s.text(label,65,y,340,55,32,GRAY)
        s.text(value,440,y,480,65,45,CYAN,True)
        s.text(body,940,y+8,590,90,29)

    s=new("Productos tecnológicos\nCada herramienta cumple una función")
    for i,(a,b,c) in enumerate([("Conservar","GCS + Parquet","Archivos por fuente"),("Procesar","BigQuery + SQL","Silver, calidad y Gold"),("Reproducir eventos","Python + SQLite","Estado durable por sensor"),("Consultar","Node.js + React","API y web de pasajeros")]):
        y=255+i*130
        s.text(a,65,y,350,70,33,INK,True)
        s.text(b,460,y,510,65,36,CYAN,True)
        s.text(c,1000,y,500,90,30,GRAY)
    s.text("Pub/Sub tiene un tópico configurado. Cloud Run es el alojamiento previsto para la API.",65,790,1460,45,25,GRAY)

    s=new("Dependencias y supuestos\nQué requiere el siguiente nivel operativo")
    for i,(t,b) in enumerate([("Asignar vehículos","Flota disponible, turnos y solicitudes vigentes."),("Anticipar fallas","Historial por activo y fallas etiquetadas."),("Dimensionar el año","Consumo mensual constante y doce meses de operación.")]):
        y=270+i*165
        s.rect(65,y,8,112,CYAN)
        s.text(t,100,y,560,70,35,INK,True)
        s.text(b,700,y,770,110,33,GRAY)
    s.text("El subtotal económico excluye GCS, streaming continuo y recuperación multirregional.",65,790,1470,60,25,GRAY)

    s=new("Arquitectura final\nDel archivo a la decisión del pasajero")
    for i,(a,b) in enumerate([("GCS / Parquet","Reingesta multifuente"),("Bronze / Silver","Depurar y auditar"),("Gold / API","Agregar y exponer"),("React","Consultar alternativas")]):
        x=65+i*380
        s.node(a,b,x,270,330)
        if i<3:s.arrow(x+337,335,35)
    s.rect(445,480,710,105,INK)
    s.text("BigQuery · 2 TiB consultados y 100 GiB al mes",465,500,670,75,29,WHITE)
    s.text("Cloud Run previsto\n10.000 solicitudes/mes",1190,490,345,110,28,GRAY)
    s.node("Telemetría","Replay de eventos históricos",65,650,400)
    s.arrow(485,720)
    s.node("Python + SQLite","Estado y recuperación local",550,650,450)
    s.text("GCS e IoT requieren\ncosto adicional.",1125,670,400,100,29,GRAY)

    s=new("Procesamiento\nLa depuración conserva una fila por viaje","Fuente · ejecución 20261121_061102_f4eb33.")
    for i,(t,b) in enumerate([("5.000.000","Filas de entrada"),("3.991","Filas excluidas"),("4.996.009","Viajes únicos en Gold")]):
        x=65+i*520
        s.text(t,x,300,480,100,64,CYAN if i!=1 else GRAY,True)
        s.text(b,x,420,450,100,32)
        if i<2:s.text("−" if i==0 else "=",x+450,335,65,90,52,GRAY,True)
    s.rect(65,650,1460,110,LIGHT)
    s.text("Cuatro reglas controlan la publicación. Cada una debe alcanzar al menos 95 %.",95,675,1400,75,34,INK,True)

    s=new("Resultados OLAP\nMultimodal OART concentra el 38 %","Fuente · Gold validado, 4.996.009 viajes únicos. Porcentajes redondeados.")
    labels={"LUMINA_MULTIMODAL_OART":"Multimodal OART","LUMINA_EV_MINIVAN_POOL":"Minivan eléctrica","PERSONAL_CAR_BENCHMARK":"Auto de referencia","LUMINA_BUS_RAPID":"Bus rápido","LUMINA_BIKE_AND_OART":"Bicicleta + OART"}
    modes=sorted(report["olap"],key=lambda m:m["trips"],reverse=True)
    maximum=modes[0]["trips"]
    for i,m in enumerate(modes):
        y=265+i*104
        s.text(labels[m["transport_mode"]],65,y+6,420,65,31)
        s.rect(490,y,m["trips"]/maximum*710,42,CYAN if i==0 else "84D9E3")
        s.text(num(m["trips"]),1230,y,310,60,31,INK,True)
        s.text(f'{m["trips"]/4996009*100:.1f} %'.replace(".",","),490,y+48,350,40,23,GRAY)

    s=new("Calculadora de consumo\nUSD 216,81 para el primer año","Fuente · costos.json. Tarifas guardadas el 20/11/2026. Piloto OLAP con 20 % de contingencia.")
    for i,(label,v,detail) in enumerate([("Consultas BigQuery",12.5,"2 TiB al mes"),("Almacenamiento",2.3,"100 GiB al mes"),("API en Cloud Run",.2565,"CPU, memoria y 10.000 peticiones")]):
        y=255+i*112
        s.text(label,65,y,430,65,31,INK,True)
        s.rect(500,y,v/12.5*550,36,CYAN if i==0 else "84D9E3")
        amount=f"{v:.2f}" if v>=1 else f"{v:.4f}"
        s.text("USD "+amount.replace(".",","),1110,y,420,60,34,INK,True)
        s.text(detail,500,y+45,900,50,25,GRAY)
    s.rect(65,645,1460,140,INK)
    s.text("15,0565 USD/mes × 12 meses × 1,20",95,665,930,100,38,WHITE)
    s.text("USD 216,81",1120,665,385,85,43,CYAN,True)
    s.text("Recalcular · editar P3/costos.json y ejecutar python P3/calculadora_consumo.py",65,795,1460,40,24,GRAY)

    s=new("Demo de extremo a extremo\nDel manifiesto al trayecto")
    for i,(t,b) in enumerate([("Reingesta","Conciliar las cuatro tablas"),("Validación","Revisar las reglas y el total"),("Consulta","Elegir origen y destino"),("Detalle","Examinar viajes registrados")]):
        x=65+i*380
        s.node(t,b,x,310,330)
        if i<3:s.arrow(x+337,375,35)
    s.text("Evidencia que veremos",65,575,900,70,35,INK,True)
    s.text("4 tablas conciliadas     4.996.009 viajes únicos     5 alternativas en un trayecto",65,670,1460,100,35,CYAN,True)

    demos=[
        ("Consulta del pasajero","01-viajes","Origen y destino","5 alternativas observadas en 27.340 registros para este trayecto."),
        ("Viajes registrados","02-registros","Detalle consultable","960 ejemplos exportados con trayecto, duración y tarifa."),
        ("Resumen de movilidad","03-tablero","Patrones históricos","4.998.926 registros en el Gold histórico que alimenta esta vista."),
        ("Validación integral","04-validacion","Ejecución del cierre","4.996.009 viajes únicos, 32 series evaluadas y 11 eventos procesados.")]
    for title,pic,label,body in demos:
        s=new("Demo · "+title,"Captura real del prototipo local proporcionada por el equipo.")
        s.picture(pic,55,255,1135,570)
        s.rect(1220,285,6,420,CYAN)
        s.text(label,1250,300,290,120,34,INK,True)
        s.text(body,1250,445,290,280,30,GRAY)

    s=new("Conclusiones\nQué demuestra la implementación",dark=True)
    for i,(t,b) in enumerate([("Datos trazables","Conteos conciliados y trabajos de BigQuery identificados."),("Consulta funcional","Alternativas históricas por trayecto y análisis de movilidad."),("Modelos y resiliencia","32 series evaluadas y recuperación local de 11 eventos.")]):
        y=280+i*170
        s.text(t,65,y,550,65,39,CYAN,True)
        s.text(b,710,y,790,120,34,WHITE)

    s=new("Next Steps\nDel piloto a la operación")
    for i,(t,b) in enumerate([("Actualizar fuentes","Medir coincidencias de viajes, tráfico e IoT del mismo período."),("Conectar la flota","Evaluar asignaciones con disponibilidad y turnos vigentes."),("Ampliar sensores","Evaluar fallas con series por activo y etiquetas."),("Ensayar en nube","Medir p95, recuperación regional y costo de la carga real.")]):
        y=260+i*135
        s.rect(65,y+5,12,90,CYAN)
        s.text(t,105,y,480,65,35,INK,True)
        s.text(b,650,y,850,100,32,GRAY)
    assert len(slides)==17
    return slides


def main():
    PREVIEW.mkdir(parents=True,exist_ok=True)
    slides=design()
    target=OUT/"Lumina-P3.pptx"
    with ZipFile(target) as z:
        package={n:z.read(n) for n in z.namelist()}
    presentation=ET.fromstring(package["ppt/presentation.xml"])
    size=presentation.find(f"{{{P}}}sldSz")
    size.set("cx",str(W*EMU));size.set("cy",str(H*EMU))
    package["ppt/presentation.xml"]=ET.tostring(presentation,encoding="utf-8",xml_declaration=True)
    for i,s in enumerate(slides,1):
        package[f"ppt/slides/slide{i}.xml"]=s.export(i)
        relationships=f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{R}/slideLayout" Target="../slideLayouts/slideLayout2.xml"/>'
        for rid,name in s.rels:
            relationships+=f'<Relationship Id="{rid}" Type="{R}/image" Target="../media/{name}.png"/>'
            package[f"ppt/media/{name}.png"]=(OUT/"capturas"/(name+".png")).read_bytes()
        package[f"ppt/slides/_rels/slide{i}.xml.rels"]=(relationships+"</Relationships>").encode()
    temporary=PREVIEW/"diseno.pptx"
    with ZipFile(temporary,"w",ZIP_DEFLATED) as z:
        for name,data in package.items():
            z.writestr(name,data)
    with ZipFile(temporary) as z:
        assert z.testzip() is None
        for name in z.namelist():
            if name.endswith(".xml"):ET.fromstring(z.read(name))
    shutil.copyfile(temporary,target)
    montage=Image.new("RGB",(1600,math.ceil(len(slides)/3)*300),"#D9E0E3")
    for i,s in enumerate(slides):
        montage.paste(s.pic.resize((520,293)),((i%3)*535,(i//3)*300))
    montage.save(PREVIEW/"montaje.jpg")
    print("17 diapositivas editables y 17 vistas previas generadas.")


if __name__=="__main__":
    main()
