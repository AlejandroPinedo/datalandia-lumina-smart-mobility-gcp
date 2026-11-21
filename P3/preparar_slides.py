"""Genera instrucciones editables de Google Slides a partir de diapositivas.json.

No autentica ni crea archivos externos. El conector ejecuta los lotes cuando se autorice
el método directo. Diseño 16:9 en puntos, con texto y tablas nativas.
"""
import json
from pathlib import Path

BASE=Path(__file__).resolve().parent


def color(hex_value):
    value=hex_value.lstrip('#')
    return {key:int(value[i:i+2],16)/255 for key,i in [('red',0),('green',2),('blue',4)]}


def make_requests(content):
    result=[]
    for index,slide in enumerate(content['slides']):
        sid=f'p3_slide_{index+1:02d}'
        requests=[{'createSlide':{'objectId':sid,'slideLayoutReference':{'predefinedLayout':'BLANK'}}}]
        bg='#102D3B' if index==0 else '#F7FAFC'
        requests.append({'updatePageProperties':{'objectId':sid,'pageProperties':{'pageBackgroundFill':{'solidFill':{'color':{'rgbColor':color(bg)},'alpha':1}}},'fields':'pageBackgroundFill'}})
        count=0
        def shape(text,x,y,w,h,size=18,ink='#132438',bold=False,fill=None):
            nonlocal count
            count+=1;oid=f'{sid}_text_{count}'
            properties={'pageObjectId':sid,'size':{'width':{'magnitude':w,'unit':'PT'},'height':{'magnitude':h,'unit':'PT'}},
                        'transform':{'scaleX':1,'scaleY':1,'translateX':x,'translateY':y,'unit':'PT'}}
            requests.append({'createShape':{'objectId':oid,'shapeType':'TEXT_BOX','elementProperties':properties}})
            sp={'outline':{'propertyState':'NOT_RENDERED'}}
            if fill: sp['shapeBackgroundFill']={'solidFill':{'color':{'rgbColor':color(fill)},'alpha':1}}
            requests.append({'updateShapeProperties':{'objectId':oid,'shapeProperties':sp,'fields':','.join(sp)}})
            requests.append({'insertText':{'objectId':oid,'text':text,'insertionIndex':0}})
            requests.append({'updateTextStyle':{'objectId':oid,'textRange':{'type':'ALL'},'style':{
                'fontFamily':'Arial','fontSize':{'magnitude':size,'unit':'PT'},'bold':bold,
                'foregroundColor':{'opaqueColor':{'rgbColor':color(ink)}}},'fields':'fontFamily,fontSize,bold,foregroundColor'}})
            requests.append({'updateParagraphStyle':{'objectId':oid,'textRange':{'type':'ALL'},
                'style':{'spaceAbove':{'magnitude':0,'unit':'PT'},'spaceBelow':{'magnitude':8,'unit':'PT'},'lineSpacing':110},
                'fields':'spaceAbove,spaceBelow,lineSpacing'}})
            return oid
        if index==0:
            shape('UTEC · BIG DATA · GRUPO 7',42,32,640,28,13,'#9BDDCC',True)
            shape(slide['title'],42,105,640,120,40,'#FFFFFF',True)
            shape(slide['subtitle'],42,245,640,50,21,'#D0E2E9')
            shape(content['authors'],42,315,640,32,16,'#FFFFFF')
            shape('Sustentación final · 21 de noviembre de 2026',42,366,640,22,11,'#9BDDCC')
        else:
            shape(slide.get('section',''),40,18,640,20,10,'#126A60',True)
            shape(slide['title'],40,44,640,60,25,'#132438',True)
            if 'diagram' in slide:
                for j,label in enumerate(slide['diagram']):
                    shape(label,40+j*108,119,96,45,11,'#FFFFFF',True,'#126A60')
                shape('\n'.join(slide['body']),40,190,640,173,16)
            elif 'table' in slide:
                data=[slide['table']['headers']]+slide['table']['rows']
                tid=f'{sid}_table'
                requests.append({'createTable':{'objectId':tid,'rows':len(data),'columns':len(data[0]),
                    'elementProperties':{'pageObjectId':sid,'size':{'width':{'magnitude':640,'unit':'PT'},'height':{'magnitude':200,'unit':'PT'}},
                    'transform':{'scaleX':1,'scaleY':1,'translateX':40,'translateY':112,'unit':'PT'}}}})
                for r,row in enumerate(data):
                    for c,value in enumerate(row):
                        loc={'rowIndex':r,'columnIndex':c}
                        requests.append({'insertText':{'objectId':tid,'cellLocation':loc,'text':value,'insertionIndex':0}})
                        requests.append({'updateTextStyle':{'objectId':tid,'cellLocation':loc,'textRange':{'type':'ALL'},
                            'style':{'fontFamily':'Arial','fontSize':{'magnitude':11,'unit':'PT'},'bold':r==0,
                            'foregroundColor':{'opaqueColor':{'rgbColor':color('#164C43' if r==0 else '#132438')}}},
                            'fields':'fontFamily,fontSize,bold,foregroundColor'}})
                if slide.get('body'): shape('\n'.join(slide['body']),40,324,640,60,11)
            else:
                shape('\n'.join(slide['body']),40,120,640,232,19)
            shape(f'Lumina · Entrega 3                                        {index+1:02d} / {len(content["slides"]):02d}',40,383,640,16,8,'#526779')
        result.append({'slide_id':sid,'title':slide['title'],'requests':requests,'speaker_notes':slide['notes']})
    return result


if __name__=='__main__':
    content=json.loads((BASE/'diapositivas.json').read_text(encoding='utf-8'))
    output=BASE/'slides_requests.json'
    output.write_text(json.dumps(make_requests(content),ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'{len(content["slides"])} diapositivas preparadas; sin escrituras externas')
