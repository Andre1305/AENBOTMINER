"""Isolated PDF writer, invoked through UTF-8 JSON stdin. No plaintext PDF files."""
import io,json,base64,sys,hashlib
from pathlib import Path

def write_card(job):
 from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Image,Table,TableStyle
 from reportlab.lib.styles import getSampleStyleSheet
 from reportlab.lib import colors
 from reportlab.pdfbase import pdfmetrics
 from reportlab.pdfbase.ttfonts import TTFont
 from reportlab.lib.utils import ImageReader
 from pypdf import PdfReader,PdfWriter
 from xml.sax.saxutils import escape
 from private_pdf import read_secret
 photo=base64.b64decode(job['png']);meta=job['metadata'];font=Path(__file__).parent/'assets/NotoSans.ttf';pdfmetrics.registerFont(TTFont('SonarSans',str(font)))
 styles=getSampleStyleSheet()
 for style in styles.byName.values():style.fontName='SonarSans'
 styles['Normal'].fontSize=9;styles['Normal'].leading=13;styles['Title'].fontSize=20
 story=[Paragraph('SonarStudio — Ficha de alvo',styles['Title']),Paragraph('Evidência local / posição e dimensões estimadas',styles['Normal']),Spacer(1,12)]
 def p(text):return Paragraph(escape(str(text)),styles['Normal'])
 measurement=meta.get('measurement') or {};height='Não medida'
 if measurement.get('kind')=='shadow height scenarios':height=f"{measurement['height_m']:.3f} m | cenários {measurement['scenario_min_m']:.3f} a {measurement['scenario_max_m']:.3f} m"
 rows=[('Alvo / revisão',meta.get('label','Alvo')+' / '+meta.get('status','manual')),('Gravação / ping',meta['source_name']+' / '+str(meta['ping']+1)),('Data / hora',meta.get('date','')+' '+meta.get('time','')),('Latitude / longitude',f"{meta['latitude']:.7f}, {meta['longitude']:.7f}"),('Referência da posição',meta.get('coordinate_reference','Navegação do barco; alvo não projetado')),('Profundidade instrumental',f"{meta['depth_instrument_m']:.3f} m; referência do transdutor a conferir"),('Altura pela sombra',height),('Canal / classificação',meta.get('channel','both')+' | '+('IA: '+str(meta.get('model',''))+'; score '+str(meta.get('score')) if meta.get('score') is not None else 'Marcação manual; sem confiança de IA'))]
 table=Table([[p(a),p(b)] for a,b in rows],colWidths=[145,370]);table.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(0,-1),colors.HexColor('#eef0f2')),('BOTTOMPADDING',(0,0),(-1,-1),5),('TOPPADDING',(0,0),(-1,-1),5)]));story.extend([table,Spacer(1,12)])
 w,h=ImageReader(io.BytesIO(photo)).getSize();frame=meta.get('native_frame',{});ratio=max(.01,float(frame.get('along_track_m',frame.get('sampling_m',1)))/max(1e-9,float(frame.get('sampling_m',1))));shown_h=h*ratio;scale=min(515/w,220/shown_h);story.extend([Image(io.BytesIO(photo),width=w*scale,height=shown_h*scale),Spacer(1,8),p(f"Recorte nativo: {w} x {h} pixels; paleta aplicada. Proporção de apresentação pelo espaçamento médio entre pings; amostras originais incorporadas, sem redução.")])
 if measurement:
  story.append(p('Medição: '+measurement.get('formula',measurement.get('kind',''))))
  for assumption in measurement.get('assumptions',[]):story.append(p('Hipótese: '+assumption))
 story.extend([Spacer(1,8),p('Precisão de campo não certificada. A faixa de sombra é um envelope de cenários, não um intervalo estatístico. Maré/datum não substitui a altura acústica. Intensidade DN e dB digitais relativos não identificam material nem representam amplitude acústica calibrada.'),p('SHA-256 do recorte: '+hashlib.sha256(photo).hexdigest())])
 buffer=io.BytesIO();SimpleDocTemplate(buffer,pagesize=(595.276,841.89),rightMargin=40,leftMargin=40,topMargin=32,bottomMargin=32,title='Ficha pessoal de alvo SonarStudio').build(story)
 writer=PdfWriter();writer.clone_document_from_reader(PdfReader(io.BytesIO(buffer.getvalue())));writer.add_attachment('eco-nativo.png',photo);writer.add_attachment('metadados.json',json.dumps(meta,ensure_ascii=False,indent=2,allow_nan=False).encode('utf-8'));writer.encrypt(read_secret(job['key']),algorithm='AES-256')
 target=Path(job['output']);target.parent.mkdir(parents=True,exist_ok=True);temporary=target.with_suffix('.incomplete.pdf')
 try:
  with temporary.open('wb') as stream:writer.write(stream)
  check=PdfReader(temporary)
  if not check.is_encrypted or not check.decrypt(read_secret(job['key'])):raise ValueError('Falha na verificação da ficha criptografada.')
  temporary.replace(target)
 finally:temporary.unlink(missing_ok=True)
 return {'file':str(target),'pages':len(check.pages),'encrypted':True,'native_pixels':[w,h]}

if __name__=='__main__':
 try:print(json.dumps(write_card(json.loads(sys.stdin.buffer.read().decode('utf-8')))))
 except Exception as error:print(str(error),file=sys.stderr);sys.exit(1)
