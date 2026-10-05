"""Export displayed sonar viewports, not reprocessed raw sample arrays."""
from pathlib import Path
import json,math
from PySide6.QtCore import QRect,QByteArray,QBuffer,QIODevice
from PySide6.QtGui import QImage,QPainter,QColor,QFont,QFontDatabase
from PySide6.QtWidgets import QFileDialog

def visible_capture(s,overlay=True):
 c=s.viewer_controller
 if (c.enabled and (c.frame is None or c.last_painted_token!=c.token or c.frame['index']!=s.slider.value())) or (not c.enabled and (s.sonar.image is None or s.frame_index!=s.slider.value())):raise ValueError('Aguarde a imagem do ping selecionado antes de exportar.')
 views=[s.sonar]
 if c.down.isVisible():views.append(c.down)
 if s.humviewer.third.isVisible():views.append(s.humviewer.third)
 images=[]
 for view in views:
  if view.image is None:raise ValueError('Aguarde todos os canais selecionados antes de exportar.')
  pix=view.viewport().grab();im=pix.toImage();im.setDevicePixelRatio(1);images.append(im)
 row=s.recording.data.iloc[s.frame_index]
 metadata={'version':s.windowTitle(),'source_dat':str(s.recording.dat),'selected_ping_zero_based':int(s.frame_index),'latitude':float(row.lat),'longitude':float(row.lon),'depth_instrument_m':float(row.inst_dep_m),'speed_kmh':float(row.speed_ms)*3.6,'date':str(row.get('date','')),'time':str(row.get('time','')),'position_reference':'vessel GPS at selected ping, not target position','screen_view':True,'native_samples':False,'panels':len(images),'cursor_visible':c.show_cursor.isChecked(),'overlay':bool(overlay)}
 horizontal=c.sonar_split.orientation().name=='Horizontal'
 metadata['panel_arrangement']='side_by_side' if horizontal else 'stacked'
 width=sum(im.width() for im in images) if horizontal else max(im.width() for im in images);height=max(im.height() for im in images) if horizontal else sum(im.height() for im in images)
 out=QImage(width,height,QImage.Format.Format_ARGB32);out.fill(QColor('#060d13'));p=QPainter(out);top=0
 for im in images:
  p.drawImage(top if horizontal else 0,0 if horizontal else top,im);top+=im.width() if horizontal else im.height()
 if overlay:
  def number(value,digits):return f'{value:.{digits}f}' if math.isfinite(value) else 'indisponível'
  if 'Noto Sans' not in QFontDatabase.families():QFontDatabase.addApplicationFont(str(Path(__file__).parent/'assets/NotoSans.ttf'))
  font=QFont('Noto Sans',max(9,min(16,width//90)));font.setBold(True);p.setFont(font)
  text=f"Ping {s.frame_index+1} | GPS barco: {number(metadata['latitude'],6)}, {number(metadata['longitude'],6)}\nProf. aparelho: {number(metadata['depth_instrument_m'],2)} m | Velocidade: {number(metadata['speed_kmh'],1)} km/h | {metadata['date']} {metadata['time']}"
  rect=QRect(8,6,max(1,width-16),height-12);bounds=p.boundingRect(rect,0x1000,text);p.fillRect(QRect(0,0,width,min(height,bounds.height()+18)),QColor(5,12,18,185));p.setPen(QColor('#ffffff'));p.drawText(rect,0x1000,text)
 p.end()
 # JSON metadata must remain valid even when navigation values are missing.
 for k,v in list(metadata.items()):
  if isinstance(v,float) and not math.isfinite(v):metadata[k]=None
 out.setText('SonarStudio',json.dumps(metadata,ensure_ascii=False,allow_nan=False))
 return out,metadata

def export_visible(s):
 if s.recording is None:return
 s.pause_playback()
 try:image,metadata=visible_capture(s,s.png_overlay.isChecked())
 except ValueError as error:s.statusBar().showMessage(str(error));return
 path,_=QFileDialog.getSaveFileName(s,'Exportar visão atual dos canais',str(s.recording.project/'sonar-visao-atual.png'),'PNG (*.png)')
 if not path:return
 data=QByteArray();buffer=QBuffer(data);buffer.open(QIODevice.OpenModeFlag.WriteOnly)
 if not image.save(buffer,'PNG'):raise RuntimeError('Não foi possível codificar a captura PNG.')
 buffer.close();payload=bytes(data)
 def work(note,progress,cancel):
  if cancel():raise InterruptedError('Exportação cancelada.')
  target=Path(path);temp=target.with_suffix('.incomplete.png');temp.write_bytes(payload);temp.replace(target);Path(str(target)+'.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');return str(target)
 s.run_job(work,lambda result:s.log.appendPlainText('Visão atual dos canais salva: '+result))
