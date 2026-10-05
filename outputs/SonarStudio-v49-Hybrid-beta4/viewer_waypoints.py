"""Explicit navigation vs estimated target waypoints, stored with local findings."""
from pathlib import Path
import math
import xml.etree.ElementTree as ET
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QInputDialog,QFileDialog
from viewer_metrology import point_from_pixel

def export_gpx(rows,path):
 ns='http://www.topografix.com/GPX/1/1';ET.register_namespace('',ns)
 root=ET.Element('{'+ns+'}gpx',version='1.1',creator='SonarStudio')
 for row in rows:
  m=row['metadata']
  if not m.get('waypoint'):continue
  lat,lon=float(m['latitude']),float(m['longitude'])
  if not math.isfinite(lat) or not math.isfinite(lon) or not -90<=lat<=90 or not -180<=lon<=180:raise ValueError('Coordenadas inválidas.')
  node=ET.SubElement(root,'{'+ns+'}wpt',lat=str(lat),lon=str(lon))
  for tag,value in [('name',row['tag']),('desc',f"{m['coordinate_reference']} | Ping {row['ping']+1} | {m.get('date','')} {m.get('time','')} | Profundidade do aparelho {m['depth_instrument_m']} m"),('type',m['waypoint_kind'])]:ET.SubElement(node,'{'+ns+'}'+tag).text=str(value)
 target=Path(path);temp=target.with_suffix('.incomplete.gpx');ET.ElementTree(root).write(temp,encoding='utf-8',xml_declaration=True);temp.replace(target)

class WaypointControls:
 def __init__(self,c):
  self.c=c;self.s=c.s;menu=self.s.menuBar().addMenu('Waypoints')
  for text,key,callback in [('Marcar barco no ping atual','Ctrl+W',self.vessel),('Marcar alvo no SideScan','Ctrl+Shift+W',self.arm),('Lista de pontos / achados','',c.show_findings),('Exportar waypoints GPX','',self.export)]:
   action=QAction(text,self.s)
   if key:action.setShortcut(key)
   action.triggered.connect(callback);menu.addAction(action)
  self.s.sonar.waypoint.connect(self.clicked)
 def name(self):
  name,ok=QInputDialog.getText(self.s,'Novo waypoint','Nome do ponto:')
  return name.strip() if ok and name.strip() else None
 def save(self,name,kind,point=None):return self.c.quick_tag(name,waypoint_kind=kind,waypoint_point=point)
 def vessel(self):
  if self.s.recording is None:return
  self.s.pause_playback();name=self.name()
  if name:self.save(name,'vessel_gps')
 def arm(self):
  if self.s.recording is None:return
  if self.s.sonar_is_down:self.s.statusBar().showMessage('Use o SideScan para estimar a posição lateral do alvo.');return
  self.c.tool.setCurrentText('Waypoint');self.s.statusBar().showMessage('Clique no alvo no SideScan; a posição será estimada pela geometria do sonar.')
 def clicked(self,pixel):
  c=self.c
  if c.frame is None or c.filtered is None or c.frame['token']!=c.token or c.frame['index']!=self.s.slider.value():self.s.statusBar().showMessage('Aguarde a janela atual antes de marcar.');return
  try:
   f=c.filtered;left=self.s.sonar_crop[0] if self.s.sonar_crop else 0
   point=point_from_pixel(self.s.recording,c.frame['info'],pixel.x()*f['scale_x']+left,pixel.y()*f['scale_y'],c.altitude_override)
   name=self.name()
   if name:self.save(name,'target_estimated',point)
  except ValueError as error:self.s.statusBar().showMessage(str(error))
  finally:c.tool.setCurrentText('Navegar')
 def export(self):
  if self.s.recording is None:return
  rows=[r for r in self.c.findings.list(self.s.recording.dat) if r['metadata'].get('waypoint')]
  if not rows:self.s.statusBar().showMessage('Nenhum waypoint salvo nesta gravação.');return
  path,_=QFileDialog.getSaveFileName(self.s,'Exportar waypoints',str(self.s.recording.project/'waypoints.gpx'),'GPX (*.gpx)')
  if path:export_gpx(rows,path);self.s.statusBar().showMessage('Waypoints GPX exportados.')
