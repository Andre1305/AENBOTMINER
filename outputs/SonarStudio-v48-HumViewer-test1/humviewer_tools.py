"""Independent HumViewer-inspired navigation controls; source recordings stay read-only."""
import numpy as np
from PySide6.QtCore import Qt,QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QHBoxLayout,QComboBox,QCheckBox,QPushButton,QSpinBox,QLabel,QApplication,QInputDialog,QColorDialog,QFileDialog

class HumViewerTools:
 def __init__(self,studio,layout):
  self.s=studio;self.c=studio.viewer_controller
  row=QHBoxLayout();row.addWidget(QLabel('Layout'))
  self.layout=QComboBox();self.layout.addItems(['Um painel','Dois painéis — vertical','Dois painéis — horizontal','Três painéis']);row.addWidget(self.layout)
  self.channel=QComboBox();self.channel.setMinimumWidth(150);row.addWidget(self.channel)
  self.reverse=QCheckBox('Reproduzir ao contrário');row.addWidget(self.reverse)
  self.grid=QCheckBox('Grade');self.grid.setToolTip('Grade visual de oito divisões; use a Régua para distâncias físicas.');row.addWidget(self.grid)
  self.seconds=QSpinBox();self.seconds.setRange(0,2147483647);self.seconds.setSuffix(' s desde início');row.addWidget(self.seconds)
  go=QPushButton('Ir');go.clicked.connect(self.goto);row.addWidget(go)
  self.units=QComboBox();self.units.addItems(['Metros / km/h','Pés / nós']);row.addWidget(self.units)
  layout.insertLayout(1,row)
  self.layout.currentIndexChanged.connect(self.relayout);self.channel.currentIndexChanged.connect(self.select_channel)
  self.grid.toggled.connect(self.toggle_grid)
  self.channel.addItem('DownScan automático',True)
  from app import SonarView
  self.third=SonarView();self.third.setMinimumHeight(100);self.third.hide();self.c.sonar_split.addWidget(self.third)
  self.third.seek.connect(self.seek_third)
  self.timer=QTimer(studio);self.timer.setInterval(250);self.timer.timeout.connect(self.update);self.timer.start();self.recording=None
  menu=studio.menuBar().addMenu('Navegação HumViewer')
  for text,key,callback in [('Play / pausa','P',studio.toggle_playback),('Reprodução reversa','B',lambda:self.reverse.toggle()),('Grade','G',lambda:self.grid.toggle()),('Anterior','Left',lambda:self.step(-1)),('Próximo','Right',lambda:self.step(1)),('Copiar posição do eco','Ctrl+Shift+C',self.copy_position),('Encontrar mudança de profundidade','Ctrl+D',self.depth_change)]:
   action=QAction(text,studio);action.setShortcut(key);action.triggered.connect(callback);menu.addAction(action)
  self.status=QLabel();studio.statusBar().addPermanentWidget(self.status)
  for text,callback in [('Editar paleta de cinco cores',self.edit_palette),('Exportar vídeo AVI',self.movie),('Imprimir visualização',self.print_view)]:
   action=QAction(text,studio);action.triggered.connect(callback);menu.addAction(action)
 def relayout(self):
  horizontal=self.layout.currentIndex()==2
  self.c.sonar_split.setOrientation(Qt.Orientation.Horizontal if horizontal else Qt.Orientation.Vertical)
  self.c.down.setMaximumHeight(16777215);self.c.sonar_split.setSizes([600,400])
  self.third.setVisible(self.layout.currentIndex()==3);self.c.third_enabled=self.layout.currentIndex()==3
  self.c.sonar_split.setSizes([400,200,200] if self.c.third_enabled else [600,400,0])
  self.c.latest_descriptor=None
  self.c.split.setChecked(self.layout.currentIndex()!=0)
  self.s.schedule_sonar()
 def seek_third(self,point):
  frame=self.c.frame;filtered=getattr(self.c,'third_filtered',None)
  if frame and frame.get('third') and filtered:self.c.seek(round(frame['third'][1]['start']+point.x()*filtered['scale_x']))
 def select_channel(self):
  self.c.secondary_channel=self.channel.currentData() or True
  self.c.latest_descriptor=None;self.s.schedule_sonar()
 def toggle_grid(self,checked):
  for view in [self.s.sonar,self.c.down]:view.grid_visible=checked;view.viewport().update()
 def goto(self):
  if self.s.recording is None:return
  times=self.s.play_times;target=float(times[0])+self.seconds.value();index=int(np.searchsorted(times,target));self.c.seek(min(len(times)-1,index))
 def step(self,direction):
  if self.s.recording is not None:self.c.seek(max(0,min(self.s.slider.maximum(),self.s.slider.value()+direction*10)))
 def depth_change(self):
  if self.s.recording is None:return
  threshold,ok=QInputDialog.getDouble(self.s,'Mudança de profundidade','Variação mínima entre pings (m)',1.,.01,1000.,2)
  if not ok:return
  depth=self.s.recording.data.inst_dep_m.to_numpy(float);hits=np.flatnonzero(np.isfinite(depth[1:])&np.isfinite(depth[:-1])&(np.abs(np.diff(depth))>=threshold))+1
  hits=hits[hits>self.s.slider.value()]
  if len(hits):self.c.seek(int(hits[0]))
  else:self.s.statusBar().showMessage('Nenhuma mudança acima do limiar no restante da gravação.',5000)
 def copy_position(self):
  if not self.s.recording:return
  point=getattr(self.c,'last_echo_position',None)
  if point and self.c.frame and point[2]==self.c.token and not self.s.sonar_is_down:
   from viewer_metrology import point_from_pixel
   try:
    position=point_from_pixel(self.s.recording,self.c.frame['info'],point[0],point[1],self.c.altitude_override)
   except ValueError as error:self.s.statusBar().showMessage(str(error),5000);return
   from pyproj import Transformer
   converter=Transformer.from_crs(self.s.recording.crs,4326,always_xy=True);longitude,latitude=converter.transform(position['east'],position['north'])
   QApplication.clipboard().setText(f'{latitude:.8f}, {longitude:.8f}');self.s.statusBar().showMessage('Coordenada estimada do eco copiada; precisão de campo não aferida.',5000)
  else:self.s.statusBar().showMessage('Posicione o mouse sobre um eco válido do SideScan antes de copiar.',5000)
 def update(self):
  rec=self.s.recording
  if rec is None:return
  if rec is not self.recording:
   self.recording=rec;self.channel.blockSignals(True);self.channel.clear();self.channel.addItem('DownScan automático',True)
   for key in rec.channels:
    if not key.startswith('ss_'):self.channel.addItem(key,key)
   self.channel.blockSignals(False)
   # Populating controls must not invalidate an in-flight inference/window.
   self.c.secondary_channel=True
  row=rec.data.iloc[self.s.slider.value()];depth=float(row.inst_dep_m);speed=float(row.get('speed_ms',0));feet=self.units.currentIndex()==1
  self.status.setText(f'Ping {self.s.slider.value()+1:,} | {depth*3.280839895 if feet else depth:.2f} {"ft" if feet else "m"} | {speed*1.943844 if feet else speed*3.6:.1f} {"kn" if feet else "km/h"}')
 def state(self):return dict(layout=self.layout.currentIndex(),secondary=self.channel.currentData(),reverse=self.reverse.isChecked(),grid=self.grid.isChecked(),units=self.units.currentIndex())
 def edit_palette(self):
  colors=[]
  from PySide6.QtGui import QColor
  for name,default in [('Fundo','#000000'),('Eco baixo','#2d1305'),('Eco médio','#793d12'),('Eco alto','#d78735'),('Eco máximo','#ffebb5')]:
   color=QColorDialog.getColor(QColor(default),self.s,name)
   if not color.isValid():return
   colors.append(color.name())
  name='Personalizada:'+','.join(colors)
  index=self.s.palette.findText(name)
  if index<0:self.s.palette.addItem(name);index=self.s.palette.count()-1
  self.s.palette.setCurrentIndex(index)
 def print_view(self):
  from PySide6.QtPrintSupport import QPrinter,QPrintDialog
  from PySide6.QtGui import QPainter
  printer=QPrinter(QPrinter.PrinterMode.HighResolution);dialog=QPrintDialog(printer,self.s)
  if dialog.exec()!=dialog.DialogCode.Accepted:return
  painter=QPainter(printer)
  if not painter.isActive():self.s.notify_error('Não foi possível iniciar a impressão.');return
  try:
   image=self.s.sonar.viewport().grab();area=printer.pageLayout().paintRectPixels(printer.resolution());scale=min(area.width()/image.width(),area.height()/image.height());painter.translate(area.x(),area.y());painter.scale(scale,scale);painter.drawPixmap(0,0,image)
  finally:painter.end()
 def movie(self):
  if self.s.recording is None:return
  first,ok=QInputDialog.getInt(self.s,'Exportar vídeo','Primeiro ping',self.s.slider.value()+1,1,len(self.s.recording.data))
  if not ok:return
  last,ok=QInputDialog.getInt(self.s,'Exportar vídeo','Último ping',min(len(self.s.recording.data),first+1000),first,len(self.s.recording.data))
  if not ok:return
  path,_=QFileDialog.getSaveFileName(self.s,'Salvar vídeo do canal selecionado','sonar.avi','AVI (*.avi)')
  if not path:return
  from sonar_movie import export_movie
  rec=self.s.recording;channel=self.s.channel.currentData();water=self.s.water.isChecked() and not self.s.sonar_is_down;settings=self.c.options();count=self.c.context.value();reverse_cascade=bool(self.s.cascade_direction.currentData());reverse_time=self.reverse.isChecked();speed=abs(float(self.s.speed.currentData()))
  self.s.run_job(lambda note,progress,cancel:export_movie(rec.dat,rec.project,path,first-1,last-1,channel,water,count,settings,speed,reverse_time,reverse_cascade,progress,cancel),lambda value:self.s.log.appendPlainText('Vídeo salvo: '+value['path']))
 def restore(self,state):
  self.layout.setCurrentIndex(state.get('layout',0));self.reverse.setChecked(state.get('reverse',False));self.grid.setChecked(state.get('grid',False));self.units.setCurrentIndex(state.get('units',0))
  index=self.channel.findData(state.get('secondary',True))
  if index>=0:self.channel.setCurrentIndex(index)
