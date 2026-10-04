"""Viewer-first application controller: streaming, filters, metrology and live AI."""
from pathlib import Path
from dataclasses import replace
from collections import deque
import json,time,math,hashlib
import numpy as np
from PySide6.QtCore import QObject,Qt,QTimer,QPoint
from PySide6.QtGui import QAction,QPen,QColor,QFont
from PySide6.QtWidgets import QWidget,QSplitter,QHBoxLayout,QVBoxLayout,QCheckBox,QPushButton,QComboBox,QLabel,QSlider,QSpinBox,QDialog,QFormLayout,QDoubleSpinBox,QTableWidget,QTableWidgetItem,QFileDialog
from image_processing import ImageSettings
from viewer_stream import WindowStream
from viewer_gpu import ColorShader
from viewer_filters import structural,colors
from viewer_timeline import IntensityTimeline,intensity_profile
from viewer_findings import Findings
from viewer_inference import ResultCache,source_identity,inference_key,interpolated_boxes
from viewer_metrology import point_from_pixel,measure_points,shadow_scenarios

class ViewerController(QObject):
 def __init__(self,studio,layout):
  super().__init__(studio);s=studio;self.s=studio;self.stream=None;self.frame=None;self.filtered=None;self.pending=None;self.filter_worker=None;self.profile_worker=None;self.token=0;self.filter_token=0;self.frozen=False;self.measurement=None;self.profile_epoch=0;self.closed=False;self.observations={};self.metrics=deque(maxlen=500);self.last_painted_token=None;self.latest_descriptor=None;self.nav=deque(maxlen=20);self.cache_hits=0;self.discarded_ai=0;self.identity=None;self.ai_busy_key=None;self.ai_pending=False
  self.shader=ColorShader(legacy=True);self.down_shader=ColorShader(legacy=True);self.shader_time=0.;self.filter_key=None
  toolbar=QHBoxLayout();self.background=QCheckBox('Leitura em fundo');self.background.setChecked(True);self.background.toggled.connect(self.toggle_background);toolbar.addWidget(self.background)
  self.split=QCheckBox('SideScan + DownScan');self.split.toggled.connect(lambda:s.schedule_sonar());toolbar.addWidget(self.split)
  self.freeze=QPushButton('Congelar / filtro final');self.freeze.setCheckable(True);self.freeze.toggled.connect(self.freeze_changed);toolbar.addWidget(self.freeze)
  self.caption=QLabel('');self.caption.setWordWrap(True);toolbar.addWidget(self.caption,1);layout.insertLayout(1,toolbar)
  tools=QHBoxLayout();self.tool=QComboBox();self.tool.addItems(['Navegar','Régua','Sombra']);self.tool.currentTextChanged.connect(self.tool_changed);tools.addWidget(self.tool)
  params=QPushButton('Hipóteses métricas');params.clicked.connect(self.parameters);tools.addWidget(params)
  snap=QPushButton('Capturar + legenda');snap.clicked.connect(self.snapshot);tools.addWidget(snap)
  findings=QPushButton('Achados (T/R/S)');findings.clicked.connect(self.show_findings);tools.addWidget(findings);self.measure_label=QLabel('Medições aproximadas; hipóteses registradas.');self.measure_label.setWordWrap(True);tools.addWidget(self.measure_label,1);layout.insertLayout(2,tools)
  open_card=QAction('Abrir ficha PDF privada…',s);open_card.triggered.connect(self.open_card);s.menuBar().addAction(open_card)
  self.comparison=QSlider(Qt.Orientation.Horizontal);self.comparison.setRange(0,100);self.comparison.setValue(50);self.comparison.setToolTip('Divisor original / tratado, sem duplicar a imagem');self.comparison.valueChanged.connect(self.recolor);layout.insertWidget(3,self.comparison)
  from viewer_signature import SignatureChart
  self.signature=SignatureChart();self.signature.setVisible(False);self.loupe=QCheckBox('Lupa de intensidade nativa');tools.insertWidget(0,self.loupe);self.loupe.toggled.connect(self.signature.setVisible);layout.insertWidget(4,self.signature)
  self.signature_worker=None;self.hover_pending=None;self.hover_timer=QTimer(self);self.hover_timer.setSingleShot(True);self.hover_timer.setInterval(60);self.hover_timer.timeout.connect(self.start_signature);s.sonar.hover.connect(self.hover_signature)
  self.measure_timer=QTimer(self);self.measure_timer.setSingleShot(True);self.measure_timer.setInterval(60);self.measure_timer.timeout.connect(lambda:self.measure(*self.measure_pending));s.sonar.gesture_preview.connect(self.preview_measure)
  self.timeline=IntensityTimeline();self.timeline.picked.connect(self.seek);layout.insertWidget(layout.indexOf(s.slider),self.timeline)
  from app import SonarView
  self.down=SonarView();self.down.setMinimumHeight(110);self.down.setMaximumHeight(240);self.down.setVisible(False);self.down.seek.connect(self.seek_down);layout.insertWidget(layout.indexOf(s.sonar)+1,self.down)
  from viewer_overview import SonarOverview
  self.overview=SonarOverview();self.overview.picked.connect(self.seek);position=layout.indexOf(s.sonar);layout.removeWidget(s.sonar);layout.removeWidget(self.down);pane=QWidget();row=QHBoxLayout(pane);row.setContentsMargins(0,0,0,0);row.addWidget(s.sonar,1);row.addWidget(self.overview);self.sonar_split=QSplitter(Qt.Orientation.Vertical);self.sonar_split.addWidget(pane);self.sonar_split.addWidget(self.down);self.sonar_split.setStretchFactor(0,1);self.sonar_split.setStretchFactor(1,0);self.sonar_split.setSizes([600,110]);layout.insertWidget(position,self.sonar_split,1)
  vertical=QHBoxLayout();vertical.addWidget(QLabel('Escala vertical'));self.vertical=QSlider(Qt.Orientation.Horizontal);self.vertical.setRange(10,800);self.vertical.setValue(100);self.vertical.setToolTip('10% comprime; 100% enquadra; até 800% amplia só a vertical. Shift + roda também ajusta.');vertical.addWidget(self.vertical,1);self.vertical_label=QLabel('100%');vertical.addWidget(self.vertical_label)
  vertical.addWidget(QLabel('Pings na janela'));self.context=QSpinBox();self.context.setRange(64,3000);self.context.setValue(s.professional.pings.value());vertical.addWidget(self.context)
  self.virtual=QCheckBox('Roda: navegar gravação');self.virtual.setChecked(True);self.virtual.setToolTip('Roda percorre pings; Ctrl + roda mantém zoom comum.');vertical.addWidget(self.virtual);layout.insertLayout(3,vertical)
  self.vertical.valueChanged.connect(self.vertical_scale);s.sonar.vertical_changed.connect(lambda value:self.vertical.setValue(round(value*100)));s.sonar.scroll_pings.connect(self.scroll);s.sonar.viewport_changed.connect(self.update_overview);self.virtual.toggled.connect(lambda checked:setattr(s.sonar,'virtual_navigation',checked));s.sonar.virtual_navigation=True
  self.context.valueChanged.connect(s.professional.pings.setValue);s.professional.pings.valueChanged.connect(self.context.setValue);s.professional.pings.valueChanged.connect(lambda:s.schedule_sonar())
  s.sonar.gesture.connect(self.measure);s.sonar.seek.connect(self.seek_side)
  self.live_timer=QTimer(self);self.live_timer.setInterval(400);self.live_timer.timeout.connect(self.live_tick);self.live_timer.start()
  self.color_timer=QTimer(self);self.color_timer.setSingleShot(True);self.color_timer.setInterval(16);self.color_timer.timeout.connect(self.paint_colors)
  self.ai_max_rate=10.;self.s.ai_continuous.setToolTip('IA assíncrona quando a navegação fica abaixo da taxa configurada em Hipóteses métricas.');self.altitude_override=None;self.depth_uncertainty=.2;self.picking_samples=2.;self.slope_deg=2.
  for key,tag in [('T','Alvo'),('R','Rocha'),('S','Naufrágio')]:
   action=QAction('Marcar '+tag,s);action.setShortcut(key);action.triggered.connect(lambda checked=False,tag=tag:self.quick_tag(tag));s.addAction(action)
  export_metrics=QAction('Viewer — exportar tempos de execução…',s);export_metrics.triggered.connect(self.export_metrics);s.menuBar().addAction(export_metrics)
  self.products=QAction('Mapa / produtos',s);self.products.setCheckable(True);self.products.toggled.connect(self.show_products);s.menuBar().addAction(self.products)
  self.enabled=True
  s.professional.method.currentIndexChanged.connect(self.live_settings)
  for control in [s.professional.strength,s.professional.destripe,s.professional.clahe,s.professional.sharpen,s.professional.black,s.professional.white]:control.valueChanged.connect(self.live_settings)
  s.professional.compare.toggled.connect(self.live_settings)
  import os
  if os.environ.get('SONAR_VIEWER_COMPAT')=='1':self.background.setChecked(False)
  else:self.show_products(False)
 def preview_measure(self,first,last):
  self.measure_pending=(first,last);self.measure_timer.start()
 def hover_signature(self,point):
  if self.frame is None or self.filtered is None or self.frame['token']!=self.token:return
  f=self.filtered;left=self.s.sonar_crop[0] if self.s.sonar_crop else 0
  self.last_echo_position=(point.x()*f['scale_x']+left,point.y()*f['scale_y'],self.token)
  if not self.loupe.isChecked():return
  self.hover_pending=(point.x()*f['scale_x']+left,point.y()*f['scale_y'],self.token,dict(self.frame['info']),self.s.sonar_is_down);self.hover_timer.start()
 def start_signature(self):
  if self.closed or not self.hover_pending:return
  if self.signature_worker and self.signature_worker.isRunning():return
  x,y,token,info,isdown=self.hover_pending;self.hover_pending=None;rec=self.s.recording
  from app import Worker
  from viewer_signature import native_profile
  self.signature_worker=Worker(lambda note,progress,cancel:native_profile(rec,info,x,y,isdown));self.signature_worker.ready.connect(lambda value:self.signature.set_profile(value) if token==self.token and not self.closed else None);self.signature_worker.failed.connect(lambda message:self.s.statusBar().showMessage(message));self.signature_worker.finished.connect(lambda:self.hover_timer.start() if self.hover_pending and not self.closed else None);self.signature_worker.start()
 def show_products(self,checked):
  self.s.map_tools.setVisible(checked);self.s.vertical_splitter.setSizes([340,600] if checked else [0,1000])
 def toggle_background(self,checked):
  self.enabled=checked
  self.s.sonar.virtual_navigation=checked and self.virtual.isChecked();self.overview.setVisible(checked)
  if hasattr(self,'products'):self.show_products(self.products.isChecked() if checked else True)
  if not checked and self.stream:self.stream.close();self.stream=None
  elif checked and self.s.recording and self.stream is None:self.stream=WindowStream(self.s.recording);self.stream.ready.connect(self.loaded);self.stream.failed.connect(self.s.log.appendPlainText)
  if self.s.recording:self.s.schedule_sonar()
 def reset(self):
  self.token+=1;self.filter_token+=1;self.profile_epoch+=1;self.frame=None;self.filtered=None;self.filter_key=None;self.measurement=None;self.observations.clear();self.timeline.times=None;self.timeline.samples=None;self.timeline.markers=[];self.timeline.update();self.nav.clear()
  self.hover_timer.stop();self.measure_timer.stop();self.hover_pending=None;self.signature.set_profile(None)
  if self.signature_worker and self.signature_worker.isRunning():self.signature_worker.stop.set();self.signature_worker.wait(15000)
  if self.stream:self.stream.close();self.stream=None
  if self.profile_worker and self.profile_worker.isRunning():self.profile_worker.stop.set();self.profile_worker.wait(15000)
  self.overview.set_data(0)
 def recording_loaded(self,rec):
  self.closed=False;self.identity=source_identity(rec);self.cache=ResultCache(rec.project/'viewer-inference.sqlite');self.findings=Findings(rec.project/'viewer-findings.sqlite')
  if self.enabled:
   self.stream=WindowStream(rec);self.stream.ready.connect(self.loaded);self.stream.failed.connect(self.s.log.appendPlainText)
   from app import Worker
   epoch=self.profile_epoch;dat,project=rec.dat,rec.project
   self.profile_worker=Worker(lambda note,progress,cancel:intensity_profile(dat,project,cancel,collect_preview=True));self.profile_worker.ready.connect(lambda value:self.profile_ready(value,epoch));self.profile_worker.failed.connect(self.s.log.appendPlainText);self.profile_worker.start()
  self.timeline.set_data(rec.data.time_s.to_numpy(),None);self.refresh_markers()
  self.overview.set_data(len(rec.data))
 def profile_ready(self,samples,epoch):
  if epoch==self.profile_epoch and self.s.recording and samples is not None:
   self.timeline.set_data(self.s.recording.data.time_s.to_numpy(),samples['samples']);self.overview.set_data(len(self.s.recording.data),samples['preview']);self.update_overview()
 def vertical_scale(self,value):
  self.vertical_label.setText(str(value)+'%');self.s.sonar.set_vertical_zoom(value/100);self.update_overview()
 def scroll(self,direction):
  if self.s.recording is None:return
  self.s.pause_playback();step=max(1,round(self.context.value()/12));index=self.s.slider.value()+direction*step*(-1 if self.s.cascade_direction.currentData() else 1);self.s.set_index(int(np.clip(index,0,len(self.s.recording.data)-1)))
 def update_overview(self):
  if self.frame is None or self.filtered is None:return
  view=self.s.sonar;size=view.viewport().size();points=[view.image_item.mapFromScene(view.mapToScene(QPoint(0,0))),view.image_item.mapFromScene(view.mapToScene(QPoint(size.width(),size.height())))] if view.image_item else []
  if not points:return
  f=self.filtered;axis=[p.x()*f['scale_x'] if self.s.sonar_is_down else p.y()*f['scale_y'] for p in points];info=self.frame['info'];height=info['end']-info['start'];self.overview.set_window(info['start']+max(0,min(axis)),info['start']+min(height,max(axis)))
 def navigated(self,index):
  self.nav.append((time.monotonic(),index));self.timeline.index=index;self.timeline.update()
  if self.frame is None or self.frame['index']!=index:self.token+=1
  if self.freeze.isChecked():self.freeze.blockSignals(True);self.freeze.setChecked(False);self.freeze.blockSignals(False);self.frozen=False
  self.measurement=None
 def seek(self,index):self.s.pause_playback();self.s.set_index(index)
 def seek_side(self,point):
  if self.frame and self.filtered and self.tool.currentText()=='Navegar':self.seek(round(self.frame['info']['start']+(point.x()*self.filtered['scale_x'] if self.s.sonar_is_down else point.y()*self.filtered['scale_y'])))
 def seek_down(self,point):
  if self.frame and self.frame.get('down') and getattr(self,'down_filtered',None):self.seek(round(self.frame['down'][1]['start']+point.x()*self.down_filtered['scale_x']))
 def request(self):
  if self.stream is None or self.closed:return
  s=self.s;index=s.slider.value();channel=s.channel.currentData();descriptor=(index,channel,s.water.isChecked(),s.professional.pings.value(),s.professional.range_mode.currentIndex(),self.split.isChecked(),s.professional.aspect.currentIndex())
  if self.frame is not None and self.latest_descriptor==descriptor and self.frame['token']==self.token:self.refilter();return
  self.latest_descriptor=descriptor;self.token+=1;self.request_started=time.perf_counter();side=channel=='both' or str(channel).startswith('ss_');s.sonar_is_down=not side;s.water.setEnabled(side);s.ai_mode.setEnabled(side);s.analyze_button.setEnabled(side);s.cascade_direction.setEnabled(side);self.tool.setEnabled(side);s.sonar.tool_mode=self.tool.currentText() if side else 'Navegar'
  self.down.setVisible(self.split.isChecked() and side);self.caption.setText('Carregando janela em fundo…')
  self.stream.request(index,channel,s.water.isChecked() and side,s.professional.pings.value(),'fixed' if s.professional.range_mode.currentIndex()==2 else 'local',self.split.isChecked() and side,self.token)
 def loaded(self,value):
  if value['token']!=self.token or self.closed:return
  s=self.s;info=value['info'];self.frame=value;s.raw_array=value['array'] if not s.sonar_is_down else value['array'].T;s.frame_info=info;s.frame_index=value['index']
  from viewer_geometry import display_crop
  s.sonar_crop=display_crop(info,s.professional.range_mode.currentIndex());ratio=info['along_track_m']/info['sampling_m'] if s.professional.aspect.currentIndex()==0 else 1.;s.sonar_y_scale=ratio if not s.sonar_is_down else 1.;s.sonar_x_scale=1. if not s.sonar_is_down else ratio
  s.sonar.fill_view=s.professional.aspect.currentIndex()==2;self.filter_key=None;self.refilter()
 def live_settings(self,*args):
  if not self.enabled:return
  settings=self.s.professional.image_settings()
  if settings.white<=settings.black:self.caption.setText('Ajuste níveis: branco deve superar preto.');return
  old=self.s.processing;self.s.processing=settings
  fields=('method','strength','destripe','clahe','sharpen','black','white')
  if any(getattr(old,k)!=getattr(settings,k) for k in fields):self.refilter()
  else:self.recolor()
 def options(self):return replace(self.s.processing,gamma=self.s.gamma.value()/100,palette=self.s.palette.currentText())
 def refilter(self):
  if self.frame is None:return
  settings=self.options();key=(self.frame['token'],settings.method,settings.strength,settings.destripe,settings.clahe,settings.sharpen,settings.black,settings.white,settings.gamma,self.frozen,self.s.screen_limit(),self.s.sonar_crop)
  if key==self.filter_key and self.filtered is not None:self.recolor();return
  self.filter_token+=1;self.filter_key=key;array=self.s.raw_array;crop=self.s.sonar_crop
  if crop:array=array[:,crop[0]:crop[1]]
  limit=None if self.frozen else self.s.screen_limit();limit=(min(1600,limit[0]),min(700,limit[1])) if limit else None
  token=self.filter_token;frame_token=self.frame['token'];down=self.frame.get('down');self.pending=(array,settings,limit,self.s.sonar_is_down,token,frame_token,down)
  self.start_filter()
 def start_filter(self):
  if self.filter_worker and self.filter_worker.isRunning() or self.pending is None or self.closed:return
  from app import Worker
  array,settings,limit,isdown,token,frame_token,down=self.pending;self.pending=None
  def work(note,progress,cancel):
   result=structural(array,settings,limit,isdown)
   if cancel():raise InterruptedError('Filtro cancelado.')
   down_result=structural(down[0].T,settings,limit,True) if down else None
   return result,down_result,token,frame_token
  self.filter_worker=Worker(work);self.filter_worker.ready.connect(self.filtered_ready);self.filter_worker.failed.connect(self.s.log.appendPlainText);self.filter_worker.finished.connect(self.filter_finished);self.filter_worker.start()
 def filter_finished(self):
  if self.s.close_when_finished:QTimer.singleShot(0,self.s.close)
  elif self.pending is not None:QTimer.singleShot(0,self.start_filter)
 def filtered_ready(self,result):
  value,down,token,frame_token=result
  if self.closed or self.frame is None or token!=self.filter_token or frame_token!=self.token:return
  self.filtered=value;self.down_filtered=down;self.paint_colors()
 def recolor(self,*args):
  if self.filtered is not None:self.color_timer.start()
 def paint_colors(self):
  if self.frame is None or self.filtered is None or self.closed:return
  start=time.perf_counter();s=self.s;settings=replace(self.options(),gamma=1.,black=0.,white=255.);compare=s.professional.compare.isChecked();split=self.comparison.value()/100;f=self.filtered;self.comparison.setVisible(compare)
  image=self.shader.render(f['treated'],f['original'],settings,compare,split);ys=s.sonar_y_scale*f['scale_y'];xs=s.sonar_x_scale*f['scale_x'];s.sonar.display_image(image,ys,xs,bool(s.cascade_direction.currentData()) and not s.sonar_is_down)
  if s.sonar.auto_fit:s.sonar.fit()
  if self.down_filtered is not None and self.down.isVisible():
   d=self.down_filtered;im=self.down_shader.render(d['treated'],d['original'],settings,compare,split);self.down.fill_view=True;self.down.display_image(im,d['scale_y'],d['scale_x']);self.down.fit();self.down.overlays([],xs=d['scale_x'],cursor=(s.slider.value()-self.frame['down'][1]['start'])/d['scale_x'],downscan=True)
  self.draw_overlays();self.update_overview();self.shader_time=(time.perf_counter()-start)*1000;elapsed=(time.perf_counter()-getattr(self,'request_started',start))*1000
  new_frame=self.last_painted_token!=self.frame['token'];self.last_painted_token=self.frame['token']
  sample={'kind':'navigation' if new_frame else 'visual_adjustment','index':s.frame_index,'read_ms':self.frame['read_ms'],'filters_ms':dict(f['filter_ms']),'structural_ms':f['total_ms'],'color_and_present_ms':self.shader_time,'end_to_end_ms':elapsed if new_frame else None,'preview':f['preview'],'backend':self.shader.last_backend,'buffer_bytes':self.frame['buffer_bytes']};self.metrics.append(sample)
  s.last_render_ms=self.shader_time+f['total_ms'];state='PRÉVIA REDUZIDA' if f['preview'] else 'RESOLUÇÃO COMPLETA';s.sonar_header.setText('SONAR VIEWER · '+('ORIGINAL / TRATADO' if compare else state));self.caption.setText(f"{state} · {self.shader.last_backend} · janela ping {s.frame_index+1} · leitura {self.frame['read_ms']:.1f} ms · filtros {f['total_ms']:.1f} ms · cor/UI {self.shader_time:.1f} ms")
 def draw_overlays(self):
  if self.frame is None or self.filtered is None:return
  from viewer_geometry import display_box
  s=self.s;info=self.frame['info'];f=self.filtered;left=s.sonar_crop[0] if s.sonar_crop else 0;reverse=bool(s.cascade_direction.currentData()) and not s.sonar_is_down;items=[]
  if not s.sonar_is_down:
   current_hash=None
   if s.ai_mode.currentIndex()>0:
    from model_registry import model_path,checked_hash
    model=model_path('ghostvision' if s.ai_mode.currentIndex()==1 else 'sonarvision');stat=model.stat();current_hash=checked_hash(str(model),stat.st_size,stat.st_mtime_ns)
   observations=sorted(self.observations.values(),key=lambda x:x['index']);before=[o for o in observations if o['index']<s.frame_index];after=[o for o in observations if o['index']>s.frame_index];interpolated=interpolated_boxes(before[-1],after[0],s.frame_index) if before and after else []
   identities={i['id'] for i in interpolated}
   for item in [*s.object_review.filtered(),*interpolated]:
    if item.get('status','pendente')=='pendente' and (current_hash is None or item.get('model_sha256',current_hash)!=current_hash):continue
    if not item.get('visual_interpolation') and item['id'] in identities:continue
    if item.get('status')=='rejeitado' or item.get('frame_channel')!=info['channel'] or item.get('frame_part_width')!=info['part_width'] or item.get('frame_water')!=info['remove_water']:continue
    box=np.array(item['box'],float);box[[1,3]]+=item['frame_start']-info['start'];box[[0,2]]-=left;box[[0,2]]/=f['scale_x'];box[[1,3]]/=f['scale_y']
    if box[3]<0 or box[1]>f['shape'][0] or box[2]<0 or box[0]>f['shape'][1]:continue
    obj=dict(item);obj['box']=display_box(box,0,f['shape'][0],reverse);items.append(obj)
  cursor=(s.frame_index-info['start'])/(f['scale_x'] if s.sonar_is_down else f['scale_y']);cursor=f['shape'][0]-cursor if reverse else cursor
  s.sonar.overlays(items,ys=s.sonar_y_scale*f['scale_y'],xs=s.sonar_x_scale*f['scale_x'],cursor=cursor,downscan=s.sonar_is_down)
  if self.s.professional.compare.isChecked():
   x=f['shape'][1]*self.comparison.value()/100*s.sonar_x_scale*f['scale_x'];pen=QPen(QColor('#ffffff'),1);pen.setCosmetic(True);line=s.sonar.scene().addLine(x,0,x,s.sonar.image_item.sceneBoundingRect().height(),pen);s.sonar.overlay_items.append(line)
 def freeze_changed(self,checked):self.s.pause_playback();self.frozen=checked;self.refilter()
 def tool_changed(self,name):self.s.pause_playback();self.measure_timer.stop();self.s.sonar.tool_mode=name;self.s.sonar.measure_start=None;self.s.sonar.viewport().update()
 def parameters(self):
  d=QDialog(self.s);d.setWindowTitle('Hipóteses da medição — não são calibração');form=QFormLayout(d);manual=QCheckBox('Informar altura transdutor-fundo');manual.setChecked(self.altitude_override is not None);form.addRow(manual)
  values=[]
  for label,value,lo,hi in [('Altura transdutor-fundo (m)',self.altitude_override or 10.,.01,4000),('Variação de altura para cenários (m)',self.depth_uncertainty,0,20),('Variação de marcação (amostras)',self.picking_samples,0,100),('Inclinação de cenário (graus)',self.slope_deg,0,15),('Taxa máx. para IA automática (pings/s)',self.ai_max_rate,.1,1000)]:
   spin=QDoubleSpinBox();spin.setRange(lo,hi);spin.setDecimals(3);spin.setValue(value);form.addRow(label,spin);values.append(spin)
  note=QLabel('Sem altura manual, usa inst_dep_m: confira referência do transdutor.\nO ângulo de incidência é derivado da geometria, não da abertura do feixe.\nFaixa de cenários não é intervalo de confiança.');note.setWordWrap(True);form.addRow(note);ok=QPushButton('Aplicar');ok.clicked.connect(d.accept);form.addRow(ok)
  if d.exec():self.altitude_override=values[0].value() if manual.isChecked() else None;self.depth_uncertainty=values[1].value();self.picking_samples=values[2].value();self.slope_deg=values[3].value();self.ai_max_rate=values[4].value()
 def measure(self,first,last):
  if self.frame is None or self.filtered is None or self.s.sonar_is_down:return
  if self.frame['index']!=self.s.slider.value():self.measure_label.setText('Aguarde a janela do ping selecionado.');return
  f=self.filtered;left=self.s.sonar_crop[0] if self.s.sonar_crop else 0
  try:
   point=lambda p:point_from_pixel(self.s.recording,self.frame['info'],p.x()*f['scale_x']+left,p.y()*f['scale_y'],self.altitude_override)
   a,b=point(first),point(last)
   if self.tool.currentText()=='Régua':value=measure_points(self.s.recording,a,b);text=f"Distância horizontal estimada: {value['distance_m']:.2f} m · percurso longitudinal {value['navigation_path_m']:.2f} m"
   else:value=shadow_scenarios(a,b,self.frame['info']['sampling_m'],self.depth_uncertainty,self.picking_samples,self.slope_deg);text=f"Altura: {value['height_m']:.2f} m · cenários {value['scenario_min_m']:.2f}–{value['scenario_max_m']:.2f} m (fundo plano nominal)"
   self.measurement=value;self.measure_label.setText(text+' · T/R/S registra hipóteses')
  except Exception as error:self.measurement=None;self.measure_label.setText(str(error))
 def quick_tag(self,tag):
  if self.s.recording is None:return
  rec=self.s.recording;point=self.measurement.get('a') if self.measurement else None
  if point is None and self.frame and not self.s.sonar_is_down and getattr(self,'last_echo_position',None) and self.last_echo_position[2]==self.token:
   try:point=point_from_pixel(rec,self.frame['info'],*self.last_echo_position[:2],self.altitude_override)
   except ValueError:point=None
  ping=int(point['ping']) if point else self.s.slider.value();row=rec.data.iloc[ping]
  metadata={'manual':True,'not_ai_detection':True,'source_identity':self.identity,'time_s':float(row.time_s),'date':str(row.get('date','')),'time':str(row.get('time','')),'latitude':float(row.lat),'longitude':float(row.lon),'depth_instrument_m':float(row.inst_dep_m),'east':float(row.e),'north':float(row.n),'coordinate_reference':'vessel navigation, not object position','display_settings':self.options().json(),'measurement':self.measurement}
  if point:
   from pyproj import Transformer
   longitude,latitude=Transformer.from_crs(rec.crs,4326,always_xy=True).transform(point['east'],point['north']);metadata.update(east=point['east'],north=point['north'],longitude=longitude,latitude=latitude,coordinate_reference=point['accuracy'])
  if not np.isfinite([metadata[k] for k in ['latitude','longitude','depth_instrument_m','east','north']]).all():self.s.log.appendPlainText('Achado não salvo: metadados de posição/profundidade inválidos.');return
  from target_evidence import target_image,png_echo
  # Native capture is small (one window) and performed only on an explicit tag.
  raw,info=target_image(rec,ping,self.frame['info']['channel'] if self.frame else 'both',focus=point);folder=rec.project/'findings-images';folder.mkdir(exist_ok=True);photo=folder/(hashlib.sha256((str(rec.dat)+str(time.time_ns())).encode()).hexdigest()[:24]+'.png');temporary=photo.with_suffix('.incomplete.png');temporary.write_bytes(png_echo(raw,self.options().palette));temporary.replace(photo);metadata.update(snapshot_path=str(photo),snapshot_frame=info,snapshot_sha256=hashlib.sha256(photo.read_bytes()).hexdigest(),target_point=point,frame_channel=self.frame['info']['channel'] if self.frame else 'both')
  identity=self.findings.add(rec.dat,ping,tag,metadata);self.refresh_markers();self.s.statusBar().showMessage(f'{tag} salvo localmente com eco nativo no ping {ping+1}: {identity[:8]}')
 def refresh_markers(self):
  if self.s.recording and hasattr(self,'findings'):self.timeline.markers=[r['ping'] for r in self.findings.list(self.s.recording.dat)];self.timeline.update()
 def show_findings(self):
  if self.s.recording is None:return
  d=QDialog(self.s);d.setWindowTitle('Achados manuais — banco local');d.resize(850,500);layout=QVBoxLayout(d);rows=self.findings.list(self.s.recording.dat);table=QTableWidget(len(rows),4);table.setHorizontalHeaderLabels(['Ping','Classe manual','Hora','Medição / hipóteses']);layout.addWidget(table)
  for i,row in enumerate(rows):
   values=[row['ping']+1,row['tag'],row['metadata']['time'],json.dumps(row['metadata'].get('measurement'),ensure_ascii=False)[:160]]
   for j,value in enumerate(values):table.setItem(i,j,QTableWidgetItem(str(value)))
  table.resizeColumnsToContents();table.cellDoubleClicked.connect(lambda i,j:(self.seek(rows[i]['ping']),d.close()));button=QPushButton('Exportar achados com metadados JSON');button.clicked.connect(lambda:self.export_findings(rows));layout.addWidget(button);card=QPushButton('Ficha PDF privada do achado selecionado');card.clicked.connect(lambda:self.target_card(rows[table.currentRow()]) if 0<=table.currentRow()<len(rows) else None);layout.addWidget(card);d.exec()
 def target_card(self,item):
  if not item or self.s.recording is None:return
  self.s.pause_playback();rec=self.s.recording
  from private_pdf import key_path
  try:key_path()
  except Exception as error:self.s.log.appendPlainText(str(error));return
  path,_=QFileDialog.getSaveFileName(self.s,'Ficha pessoal criptografada',str(rec.project/f"alvo-ping-{int(item['ping'])+1}.pdf"),'PDF (*.pdf)')
  if not path:return
  from target_evidence import export_pdf
  saved=json.loads(json.dumps(item));palette=self.options().palette
  self.s.run_job(lambda note,progress,cancel:export_pdf(rec,saved,path,palette),self.card_ready)
 def card_ready(self,result):
  self.last_card=result['file'];self.s.log.appendPlainText(f"Ficha privada AES-256 criada: {result['file']} ({result['pages']} página(s)).")
  from private_pdf import preview_pdf
  try:preview_pdf(self.s,self.last_card)
  except Exception as error:self.s.log.appendPlainText('Ficha salva; prévia indisponível: '+str(error))
 def open_card(self):
  path,_=QFileDialog.getOpenFileName(self.s,'Ficha pessoal criptografada',str(self.s.recording.project) if self.s.recording else '', 'PDF (*.pdf)')
  if not path:return
  from private_pdf import preview_pdf
  try:preview_pdf(self.s,path)
  except Exception as error:self.s.log.appendPlainText(str(error))
 def export_findings(self,rows):
  path,_=QFileDialog.getSaveFileName(self.s,'Achados locais com hipóteses',str(self.s.recording.project/'achados.json'),'JSON (*.json)')
  if path:Path(path).write_text(json.dumps(rows,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
 def snapshot(self):
  if self.frame is None:return
  if self.frame['index']!=self.s.slider.value():self.s.statusBar().showMessage('Aguarde a janela atual antes de capturar.');return
  self.s.pause_playback();path,_=QFileDialog.getSaveFileName(self.s,'Captura de resolução completa com legenda',str(self.s.recording.project/'sonar-captura.png'),'PNG (*.png)')
  if not path:return
  array=self.s.raw_array.copy();settings=self.options();info=dict(self.frame['info']);index=self.s.frame_index;rec=self.s.recording;row=rec.data.iloc[index];reverse=bool(self.s.cascade_direction.currentData()) and not self.s.sonar_is_down
  metadata={'source_dat':str(rec.dat),'source_identity':self.identity,'ping':index,'date':str(row.get('date','')),'time':str(row.get('time','')),'depth_instrument_m':float(row.inst_dep_m),'latitude':float(row.lat),'longitude':float(row.lon),'instrument_gain':str(row.get('gain','não disponível no arquivo')),'display_settings':settings.json(),'frame':info,'measurement':self.measurement,'preview':False,'orientation':'port_left_starboard_right;cascade_reversed='+str(reverse)}
  isdown=self.s.sonar_is_down
  def work(note,progress,cancel):
   from PIL import Image,ImageDraw,ImageFont,PngImagePlugin
   processed=structural(array,settings,None,isdown);rgba=colors(processed['treated'],replace(settings,gamma=1.,black=0.,white=255.));rgba=rgba[::-1] if reverse else rgba;image=Image.fromarray(rgba);font=ImageFont.truetype(str(Path(__file__).parent/'assets/NotoSans.ttf'),max(14,min(36,image.width//70)))
   caption=f"{metadata['date']} {metadata['time']} | Ping {index+1} | Prof. instrumento {metadata['depth_instrument_m']:.2f} m\n{metadata['latitude']:.6f}, {metadata['longitude']:.6f} | Ganho instrumento: {metadata['instrument_gain']}\nGamma {settings.gamma:.2f} | Níveis {settings.black:g}–{settings.white:g} | {settings.palette} | resolução completa"
   lineheight=font.getbbox('Ag')[3]+8;caption_width=math.ceil(max(font.getlength(line) for line in caption.splitlines()))+16;canvas=Image.new('RGBA',(max(image.width,caption_width),image.height+lineheight*3+18),(10,18,26,255));canvas.paste(image,(0,0));draw=ImageDraw.Draw(canvas);draw.multiline_text((8,image.height+6),caption,font=font,fill=(255,222,178),spacing=8);pnginfo=PngImagePlugin.PngInfo();pnginfo.add_text('SonarStudio',json.dumps(metadata,ensure_ascii=False,allow_nan=False));tmp=Path(path).with_suffix('.incomplete.png');canvas.save(tmp,pnginfo=pnginfo);tmp.replace(path);Path(str(path)+'.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');return {'capture':path}
  self.s.run_job(work,lambda result:self.s.log.appendPlainText('Captura com metadados: '+result['capture']))
 def navigation_rate(self):
  recent=[(t,i) for t,i in self.nav if time.monotonic()-t<1.]
  return sum(abs(b[1]-a[1]) for a,b in zip(recent,recent[1:]))/max(.1,recent[-1][0]-recent[0][0]) if len(recent)>1 else 0.
 def live_tick(self):
  if self.enabled and self.s.ai_continuous.isChecked() and self.navigation_rate()<self.ai_max_rate and time.monotonic()-self.s.last_ai_time>1:self.analyze(False)
 def analyze(self,force=False):
  s=self.s
  if self.frame is not None and (self.frame['index']!=s.slider.value() or self.frame['info']['channel']!=s.channel.currentData()):return
  if self.frame is None or s.sonar_is_down or s.ai_mode.currentIndex()==0 or s.batch_dialog.worker and s.batch_dialog.worker.isRunning():return
  if s.ai_worker and s.ai_worker.isRunning():return
  if not force and self.navigation_rate()>=self.ai_max_rate:return
  rec=s.recording;mode=s.ai_mode.currentIndex();threshold=s.ai_threshold.value();detailed=s.ai_quality.currentIndex()==1;center=min(len(rec.data)-1,s.frame_index//32*32);count=min(64,len(rec.data));start=max(0,min(center-count//2,len(rec.data)-count));info=dict(self.frame['info'],start=start,end=start+count);info.pop('channel_timing',None)
  from model_registry import model_path,checked_hash
  path=model_path('ghostvision' if mode==1 else 'sonarvision');stat=path.stat();model_hash=checked_hash(str(path),stat.st_size,stat.st_mtime_ns);key=inference_key(self.identity,info,model_hash,threshold,detailed);self.ai_busy_key=key;s.last_ai_time=time.monotonic();epoch=s.ai_epoch;identity=self.identity;token=self.token
  # Cache validation reads actual ROI content in the worker, not file timestamps.
  from app import Worker
  channel,water=info['channel'],info['remove_water'];cache=self.cache
  def work(note,progress,cancel):
   from batch_ai import physical_image,ground_mask
   from sonar_ai import detect_yolo,detect_multiclass,locate
   raw,actual=rec.waterfall(center,channel,water,count,range_mode=info['range_mode'])
   if actual['part_width']!=info['part_width']:
    old_width=actual['part_width'];wanted=info['part_width'];parts=[]
    for number in range(2 if channel=='both' else 1):
     source=raw[:,number*old_width:(number+1)*old_width];part=np.zeros((len(raw),wanted),np.uint8);n=min(old_width,wanted)
     if channel=='both' and number==0 or str(channel).startswith('ss_port'):part[:,-n:]=source[:,-n:]
     else:part[:,:n]=source[:,:n]
     parts.append(part)
    raw=np.concatenate(parts,axis=1);actual['part_width']=wanted
   from viewer_inference import roi_identity
   content=roi_identity(raw,rec.data.iloc[actual['start']:actual['end']])
   weights_hash=hashlib.sha256(path.read_bytes()).hexdigest()
   if weights_hash!=model_hash:raise ValueError('Pesos alterados: recarregue o modelo antes de analisar.')
   key=inference_key(identity,actual,weights_hash,threshold,detailed,content)
   cached=cache.get(key)
   if cached:return dict(cached,cache_hit=True)
   masked=ground_mask(raw,rec,actual);physical,ratio=physical_image(masked,rec,actual)
   detector=detect_yolo if mode==1 else detect_multiclass;max_side=(8192 if detailed else 4096) if mode==1 else (2048 if detailed else 1024);items,metadata=detector(physical,threshold,max_tiles=192,cancel=cancel,max_side=max_side)
   for item in items:item['box'][1]/=ratio;item['box'][3]/=ratio
   located=locate(items,rec,actual);located=[item for item in located if not item.get('in_water_column')]
   for item in located:item['id']=hashlib.sha256((item['id']+'|'+model_hash).encode()).hexdigest()[:16];item.update(frame_start=actual['start'],frame_channel=actual['channel'],frame_part_width=actual['part_width'],frame_water=actual['remove_water'],source_dat=str(rec.dat),model_sha256=model_hash,inference_row_ratio=ratio)
   value={'items':located,'metadata':metadata,'source_identity':identity,'window_start':actual['start'],'window_end':actual['end'],'window_center':center,'model_sha256':model_hash,'cache_key':key,'mode':mode,'threshold':threshold,'detailed':detailed,'channel':channel,'water':water};cache.put(key,value);return value
  s.ai_worker=Worker(work);s.ai_worker.ready.connect(lambda value:self.accept_ai(value,epoch,identity,start,start+count,center,bool(value.get('cache_hit')),force,token));s.ai_worker.failed.connect(s.log.appendPlainText);s.ai_worker.note.connect(s.log.appendPlainText);s.ai_worker.finished.connect(s.ai_finished);s.ai_worker.start()
 def accept_ai(self,value,epoch,identity,start,end,center,cached,force=False,request_token=None):
  s=self.s
  if self.closed or epoch!=s.ai_epoch or identity!=self.identity:return
  if value.get('mode')!=s.ai_mode.currentIndex() or value.get('threshold')!=s.ai_threshold.value() or value.get('detailed')!=(s.ai_quality.currentIndex()==1) or value.get('channel')!=s.channel.currentData() or value.get('water')!=(s.water.isChecked() and not s.sonar_is_down):self.discarded_ai+=1;return
  from model_registry import model_path,checked_hash
  model=model_path('ghostvision' if s.ai_mode.currentIndex()==1 else 'sonarvision');stat=model.stat()
  if value['model_sha256']!=checked_hash(str(model),stat.st_size,stat.st_mtime_ns):self.discarded_ai+=1;return
  if not start<=s.slider.value()<end or (not force and self.navigation_rate()>=self.ai_max_rate) or (request_token is not None and request_token!=self.token):self.discarded_ai+=1;return
  if cached:self.cache_hits+=1
  self.observations[value['cache_key']]={'index':center,'items':value['items']}
  if len(self.observations)>64:self.observations.pop(next(iter(self.observations)))
  s.ai_ready((value['items'],value['metadata'],epoch));self.draw_overlays();s.statusBar().showMessage(('IA do cache' if cached else 'IA do trecho')+f" · {len(value['items'])} candidatos · sem bloquear reprodução")
 def export_metrics(self):
  path,_=QFileDialog.getSaveFileName(self.s,'Tempos medidos do Viewer',str(self.s.recording.project/'viewer-performance.json') if self.s.recording else 'viewer-performance.json','JSON (*.json)')
  if path:Path(path).write_text(json.dumps({'version':'4.7-Pro-Analysis-test.1','renderer':self.shader.renderer,'backend':self.shader.last_backend,'fallback_reason':self.shader.reason,'samples':list(self.metrics),'inference_cache_hits':self.cache_hits,'discarded_ai':self.discarded_ai},indent=2),encoding='utf-8')
 def state(self):
  return {'vertical_percent':self.vertical.value(),'context_pings':self.context.value(),'virtual_scroll':self.virtual.isChecked(),'ai_max_rate':self.ai_max_rate,'background':self.background.isChecked(),'split_channels':self.split.isChecked(),'comparison_split':self.comparison.value(),'altitude_override_m':self.altitude_override,'depth_uncertainty_m':self.depth_uncertainty,'picking_samples':self.picking_samples,'slope_scenario_deg':self.slope_deg}
 def restore(self,state):
  self.vertical.setValue(int(state.get('vertical_percent',100)));self.context.setValue(int(state.get('context_pings',self.context.value())));self.virtual.setChecked(bool(state.get('virtual_scroll',True)))
  self.ai_max_rate=float(state.get('ai_max_rate',10.));self.background.setChecked(bool(state.get('background',self.enabled)));self.split.setChecked(bool(state.get('split_channels',False)));self.comparison.setValue(int(state.get('comparison_split',50)));self.altitude_override=state.get('altitude_override_m');self.depth_uncertainty=float(state.get('depth_uncertainty_m',.2));self.picking_samples=float(state.get('picking_samples',2));self.slope_deg=float(state.get('slope_scenario_deg',2))
 def prepare_close(self):
  self.closed=True;self.live_timer.stop();self.color_timer.stop();self.hover_timer.stop();self.measure_timer.stop();self.pending=None
  if self.stream:self.stream.close();self.stream=None
  busy=False
  for worker in [self.filter_worker,self.profile_worker,self.signature_worker]:
   if worker and worker.isRunning():worker.stop.set();worker.finished.connect(self.s.close);busy=True
  if busy:return False
  self.shader.close();self.down_shader.close();return True


