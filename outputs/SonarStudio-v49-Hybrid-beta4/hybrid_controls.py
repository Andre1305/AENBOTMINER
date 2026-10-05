"""Explicit beta backend selection; no automatic GPU promotion."""
import os
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QHBoxLayout,QComboBox,QLabel
from native_engine import ENGINE
from viewer_gpu import ColorShader

class HybridControls:
 def __init__(self,controller,layout):
  self.c=controller;self.s=controller.s;self.closed=False
  row=QHBoxLayout();row.addWidget(QLabel('Motor 4.9 beta'))
  self.mode=QComboBox();self.mode.addItems(['Legado — alta fidelidade','Híbrido LLVM / CPU','Híbrido + GPU experimental']);row.addWidget(self.mode)
  self.note=QLabel();self.note.setWordWrap(True);row.addWidget(self.note,1);layout.insertLayout(1,row)
  self.mode.setToolTip('LLVM acelera leitura, alcance e paleta; filtros científicos permanecem SciPy. GPU tem fallback CPU e leitura de framebuffer: não é zero-copy completo.')
  self.force_legacy=os.environ.get('SONARSTUDIO_FORCE_LEGACY')=='1'
  if self.force_legacy:
   self.mode.model().item(1).setEnabled(False);self.mode.model().item(2).setEnabled(False)
  self.mode.currentIndexChanged.connect(self.changed)
  if self.force_legacy:self.changed(0)
  else:self.mode.setCurrentIndex(1)
  self.timer=QTimer(self.s);self.timer.setInterval(500);self.timer.timeout.connect(self.status);self.timer.start()
 def changed(self,index):
  if self.force_legacy and index!=0:
   self.mode.blockSignals(True);self.mode.setCurrentIndex(0);self.mode.blockSignals(False);index=0
  c=self.c;s=self.s
  s.pause_playback();c.token+=1;c.filter_token+=1;c.latest_descriptor=None;c.filter_key=None
  if c.stream:c.stream.close();c.stream=None
  c.shader.close();c.down_shader.close();c.third_shader.close();c.shader=ColorShader(legacy=index!=2);c.down_shader=ColorShader(legacy=index!=2);c.third_shader=ColorShader(legacy=index!=2)
  c.shader.compiled=index!=0;c.down_shader.compiled=index!=0;c.third_shader.compiled=index!=0
  if index:ENGINE.start()
  if s.recording:
   s.recording.set_backend('compiled' if index else 'legacy')
   if c.enabled:
    from viewer_stream import WindowStream
    c.stream=WindowStream(s.recording);c.stream.ready.connect(c.loaded);c.stream.failed.connect(s.log.appendPlainText)
   s.schedule_sonar()
  self.status()
 def recording_loaded(self,rec):rec.set_backend('compiled' if self.mode.currentIndex() else 'legacy')
 def status(self):
  if self.closed:return
  state=ENGINE.status();mode=self.mode.currentIndex()
  text='Legado CPU' if not mode else state['state']
  if mode==2:text+=' | '+self.c.shader.backend+((' — '+self.c.shader.reason) if self.c.shader.reason else '')
  self.note.setText(text)
 def live_colors(self,settings):
  return self.mode.currentIndex()==2 and not self.c.frozen and settings.method=='Original' and settings.destripe==0 and settings.clahe==0 and settings.sharpen==0
 def state(self):return {'processing_mode':self.mode.currentIndex()}
 def restore(self,state):
  mode=int(state.get('processing_mode',1));self.mode.setCurrentIndex(0 if self.force_legacy else mode if mode in (0,1,2) else 1)
 def close(self):self.closed=True;self.timer.stop()
