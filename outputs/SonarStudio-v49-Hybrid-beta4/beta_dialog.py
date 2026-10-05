"""One configurable action for a new DAT and all beta products."""
from pathlib import Path
import json
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QDialog,QVBoxLayout,QFormLayout,QHBoxLayout,QLineEdit,QPushButton,QFileDialog,QLabel,QCheckBox,QComboBox,QDoubleSpinBox,QProgressBar,QWidget,QScrollArea
from recording import ROOT,Recording
from beta_workflow import WorkflowSettings,workflow_target,run_workflow

class BetaDialog(QDialog):
 def __init__(self,studio):
  super().__init__(studio);self.studio=studio;self.setWindowTitle('Novo levantamento — processamento beta');self.resize(740,700)
  layout=QVBoxLayout(self);body=QWidget();form=QFormLayout(body);scroll=QScrollArea();scroll.setWidgetResizable(True);scroll.setWidget(body);layout.addWidget(scroll)
  self.dat=QLineEdit();row=QHBoxLayout();row.addWidget(self.dat);button=QPushButton('Selecionar .DAT');button.clicked.connect(self.choose_dat);row.addWidget(button);form.addRow('Gravação',row)
  self.target=QLineEdit();self.target.setPlaceholderText('Pasta automática por gravação e configuração');row=QHBoxLayout();row.addWidget(self.target);button=QPushButton('Pasta de saída');button.clicked.connect(self.choose_target);row.addWidget(button);form.addRow('Projeto',row)
  self.temperature=QDoubleSpinBox();self.temperature.setRange(-2,45);self.temperature.setValue(10);self.temperature.setSuffix(' °C');form.addRow('Temperatura da água',self.temperature)
  self.resolution=QComboBox();self.resolution.addItems(['Nativa — maior resolução disponível','5 cm','10 cm','25 cm','1 m — teste rápido']);form.addRow('Mosaico',self.resolution)
  self.depth_filter=QCheckBox('Filtrar mosaico pela profundidade');form.addRow(self.depth_filter)
  self.depth_min=QDoubleSpinBox();self.depth_min.setRange(0,2000);self.depth_min.setSuffix(' m');form.addRow('Profundidade mínima do mosaico',self.depth_min)
  self.depth_max=QDoubleSpinBox();self.depth_max.setRange(.1,2000);self.depth_max.setValue(50);self.depth_max.setSuffix(' m');form.addRow('Profundidade máxima do mosaico',self.depth_max)
  self.align_overlap=QCheckBox('Alinhar sobreposição de passagens (correlação)');form.addRow(self.align_overlap)
  self.cell=QDoubleSpinBox();self.cell.setRange(.1,100);self.cell.setValue(2);self.cell.setSuffix(' m');form.addRow('Célula da batimetria TIN',self.cell)
  self.google=QDoubleSpinBox();self.google.setRange(.05,20);self.google.setValue(1);self.google.setSuffix(' m');form.addRow('Resolução Google Earth',self.google)
  self.mosaic=QCheckBox('Gerar mosaico GeoTIFF georreferenciado');self.bathy=QCheckBox('Gerar batimetria TIN e isóbatas');self.ai=QCheckBox('Varrer com modelos especializados em sonar');self.exports=QCheckBox('Exportar KML, KMZ, GPX, SHP e GeoPackage')
  for widget in [self.mosaic,self.bathy,self.ai,self.exports]:widget.setChecked(True);form.addRow(widget)
  self.progress=QProgressBar();layout.addWidget(self.progress);self.status=QLabel('Selecione o DAT ao lado da pasta de mesmo nome com os SON.');self.status.setWordWrap(True);layout.addWidget(self.status)
  row=QHBoxLayout();self.start_button=QPushButton('Importar e processar');self.start_button.clicked.connect(self.start);row.addWidget(self.start_button);button=QPushButton('Cancelar');button.clicked.connect(studio.cancel_job);row.addWidget(button);button=QPushButton('Abrir projeto beta');button.clicked.connect(self.open_project);row.addWidget(button);layout.addLayout(row)
  note=QLabel('Os originais são preservados. A resolução nativa pode gerar arquivos grandes.\nO GeoTIFF mantém a resolução escolhida; Google Earth usa a resolução acima.\nIA produz candidatos para revisão; não certifica objetos nem precisão hidrográfica.\nRepetir a mesma configuração retoma os blocos do mosaico e a varredura da IA.');note.setWordWrap(True);layout.addWidget(note)
 def open_project(self):
  if self.studio.worker and self.studio.worker.isRunning():return
  path,_=QFileDialog.getOpenFileName(self,"Abrir projeto beta",str(ROOT/"outputs"),"Projeto beta (projeto-beta.json)")
  if not path:return
  try:
   result=json.loads(Path(path).read_text(encoding="utf-8"))
   if not result.get("metadata_project") or not result.get("source_dat"):raise ValueError("Projeto beta inválido.")
   self.studio.ai_epoch+=1;self.studio.batch_dialog.cancel();self.studio.pause_playback();self.ready(result)
  except Exception as error:self.status.setText(str(error))

 def choose_dat(self):
  path,_=QFileDialog.getOpenFileName(self,'Selecionar Humminbird DAT',str(ROOT/'work'),'Humminbird (*.DAT *.dat)')
  if path:self.dat.setText(path);self.target.clear()
 def choose_target(self):
  path=QFileDialog.getExistingDirectory(self,'Pasta do novo projeto beta',str(ROOT/'outputs'))
  if path:self.target.setText(path)
 def settings(self):return WorkflowSettings(ai_model='objects',align_overlap=self.align_overlap.isChecked(),temperature=self.temperature.value(),mosaic_resolution=[None,.05,.1,.25,1.][self.resolution.currentIndex()],bathy_resolution=self.cell.value(),google_resolution=self.google.value(),mosaic=self.mosaic.isChecked(),bathymetry=self.bathy.isChecked(),ai=self.ai.isChecked(),exports=self.exports.isChecked(),mosaic_depth_min=self.depth_min.value() if self.depth_filter.isChecked() else None,mosaic_depth_max=self.depth_max.value() if self.depth_filter.isChecked() else None)
 def start(self):
  s=self.studio
  if any(w and w.isRunning() for w in [s.worker,s.ai_worker,s.batch_dialog.worker]):self.status.setText('Aguarde ou cancele o processamento atual antes de iniciar outro.');return
  try:
   settings=self.settings();dat=Path(self.dat.text().strip());target=Path(self.target.text().strip()) if self.target.text().strip() else workflow_target(dat,settings)
  except Exception as error:self.status.setText(str(error));return
  s.pause_playback();s.ai_epoch+=1;s.ai_continuous.setChecked(False);self.start_button.setEnabled(False);self.status.setText(str(target));self.progress.setValue(0)
  s.run_job(lambda note,progress,cancel:run_workflow(dat,target,settings,note,progress,cancel),self.ready)
  if s.worker:
   s.worker.progress.connect(lambda p:self.progress.setValue(int(p.get('percent',0))));s.worker.failed.connect(self.status.setText);s.worker.finished.connect(lambda:self.start_button.setEnabled(True))
 def ready(self,result):
  s=self.studio
  if any(w and w.isRunning() for w in [s.sonar_worker,s.object_review.crop_worker]):QTimer.singleShot(40,lambda:self.ready(result));return
  s.pending_project=None;s.recording_loaded(Recording(result['source_dat'],result['metadata_project']))
  if result.get('bathy_result'):s.bathy_result=result['bathy_result'];s.bathymetry_path=result['products']['bathymetry'];s.load_contours(result['products']['contours'])
  if result['products'].get('mosaic'):s.open_mosaic(result['products']['mosaic'])
  elif result['products'].get('bathymetry'):s.open_mosaic(result['products']['bathymetry'])
  if result['products'].get('candidates'):
   items=json.loads(Path(result['products']['candidates']).read_text(encoding='utf-8'))['objects'];s.ai_ready((items,{'model':'varredura beta'},s.ai_epoch))
  self.progress.setValue(100 if result.get('success') else 0);self.status.setText(('Concluído. ' if result.get('success') else 'Projeto parcial. ')+'Produtos em '+str(result['products'].get('exports',Path(result['metadata_project']).parent)));self.hide()
