"""Qt controller for resumable batch triage using real recordings."""
from pathlib import Path
import time
from PySide6.QtCore import Signal,QTimer
from PySide6.QtWidgets import QDialog,QVBoxLayout,QFormLayout,QHBoxLayout,QLabel,QComboBox,QDoubleSpinBox,QSpinBox,QPushButton,QProgressBar,QCheckBox
from batch_ai import BatchSettings,fingerprint,run_batch
from recording import Recording

class BatchDialog(QDialog):
    def __init__(self,studio):
        super().__init__(studio);self.studio=studio;self.worker=None;self.pending={};self.epoch=-1
        self.setWindowTitle('IA — varredura de gravação e retomada');self.resize(700,630)
        layout=QVBoxLayout(self);self.recording_note=QLabel();self.recording_note.setWordWrap(True);layout.addWidget(self.recording_note)
        form=QFormLayout();layout.addLayout(form)
        self.model=QComboBox();self.model.addItems(['Dois modelos especializados em sonar','GhostVision — covos','SonarVision — objetos SSS (experimental)']);self.model.setCurrentIndex(2);self.model.model().item(0).setEnabled(False);self.model.model().item(1).setEnabled(False);self.model.view().setRowHidden(0,True);self.model.view().setRowHidden(1,True);form.addRow('Pipeline',self.model)
        self.score=QDoubleSpinBox();self.score.setRange(.05,.99);self.score.setSingleStep(.05);self.score.setValue(.4);form.addRow('Score dos classificadores',self.score)
        self.first=QSpinBox();self.first.setRange(1,100000000);self.first.setValue(1);form.addRow('Primeiro ping',self.first)
        self.last=QSpinBox();self.last.setRange(1,100000000);form.addRow('Último ping',self.last)
        self.sampling=QComboBox();self.sampling.addItems(['Cobrir todos os pings','Amostragem rápida — passo 3×','Amostragem rápida — passo 6×']);form.addRow('Cobertura',self.sampling)
        self.detailed=QCheckBox('Maior resolução de análise (mais lenta)');form.addRow(self.detailed)
        self.water=QCheckBox('Corrigir slant-range / remover coluna d’água');form.addRow(self.water)
        self.gain=QCheckBox('Normalização empírica de ganho — experimental');form.addRow(self.gain)
        self.stripes=QCheckBox('Atenuar listras com FFT — experimental');form.addRow(self.stripes)
        self.triage=QCheckBox('Triagem Isolation Forest antes dos modelos sonar — experimental');form.addRow(self.triage)
        self.triage.setToolTip('Anomalias de textura, sem identificar objetos. Pode omitir alvos; desligue para varredura completa dos classificadores. Um bloco em dez é auditado sem triagem.')
        self.contamination=QDoubleSpinBox();self.contamination.setRange(.01,.3);self.contamination.setSingleStep(.01);self.contamination.setValue(.1);form.addRow('Fração de patches atípicos na triagem',self.contamination)
        self.progress=QProgressBar();self.progress.setRange(0,100);layout.addWidget(self.progress)
        self.stats=QLabel('Sem varredura iniciada.');self.stats.setWordWrap(True);layout.addWidget(self.stats)
        buttons=QHBoxLayout();self.start_button=QPushButton('Iniciar / retomar');self.start_button.clicked.connect(self.start);buttons.addWidget(self.start_button)
        self.cancel_button=QPushButton('Cancelar e guardar checkpoint');self.cancel_button.setEnabled(False);self.cancel_button.clicked.connect(self.cancel);buttons.addWidget(self.cancel_button)
        inspect=QPushButton('Revisar / exportar candidatos');inspect.clicked.connect(studio.show_objects);buttons.addWidget(inspect);layout.addLayout(buttons)
        note=QLabel('Usa os SON reais e navegação; os classificadores foram treinados com imagens de sonar.\nNenhuma anomalia sem classe é incluída como objeto identificado.\nScores não são probabilidades calibradas; todos os candidatos continuam pendentes de revisão.\nCancelamento preserva blocos concluídos. Mesma configuração retoma o checkpoint automaticamente.');note.setWordWrap(True);layout.addWidget(note)
        self.timer=QTimer(self);self.timer.setInterval(1000);self.timer.timeout.connect(self.flush)
    def recording_changed(self):
        if self.worker and self.worker.isRunning():self.cancel()
        rec=self.studio.recording
        if not rec:return
        self.first.setMaximum(len(rec.data));self.last.setMaximum(len(rec.data));self.last.setValue(len(rec.data));self.first.setValue(1)
        seconds=float(rec.data.time_s.max()-rec.data.time_s.min());self.recording_note.setText(f'{rec.dat.name} · {len(rec.data):,} pings reais · {seconds/3600:.2f} h\nAmostragem sonar: {rec.sampling_m*100:.3f} cm; posicionamento dos alvos aproximado.')
    def settings(self):
        return BatchSettings(model=['sonar_ensemble','covos','objects'][self.model.currentIndex()],threshold=self.score.value(),first_ping=self.first.value()-1,last_ping=self.last.value()-1,sample_factor=[1,3,6][self.sampling.currentIndex()],detailed=self.detailed.isChecked(),remove_water=self.water.isChecked(),gain_normalization=self.gain.isChecked(),fft_destripe=self.stripes.isChecked(),anomaly_triage=self.triage.isChecked(),triage_contamination=self.contamination.value())
    def start(self):
        rec=self.studio.recording
        if not rec or (self.worker and self.worker.isRunning()):return
        if not any(k.startswith('ss_port') for k in rec.channels) or not any(k.startswith('ss_star') for k in rec.channels):
            self.stats.setText('A varredura lateral combinada exige bombordo e boreste.');return
        if self.studio.ai_worker and self.studio.ai_worker.isRunning():self.stats.setText('Aguarde a análise interativa atual terminar.');return
        self.studio.ai_continuous.setChecked(False);settings=self.settings();dat,project=rec.dat,rec.project
        self.epoch=self.studio.ai_epoch;epoch=self.epoch;self.pending={};self.progress.setValue(0)
        from app import Worker
        class BatchWorker(Worker):candidates=Signal(object)
        def work(note,progress,cancel):
            recording=Recording(dat,project)
            try:
                identity,_=fingerprint(recording,settings);target=project/'ai-batch'/identity[:20]
                note(f'IA em lote: {len(recording.data):,} pings disponíveis; checkpoint {target}')
                return run_batch(recording,target,settings,progress,self.worker.candidates.emit,cancel)
            finally:recording.close()
        self.worker=BatchWorker(work);self.worker.candidates.connect(lambda items:self.queue(items,epoch));self.worker.progress.connect(self.update_progress);self.worker.note.connect(self.studio.log.appendPlainText);self.worker.ready.connect(self.ready);self.worker.failed.connect(self.failed);self.worker.finished.connect(self.worker_finished)
        self.set_busy(True);self.timer.start();self.worker.start()
    def set_busy(self,busy):
        self.start_button.setEnabled(not busy);self.cancel_button.setEnabled(busy)
        for widget in [self.model,self.score,self.first,self.last,self.sampling,self.detailed,self.water,self.gain,self.stripes,self.triage,self.contamination]:widget.setEnabled(not busy)
    def queue(self,items,epoch):
        if epoch!=self.studio.ai_epoch:return
        for item in items:self.pending[item['id']]=item
    def flush(self):
        if self.epoch!=self.studio.ai_epoch:self.pending={};return
        if not self.pending:return
        values=list(self.pending.values());self.pending={};self.studio.ai_ready((values,{'model':'varredura em lote'},self.epoch))
    def update_progress(self,value):
        self.progress.setValue(int(value['percent']));eta=value.get('eta_s');duration='calculando' if eta is None else f'{eta/60:.1f} min'
        self.stats.setText(f'{value["done"]}/{value["total"]} blocos · {value["candidates"]} candidatos no checkpoint\nRestante estimado: {duration} · retomada: {"sim" if value.get("resumed") else "não"}')
    def ready(self,result):
        self.flush()
        if self.epoch==self.studio.ai_epoch:self.queue(result['objects'],self.epoch);self.flush()
        self.progress.setValue(round(100*result['done']/result['total']))
        triage='\nTriagem ligada: classificadores examinaram regiões selecionadas; não garante ausência fora delas.' if result.get('triage_may_miss_objects') else ''
        self.stats.setText(f'{"Concluída" if result["success"] else "Cancelada com checkpoint"}: {result["done"]}/{result["total"]} blocos\n{result["covered_pings"]:,}/{result["requested_pings"]:,} pings lidos · {result["candidates"]} candidatos{triage}\n{result["database"]}')
        self.studio.log.appendPlainText(self.stats.text())
    def failed(self,error):self.flush();self.stats.setText('Erro; blocos concluídos preservados.\n'+error);self.studio.log.appendPlainText(error)
    def worker_finished(self):
        self.timer.stop();self.flush();self.set_busy(False)
        if self.studio.close_when_finished:QTimer.singleShot(0,self.studio.close)
    def cancel(self):
        if self.worker and self.worker.isRunning():self.worker.stop.set();self.stats.setText('Cancelando após a operação atual; preservando o checkpoint…')
    def closeEvent(self,event):
        self.hide();event.ignore()
