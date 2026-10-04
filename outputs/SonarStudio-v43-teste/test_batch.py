import unittest,tempfile,json
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
import app
import numpy as np
from recording import Recording,ROOT
from batch_ai import BatchSettings,windows,run_batch,analyze_window,duplicate
from sonar_radiometry import radiometry,shadow_evidence

class BatchTests(unittest.TestCase):
 def setUp(self):self.rec=Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado')
 def tearDown(self):self.rec.close()
 def target(self):return Path(tempfile.mkdtemp(prefix='batch-test-',dir=ROOT/'work/tmp'))
 def test_coverage_and_sampling(self):
  for n in [1,15,64,65,127,77533]:
   mask=np.zeros(n,bool)
   for first,last in windows(n,BatchSettings()):mask[first:last]=True
   self.assertTrue(mask.all())
  mask=np.zeros(1000,bool)
  for first,last in windows(1000,BatchSettings(sample_factor=3)):mask[first:last]=True
  self.assertFalse(mask.all())
 def test_cancel_resume_and_identity(self):
  target=self.target();settings=BatchSettings(last_ping=191);seen=[]
  with patch('batch_ai.analyze_window',return_value=([],{'anomaly_calls':1})) as inference:
   result=run_batch(self.rec,target,settings,lambda value:seen.append(value),cancel=lambda:bool(seen and seen[-1]['done']>=1))
   self.assertEqual(result['status'],'cancelled');self.assertEqual(result['done'],1)
   result=run_batch(self.rec,target,settings);self.assertTrue(result['success']);self.assertEqual(result['covered_pings'],192)
   self.assertEqual(inference.call_count,result['total'])
   before=inference.call_count;run_batch(self.rec,target,settings);self.assertEqual(before,inference.call_count)
   with self.assertRaises(ValueError):run_batch(self.rec,target,replace(settings,threshold=.5))
 def test_specialized_ensemble_runs_both_without_anomaly_gate(self):
  array,info=self.rec.waterfall(12000,'both',True,64)
  with patch('batch_ai.detect_yolo',return_value=([],{})) as yolo,patch('batch_ai.detect_multiclass',return_value=([],{})) as multi:
   items,stats=analyze_window(self.rec,array,info,BatchSettings());self.assertEqual(items,[]);self.assertEqual(stats['anomaly_calls'],0);yolo.assert_called_once();multi.assert_called_once()
 def test_unspecialized_model_rejected(self):
  array,info=self.rec.waterfall(12000,'both',True,64)
  with self.assertRaises(ValueError):analyze_window(self.rec,array,info,BatchSettings(model='anomaly'))
 def test_radiometry_and_evidence(self):
  a=np.tile(np.linspace(20,180,256).astype(np.uint8),(64,1));a[:,:10]=0
  self.assertTrue(np.array_equal(a,radiometry(a,128)))
  b=radiometry(a,128,True,True);self.assertEqual(a.shape,b.shape);self.assertTrue((b[:,:10]==0).all());self.assertEqual(b.dtype,np.uint8)
  a=np.full((32,128),80,np.uint8);a[8:24,60:80]=220;a[8:24,80:100]=15
  ev=shadow_evidence(a,{'box':[60,8,80,24],'channel':'ss_star'},{});self.assertGreater(ev['contrast'],.8);self.assertFalse(ev['height_estimated'])
 def test_real_model_batch_checkpoint(self):
  result=run_batch(self.rec,self.target(),BatchSettings(model='covos',first_ping=12000,last_ping=12063,remove_water=True))
  self.assertTrue(result['success']);self.assertEqual(result['covered_pings'],64);self.assertEqual(result['model_calls']['yolo_calls'],1)
  self.assertTrue(all(i['status']=='pendente' for i in result['objects']))

class BatchGuiTests(unittest.TestCase):
 def test_worker_keeps_gui_responsive_and_review_selects_ping(self):
  import shutil,time
  from PySide6.QtCore import QTimer
  from PySide6.QtWidgets import QApplication
  qt=QApplication.instance() or QApplication([])
  project=Path(tempfile.mkdtemp(prefix='batch-gui-',dir=ROOT/'work/tmp'))
  shutil.copytree(ROOT/'outputs/Rec00001-processado/meta',project/'meta')
  w=app.Studio(autoload=False);w.recording_loaded(Recording(ROOT/'work/input/Rec00001.DAT',project))
  dialog=w.batch_dialog;dialog.model.setCurrentIndex(1);dialog.first.setValue(12001);dialog.last.setValue(12064);dialog.water.setChecked(True)
  ticks=[];timer=QTimer();timer.setInterval(25);timer.timeout.connect(lambda:ticks.append(time.monotonic()));timer.start()
  dialog.start();deadline=time.monotonic()+45
  while dialog.worker.isRunning() and time.monotonic()<deadline:qt.processEvents();time.sleep(.005)
  qt.processEvents();timer.stop();self.assertFalse(dialog.worker.isRunning());self.assertGreater(len(ticks),2);self.assertEqual(dialog.progress.value(),100)
  obj={'id':'review-test','label':'Candidato','score':.8,'model':'test','status':'pendente','ping':12020,'channel':'ss_port','frame_channel':'both','frame_water':True,'box':[50,5,70,15],'frame_start':12000,'frame_part_width':1}
  w.objects[obj['id']]=obj;w.object_review.refresh();w.object_review.table.selectRow(w.object_review.ids.index(obj['id']));qt.processEvents();self.assertEqual(w.slider.value(),12020)
  w.pause_playback();w.sonar_timer.stop()
  if w.sonar_worker and w.sonar_worker.isRunning():w.sonar_worker.wait(30000);qt.processEvents()
  w.close();qt.processEvents()

if __name__=='__main__':unittest.main(verbosity=2)
