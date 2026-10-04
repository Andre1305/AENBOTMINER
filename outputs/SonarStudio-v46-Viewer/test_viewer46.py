import unittest,tempfile,shutil,time,json,math,os
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
import app,numpy as np,pandas as pd
from PySide6.QtWidgets import QApplication,QFileDialog
from PySide6.QtCore import QPointF
from recording import ROOT,Recording
from viewer_metrology import horizontal_range,point_from_pixel,measure_points,shadow_scenarios
from viewer_filters import structural,colors
from viewer_inference import inference_key,interpolated_boxes,ResultCache
from viewer_findings import Findings
from viewer_stream import WindowStream
from image_processing import ImageSettings

class Viewer46MathTests(unittest.TestCase):
 def test_slant_horizontal_and_invalid_water_return(self):
  self.assertEqual(horizontal_range(13,5),12)
  with self.assertRaises(ValueError):horizontal_range(4,5)
 def test_shadow_top_echo_known_geometry_and_scenarios(self):
  a={'side':'port','ping':10,'slant_m':math.sqrt(208),'altitude_m':10,'ground_m':12};b=dict(a,slant_m=math.sqrt(325),incidence_from_vertical_deg=56.31)
  value=shadow_scenarios(a,b,.0192);self.assertAlmostEqual(value['height_m'],2);self.assertLess(value['scenario_min_m'],2);self.assertGreater(value['scenario_max_m'],2);self.assertFalse(value['accuracy_verified'])
  with self.assertRaises(ValueError):shadow_scenarios(a,dict(b,ping=20),.0192)
 def test_real_longitudinal_uses_navigation_then_speed_fallback(self):
  rec=SimpleNamespace(xy=np.array([[0,0],[3,4],[6,8]],float),data=pd.DataFrame({'time_s':[0,1,2],'speed_ms':[5,5,5]}));a={'ping':0,'side':'port','ground_m':12,'east':0,'north':0};b=dict(a,ping=2,east=6,north=8)
  value=measure_points(rec,a,b);self.assertEqual(value['distance_m'],10);self.assertEqual(value['navigation_path_m'],10);self.assertEqual(value['longitudinal_source'],'navigation')
  rec.xy[1]=np.nan;value=measure_points(rec,a,b);self.assertEqual(value['navigation_path_m'],10);self.assertIn('fallback',value['longitudinal_source'])
 def test_cache_key_isolates_model_source_window_and_preprocess(self):
  info={'channel':'both','start':0,'end':64,'part_width':2000,'remove_water':False};key=inference_key('record1',info,'weights1',.35,False)
  self.assertNotEqual(key,inference_key('record2',info,'weights1',.35,False));self.assertNotEqual(key,inference_key('record1',info,'weights2',.35,False));self.assertNotEqual(key,inference_key('record1',dict(info,start=32),'weights1',.35,False));self.assertNotEqual(key,inference_key('record1',dict(info,remove_water=True),'weights1',.35,False))
 def test_interpolation_bracketed_associated_and_not_detection(self):
  a={'id':'a','box':[100,10,140,30],'frame_start':0,'status':'pendente','score':.7,'model':'SSS','class_id':0,'channel':'ss_port','ground_range_m':20,'label':'Objeto'};b=dict(a,box=[102,12,142,32],score=.8)
  result=interpolated_boxes({'index':10,'items':[a]},{'index':12,'items':[b]},11);self.assertEqual(len(result),1);self.assertIsNone(result[0]['score']);self.assertTrue(result[0]['visual_interpolation']);np.testing.assert_allclose(result[0]['box'],[101,11,141,31]);self.assertEqual(interpolated_boxes({'index':10,'items':[a]},{'index':12,'items':[b]},13),[])
  self.assertEqual(interpolated_boxes({'index':10,'items':[a]},{'index':12,'items':[dict(b,class_id=1)]},11),[])
 def test_structural_preview_does_not_mutate_raw_and_full_freeze(self):
  a=np.arange(800*100,dtype=np.uint8).reshape(100,800);original=a.copy();opts=ImageSettings(method='Mediana',clahe=.2);preview=structural(a,opts,(200,50));full=structural(a,opts,None)
  self.assertTrue(preview['preview']);self.assertFalse(full['preview']);self.assertEqual(full['treated'].shape,a.shape);self.assertIn('clahe',preview['filter_ms']);np.testing.assert_array_equal(a,original)
 def test_persistent_cache_bound_and_manual_findings_roundtrip(self):
  folder=Path(tempfile.mkdtemp(dir=ROOT/'work/tmp'));cache=ResultCache(folder/'cache.sqlite',max_entries=2)
  for i in range(3):cache.put(str(i),{'items':[],'model':'sonar'})
  self.assertIsNone(cache.get('0'));self.assertEqual(ResultCache(folder/'cache.sqlite').get('2')['model'],'sonar');findings=Findings(folder/'findings.sqlite');identity=findings.add('controlled.dat',12,'Rocha',{'manual':True,'hypotheses':['flat bottom']});self.assertEqual(findings.list('controlled.dat')[0]['metadata']['hypotheses'],['flat bottom']);findings.remove(identity);self.assertEqual(findings.list('controlled.dat'),[])

class Viewer46GuiTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.qt=QApplication.instance() or QApplication([])
 def setUp(self):
  self.folder=Path(tempfile.mkdtemp(prefix='viewer46-',dir=ROOT/'work/tmp'));(self.folder/'meta').mkdir()
  for path in (ROOT/'outputs/Rec00002-beta/metadados/meta').glob('*.csv'):shutil.copy2(path,self.folder/'meta'/path.name)
  self.rec=Recording(ROOT/'work/input-rec00002/Rec00002.DAT',self.folder)
  with patch.dict(os.environ,{'SONAR_VIEWER_COMPAT':'0'}):self.w=app.Studio(autoload=False)
  self.w.recording_loaded(self.rec);self.w.show();self.w.set_index(12000);self.wait(lambda:self.w.frame_index==12000 and self.w.viewer_controller.filtered is not None)
 def wait(self,condition,seconds=20):
  deadline=time.monotonic()+seconds
  while time.monotonic()<deadline:
   self.qt.processEvents();time.sleep(.003)
   if condition():return
  self.fail('Viewer condition timed out: '+self.w.log.toPlainText()[-500:])
 def tearDown(self):
  self.w.pause_playback();self.w.sonar_timer.stop();self.w.close();self.wait(lambda:not self.w.isVisible());self.qt.processEvents()
 def test_split_master_timestamps_and_missing_channel_blank(self):
  c=self.w.viewer_controller;c.split.setChecked(True);self.wait(lambda:getattr(c,'down_filtered',None) is not None);self.assertEqual(c.frame['info']['start'],c.frame['down'][1]['start']);self.assertEqual(c.frame['info']['end'],c.frame['down'][1]['end']);self.assertLess(c.frame['down'][1]['channel_timing']['ds_vhighfreq']['max_skew_s'],.01)
  original=self.rec.channels['ds_vhighfreq'];self.rec.channels['ds_vhighfreq']=original.assign(time_s=original.time_s+1e6)
  try:
   raw,info=self.rec.waterfall(12000,'ds_vhighfreq',False,64);self.assertEqual(raw.max(),0);self.assertFalse(any(info['channel_timing']['ds_vhighfreq']['valid_rows']))
  finally:self.rec.channels['ds_vhighfreq']=original
 def test_latest_requests_discard_old_frames_and_prefetch_is_bounded(self):
  c=self.w.viewer_controller
  for index in [5000,15000,6000,16000,7000,17000]:self.w.set_index(index);self.qt.processEvents()
  self.wait(lambda:self.w.frame_index==17000 and c.frame['index']==17000);self.assertLessEqual(c.stream.bytes,c.stream.budget);self.assertLess(self.rec.cache_bytes,65*1024**2)
 def test_ruler_no_mirror_and_manual_tag_hypotheses(self):
  c=self.w.viewer_controller;c.tool.setCurrentText('Régua');f=c.filtered;part=c.frame['info']['part_width'];left=self.w.sonar_crop[0];h=float(self.rec.data.iloc[12000].inst_dep_m);sampling=c.frame['info']['sampling_m'];x1=part+round((h+10)/sampling);x2=part+round((h+12)/sampling);y=(12000-c.frame['info']['start'])/f['scale_y']
  c.measure(QPointF((x1-left)/f['scale_x'],y),QPointF((x2-left)/f['scale_x'],y));self.assertIsNotNone(c.measurement);self.assertEqual(c.measurement['a']['side'],'starboard');self.assertGreater(c.measurement['distance_m'],0);c.quick_tag('Alvo');saved=c.findings.list(self.rec.dat);self.assertEqual(len(saved),1);self.assertTrue(saved[0]['metadata']['not_ai_detection']);self.assertFalse(saved[0]['metadata']['measurement']['accuracy_verified'])
 def test_freeze_full_resolution_and_split_does_not_duplicate(self):
  c=self.w.viewer_controller;self.w.professional.compare.setChecked(True);c.refilter();self.wait(lambda:c.filtered is not None);c.freeze.setChecked(True);self.wait(lambda:c.filtered is not None and not c.filtered['preview']);self.assertEqual(c.filtered['treated'].shape[1],self.w.sonar_crop[1]-self.w.sonar_crop[0]);c.comparison.setValue(25);self.qt.processEvents();self.assertEqual(self.w.sonar.image.width(),c.filtered['treated'].shape[1])
 def test_live_filter_burst_then_levels_preserves_structural_result(self):
  c=self.w.viewer_controller;self.w.professional.method.setCurrentText('Mediana');self.w.professional.clahe.setValue(.35);self.w.professional.sharpen.setValue(.3)
  c.refilter();c.refilter();self.wait(lambda:c.filtered is not None and 'clahe' in c.filtered['filter_ms'] and 'sharpen' in c.filtered['filter_ms'] and not c.filter_worker.isRunning())
  result=c.filtered;self.w.professional.black.setValue(10);self.w.professional.white.setValue(230);self.w.gamma.setValue(150)
  self.wait(lambda:c.options().black==10 and c.options().white==230);self.qt.processEvents();self.assertIs(c.filtered,result);self.assertFalse(c.filter_worker.isRunning())
 def test_snapshot_contains_complete_pixels_and_embedded_metadata(self):
  from PIL import Image
  c=self.w.viewer_controller;target=self.folder/'snapshot.png'
  with patch.object(QFileDialog,'getSaveFileName',return_value=(str(target),'PNG')):c.snapshot()
  self.wait(lambda:target.is_file());self.wait(lambda:self.w.worker is not None and not self.w.worker.isRunning());self.qt.processEvents()
  image=Image.open(target);metadata=json.loads(image.info['SonarStudio']);self.assertFalse(metadata['preview']);self.assertEqual(metadata['ping'],12000);self.assertEqual(metadata['frame']['channel'],'both');self.assertEqual(metadata['display_settings']['palette'],self.w.palette.currentText());self.assertGreater(image.height,self.w.raw_array.shape[0]);self.assertGreaterEqual(image.width,self.w.raw_array.shape[1]);self.assertTrue(Path(str(target)+'.json').exists());image.close()
 def test_real_specialized_inference_cache_and_stale_discard(self):
  c=self.w.viewer_controller;self.w.ai_mode.setCurrentIndex(2);c.nav.clear();c.analyze(True);self.wait(lambda:self.w.ai_worker is not None and not self.w.ai_worker.isRunning(),seconds=40);self.wait(lambda:bool(c.observations));self.assertTrue(c.observations,self.w.log.toPlainText()+" discarded="+str(c.discarded_ai));hits=c.cache_hits;c.analyze(True);self.qt.processEvents();self.assertEqual(c.cache_hits,hits+1)
  value=next(iter(c.observations.values()));before=c.discarded_ai;dummy={'mode':2,'threshold':self.w.ai_threshold.value(),'detailed':False,'channel':'wrong','water':False};c.accept_ai(dummy,self.w.ai_epoch,c.identity,0,64,32,False);self.assertEqual(c.discarded_ai,before+1)
if __name__=='__main__':unittest.main()


