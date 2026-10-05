import unittest,tempfile,json
from pathlib import Path
from unittest.mock import patch
from dataclasses import replace
import app
import numpy as np
from PIL import Image
from PySide6.QtWidgets import QApplication,QFileDialog
from PySide6.QtGui import QImage,QPainter
from PySide6.QtCore import QRectF
from recording import ROOT,Recording
from viewer_geometry import suppress_range_pulses,display_crop,display_box
from image_processing import ImageSettings,enhance,colorize

class RangeGeometryTests(unittest.TestCase):
 def test_only_short_upward_range_pulses_are_suppressed(self):
  a=np.array([50,50,135,50,50,135,135,50,50,100,100,100,100,50,50,25,50,50],float);original=a.copy()
  filtered=suppress_range_pulses(a)
  self.assertTrue((filtered[[2,5,6]]==50).all());self.assertTrue((filtered[9:13]==100).all());self.assertEqual(filtered[15],25)
  np.testing.assert_array_equal(a,original)
 def test_crop_keeps_port_and_starboard_at_their_original_sides(self):
  info={'channel':'both','part_width':8,'display_part_width':4};a=np.array([[8,7,6,5,4,3,2,1,11,12,13,14,15,16,17,18]],np.uint8)
  crop=display_crop(info);np.testing.assert_array_equal(a[:,slice(*crop)],[[4,3,2,1,11,12,13,14]])
  self.assertEqual(display_crop(dict(info,channel='ss_port')),(4,8));self.assertEqual(display_crop(dict(info,channel='ss_star')),(0,4))
  self.assertEqual(display_crop(info,1),(0,16));self.assertIsNone(display_crop(dict(info,channel='ds_highfreq')))
 def test_reverse_boxes_changes_only_y_and_preserves_sample_coordinates(self):
  self.assertEqual(display_box([100,10,150,30],90,64,True),[10,34,60,54])
  self.assertEqual(display_box([100,10,150,30],90,64,False),[10,10,60,30])
 def test_native_samples_are_preserved_when_preview_is_cropped(self):
  original=np.arange(256,dtype=np.uint8).reshape(8,32);a=original.copy()
  preview=app.Studio.process_sonar((a,ImageSettings(),False,1.,1.,1,False,None,(8,24)))[0]
  np.testing.assert_array_equal(preview,original[:,8:24]);np.testing.assert_array_equal(a,original)

class Viewer44Tests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.qt=QApplication.instance() or QApplication([])
 def setUp(self):
  self.w=app.Studio(autoload=False);self.w.resize(1280,800);self.w.show();self.qt.processEvents()
  self.w.recording_loaded(Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados'))
  self.w.professional.pings.setValue(64);self.w.set_index(12015);self.w.sonar_timer.stop();self.w.show_sonar();self.qt.processEvents()
 def tearDown(self):
  self.w.pause_playback();self.w.sonar_timer.stop()
  for worker in [self.w.worker,self.w.ai_worker,self.w.sonar_worker]:
   if worker and worker.isRunning():worker.wait(30000);self.qt.processEvents()
  self.w.close();self.qt.processEvents()
 def test_real_isolated_wide_ping_remains_complete_and_does_not_expand_preview(self):
  r=self.w.recording;frames=[r.waterfall(i,'both',False,64) for i in [11960,12000,12015,12080]]
  self.assertGreater(len({a.shape[1] for a,info in frames}),1)
  crops=[display_crop(info) for a,info in frames];self.assertEqual(len({right-left for left,right in crops}),1)
  a,info=frames[2];raw,row=r.raw_ping('ss_port',12015);y=12015-info['start']
  np.testing.assert_array_equal(a[y,:info['part_width']][::-1],raw)
  self.assertGreater(info['part_width'],info['display_part_width']);self.assertEqual(self.w.professional.range_mode.currentIndex(),0)
 def test_viewport_does_not_jump_when_range_pulse_enters_and_leaves(self):
  transforms=[]
  for i in [11960,12000,12015,12080]:
   self.w.set_index(i);self.w.sonar_timer.stop();self.w.show_sonar();self.qt.processEvents();t=self.w.sonar.transform();transforms.append((t.m11(),t.m22()))
  np.testing.assert_allclose(transforms,[transforms[0]]*len(transforms),rtol=1e-6)
 def test_direction_reverses_image_vertically_without_mirroring_channels(self):
  a=np.array([[20,40,60,80],[30,50,70,90],[100,120,140,160],[110,130,150,170]],np.uint8)
  view=app.SonarView();view.display(a,1.,'Cinza',reverse=True)
  canvas=QImage(4,4,QImage.Format.Format_RGBA8888);canvas.fill(0);painter=QPainter(canvas);view.scene().render(painter,QRectF(0,0,4,4),QRectF(0,0,4,4));painter.end()
  observed=np.array([[canvas.pixelColor(x,y).red() for x in range(4)] for y in range(4)])
  np.testing.assert_array_equal(observed,a[::-1]);self.assertGreater(view.image_item.transform().m11(),0);self.assertLess(view.image_item.transform().m22(),0)
  view.close()
 def test_cursor_and_candidate_follow_inverted_cascade(self):
  w=self.w;info=w.frame_info;part=info['part_width'];left=w.sonar_crop[0];h=len(w.raw_array)
  w.objects={'test':{'id':'test','label':'Teste de geometria','score':.9,'status':'pendente','model':'unit-fixture','channel':'ss_port','frame_channel':'both','frame_part_width':part,'frame_water':info['remove_water'],'frame_start':info['start'],'ping':info['start']+12,'box':[part-50,10,part-10,20]}}
  original=w.raw_array.copy();w.cascade_direction.setCurrentIndex(1);w.draw_objects()
  rect=w.sonar.overlay_items[0].rect();self.assertEqual((rect.x(),rect.y(),rect.width(),rect.height()),(part-50-left,h-20,40,10))
  cursor=w.sonar.overlay_items[-1].line();self.assertEqual(cursor.y1(),h-1-(w.frame_index-info['start']));self.assertEqual(cursor.x2(),w.sonar.image_item.sceneBoundingRect().width());np.testing.assert_array_equal(w.raw_array,original)
 def test_visible_png_keeps_displayed_direction_and_crop(self):
  from viewer_screen_export import visible_capture
  w=self.w;raw=w.raw_array.copy();w.cascade_direction.setCurrentIndex(1);w.png_overlay.setChecked(False);w.sonar_timer.stop();w.show_sonar();self.qt.processEvents()
  expected,meta=visible_capture(w,False);folder=Path(tempfile.mkdtemp(prefix='viewer44-',dir=ROOT/'work/tmp'));path=folder/'cascade.png'
  with patch.object(QFileDialog,'getSaveFileName',return_value=(str(path),'PNG')):w.export_sonar()
  self.assertTrue(w.worker.wait(30000));self.qt.processEvents()
  from PySide6.QtGui import QImage
  self.assertEqual(QImage(str(path)),expected);data=json.loads(Path(str(path)+'.json').read_text(encoding='utf-8'));self.assertTrue(data['screen_view']);self.assertFalse(data['native_samples']);np.testing.assert_array_equal(raw,w.raw_array)
 def test_project_saves_range_mode_and_direction(self):
  w=self.w;w.professional.range_mode.setCurrentIndex(1);w.cascade_direction.setCurrentIndex(1)
  folder=Path(tempfile.mkdtemp(prefix='project44-',dir=ROOT/'work/tmp'));path=folder/'project.json'
  with patch.object(QFileDialog,'getSaveFileName',return_value=(str(path),'JSON')):w.save_project()
  data=json.loads(path.read_text(encoding='utf-8'));self.assertEqual(data['player']['cascade_direction'],1);self.assertEqual(data['professional']['range_mode'],1)
  w.professional.range_mode.setCurrentIndex(0);w.pending_project=data;w.recording_loaded(Recording(data['dat'],data['project']))
  self.assertEqual(w.cascade_direction.currentIndex(),1);self.assertEqual(w.professional.range_mode.currentIndex(),1)

if __name__=='__main__':unittest.main(verbosity=2)
