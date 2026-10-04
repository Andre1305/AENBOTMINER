import time
import numpy as np
import unittest
import hashlib,json
from PySide6.QtCore import Qt
from test_viewer46 import Viewer46GuiTests

class HumViewer48Tests(Viewer46GuiTests):
 def test_movie_readonly_and_custom_palette(self):
  from sonar_movie import export_movie,movie_indices
  from image_processing import palette_rgb,ImageSettings
  palette='Personalizada:#000000,#ff0000,#00ff00,#0000ff,#ffffff'
  np.testing.assert_allclose(palette_rgb(np.array([0.,.5,1.]),palette),[[0,0,0],[0,1,0],[1,1,1]])
  np.testing.assert_array_equal(movie_indices([0.,1.,2.],0,2,reverse=True),movie_indices([0.,1.,2.],0,2)[::-1])
  before=hashlib.sha256(self.rec.dat.read_bytes()).hexdigest();target=self.folder/'test.avi';result=export_movie(self.rec.dat,self.rec.project,target,12000,12002,'both',False,64,ImageSettings(palette=palette))
  self.assertGreater(target.stat().st_size,0);self.assertEqual(before,hashlib.sha256(self.rec.dat.read_bytes()).hexdigest());self.assertGreater(result['frames'],0);self.assertTrue(json.loads((self.folder/'test.avi.json').read_text())['preview_video'])
 def test_three_channel_sync_and_layout(self):
  h=self.w.humviewer;h.update();h.layout.setCurrentIndex(3)
  self.wait(lambda:self.w.viewer_controller.frame.get('third') is not None and getattr(self.w.viewer_controller,'third_filtered',None) is not None)
  frame=self.w.viewer_controller.frame
  self.assertEqual(frame['index'],self.w.slider.value());self.assertNotEqual(frame['down'][1]['channel'],frame['third'][1]['channel']);self.assertTrue(h.third.isVisible())
  h.layout.setCurrentIndex(2);self.assertEqual(self.w.viewer_controller.sonar_split.orientation(),Qt.Orientation.Horizontal);self.assertFalse(h.third.isVisible())
 def test_reverse_transport_and_time_seek(self):
  h=self.w.humviewer;h.reverse.setChecked(True);self.w.speed.setCurrentIndex(5);self.w.toggle_playback();before=self.w.slider.value();self.w._play_last=time.monotonic()-.25;self.w.play_tick();self.w.pause_playback();self.assertLess(self.w.slider.value(),before)
  h.seconds.setValue(20);h.goto();expected=np.searchsorted(self.w.play_times,self.w.play_times[0]+20);self.assertEqual(self.w.slider.value(),expected)
 def test_multiclass_default_grid_and_state(self):
  self.assertEqual(self.w.ai_mode.currentIndex(),2);self.assertFalse(self.w.ai_mode.model().item(1).isEnabled());self.assertEqual(self.w.batch_dialog.model.currentIndex(),2)
  h=self.w.humviewer;h.grid.setChecked(True);h.reverse.setChecked(True);state=h.state();h.grid.setChecked(False);h.restore(state);self.assertTrue(self.w.sonar.grid_visible);self.assertTrue(h.reverse.isChecked())

def load_tests(loader,tests,pattern):
 return unittest.TestSuite(HumViewer48Tests(name) for name in ['test_three_channel_sync_and_layout','test_reverse_transport_and_time_seek','test_multiclass_default_grid_and_state','test_movie_readonly_and_custom_palette'])
