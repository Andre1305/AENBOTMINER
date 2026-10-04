import unittest,tempfile,time
from pathlib import Path
import app
import numpy as np,pandas as pd
from PySide6.QtWidgets import QApplication
from recording import Recording,ROOT
from mosaic_depth import depth_selection,validate_limits
from triangle_renderer import triangle_warp
from skimage.transform import PiecewiseAffineTransform
from native_mosaic import install_stripe_renderer,rectify_project
from batch_ai import BatchSettings,analyze_window

class Release43Tests(unittest.TestCase):
 def test_depth_limits_and_missing_values(self):
  data=pd.DataFrame({'dep_m':[5,10,15,20,25,np.nan,0,-2]})
  np.testing.assert_array_equal(depth_selection(data,validate_limits(10,20)),[False,True,True,True,False,False,False,False])
  for limits in [(20,10),(10,10),(-1,5),(0,np.inf)]:
   with self.assertRaises(ValueError):validate_limits(*limits)
 def test_depth_mask_blocks_interpolation_across_excluded_ping(self):
  image=np.full((5,8),200,np.uint8);points=np.array([[0,0],[7,0],[0,4],[7,4]],float)
  transform=PiecewiseAffineTransform();transform.estimate(points,points)
  mask=np.array([True,True,False,True,True]);out=triangle_warp(image,transform.inverse,(5,8),valid_rows=mask)
  self.assertTrue((out[2]==0).all());self.assertTrue((out[1,1:-1]==200).all());self.assertTrue((out[3,1:-1]==200).all())
  transform.estimate(points,points*np.array([1,2]));out=triangle_warp(image,transform.inverse,(9,8),valid_rows=mask)
  self.assertTrue((out[3:6]==0).all());self.assertTrue((out[6,1:-1]==200).all())
 def test_real_recordings_keep_range_and_aspect(self):
  for dat,project in [('work/input/Rec00001.DAT','outputs/Rec00001-processado'),('work/input-rec00002/Rec00002.DAT','outputs/Rec00002-beta/metadados')]:
   rec=Recording(ROOT/dat,ROOT/project)
   try:
    frames=[rec.waterfall(i,'both',True,64) for i in [0,len(rec.data)//2,len(rec.data)-1]]
    self.assertEqual(len({a.shape for a,info in frames}),1)
    self.assertEqual(len({(info['width_m'],info['sampling_m'],info['along_track_m']) for a,info in frames}),1)
    self.assertTrue(all(a.any() for a,info in frames))
   finally:rec.close()
 def test_identification_choices_and_new_project_filter(self):
  qt=QApplication.instance() or QApplication([]);window=app.Studio()
  try:
   self.assertEqual(window.ai_mode.count(),3);self.assertEqual(window.batch_dialog.model.count(),3)
   self.assertFalse(any('Isolation' in window.ai_mode.itemText(i) for i in range(window.ai_mode.count())))
   window.professional.mosaic_depth_enabled.setChecked(True);window.professional.mosaic_depth_min.setValue(10);window.professional.mosaic_depth_max.setValue(20)
   self.assertEqual(window.professional.mosaic_depth_limits(),(10,20))
   state=window.professional.state();window.professional.mosaic_depth_enabled.setChecked(False);window.professional.restore(state)
   self.assertEqual(window.professional.mosaic_depth_limits(),(10,20))
   window.beta_dialog.depth_filter.setChecked(True);window.beta_dialog.depth_min.setValue(8);window.beta_dialog.depth_max.setValue(18)
   settings=window.beta_dialog.settings();self.assertEqual((settings.mosaic_depth_min,settings.mosaic_depth_max),(8,18));self.assertEqual(settings.ai_model,'sonar_ensemble')
  finally:window.close();qt.processEvents()
 def test_viewer_transform_stays_fixed_when_seeking_different_ranges(self):
  qt=QApplication.instance() or QApplication([]);window=app.Studio(autoload=False);window.resize(1280,800);window.show();qt.processEvents()
  window.recording_loaded(Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados'))
  try:
   matrices=[];extents=[]
   for index in [0,16000,31970]:
    window.set_index(index);window.sonar_timer.stop();window.show_sonar();qt.processEvents()
    matrix=window.sonar.transform();matrices.append((matrix.m11(),matrix.m22()))
    rect=window.sonar.image_item.sceneBoundingRect();extents.append((rect.width(),rect.height()))
   np.testing.assert_allclose(matrices,[matrices[0]]*3,rtol=1e-6)
   np.testing.assert_allclose(extents,[extents[0]]*3,rtol=1e-6)
  finally:window.sonar_timer.stop();window.close();qt.processEvents()
 def test_adapter_compiles(self):self.assertIsNotNone(install_stripe_renderer())

if __name__=='__main__':unittest.main(verbosity=2)
