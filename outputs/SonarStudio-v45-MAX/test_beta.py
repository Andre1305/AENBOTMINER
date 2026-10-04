import unittest,time,tempfile,json
from pathlib import Path
from unittest.mock import patch
import app
from PySide6.QtWidgets import QApplication
from recording import ROOT,Recording,import_recording
from beta_workflow import WorkflowSettings,workflow_target,run_workflow
DAT=ROOT/'work/input-rec00002/Rec00002.DAT';PROJECT=ROOT/'outputs/Rec00002-beta/metadados'
class BetaTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.qt=QApplication.instance() or QApplication([])
 def test_other_dat_all_channels_and_ends(self):
  rec=Recording(DAT,PROJECT)
  try:
   self.assertEqual(len(rec.channels),4);self.assertEqual(len(rec.data),31971)
   for channel in ['both']+list(rec.channels):
    for index in [0,len(rec.data)//2,len(rec.data)-1]:
     a,info=rec.waterfall(index,channel,channel=='both',64);self.assertEqual(len(a),64);self.assertGreater(a.max(),0);self.assertLessEqual(info['end'],len(rec.data))
  finally:rec.close()
 def test_import_creates_nested_parent(self):
  target=ROOT/'work/tmp'/('beta-import-'+str(time.time_ns()))/'parent'/'project'
  def decode(*args):
   self.assertTrue(target.parent.is_dir());target.mkdir();(target/'source.json').write_text('{}')
  with patch('pingverter.hum2pingmapper',side_effect=decode),patch('recording.Recording') as constructor:
   import_recording(DAT,target);constructor.assert_called_once()
 def test_switch_recording_removes_previous_products(self):
  w=app.Studio(autoload=False)
  try:
   w.recording_loaded(Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado'))
   self.assertIsNotNone(w.map.dataset)
   w.recording_loaded(Recording(DAT,PROJECT))
   self.assertIsNone(w.map.dataset);self.assertIsNone(w.last_mosaic);self.assertIsNone(w.bathy_result);self.assertFalse(w.objects);self.assertEqual(w.slider.maximum(),31970);self.assertEqual(w.batch_dialog.last.value(),31971)
   for channel in range(w.channel.count()):w.channel.setCurrentIndex(channel);w.show_sonar();self.assertIsNotNone(w.raw_array)
  finally:
   w.sonar_timer.stop();w.close();self.qt.processEvents()
 def test_workflow_identity_cancel_and_preview_only(self):
  folder=Path(tempfile.mkdtemp(prefix='beta-workflow-',dir=ROOT/'work/tmp'));settings=WorkflowSettings(mosaic=False,bathymetry=False,ai=False,exports=False)
  result=run_workflow(DAT,folder,settings,project=PROJECT)
  self.assertTrue(result['success']);self.assertEqual(result['pings'],31971);self.assertTrue((folder/'sonar/both.png').is_file())
  self.assertNotEqual(workflow_target(DAT,settings),workflow_target(ROOT/'work/input/Rec00001.DAT',settings))
  with self.assertRaises(InterruptedError):run_workflow(DAT,folder,settings,cancel=lambda:True,project=PROJECT)
  self.assertEqual(json.loads((folder/'projeto-beta.json').read_text())['status'],'cancelled')
 def test_beta_controls_default_complete_pipeline(self):
  w=app.Studio(autoload=False)
  try:
   dialog=w.beta_dialog;dialog.dat.setText(str(DAT));settings=dialog.settings();self.assertTrue(settings.mosaic and settings.bathymetry and settings.ai and settings.exports);self.assertIsNone(settings.mosaic_resolution);self.assertEqual(settings.ai_last_ping,-1)
  finally:w.close();self.qt.processEvents()
if __name__=='__main__':unittest.main(verbosity=2)
