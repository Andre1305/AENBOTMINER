import unittest,tempfile,json
from pathlib import Path
from unittest.mock import patch
import app
import numpy as np
from recording import ROOT,Recording
from image_processing import PALETTES,ImageSettings,colorize,palette_rgb
from beta_workflow import WorkflowSettings,run_workflow

class Release42Tests(unittest.TestCase):
 def test_palettes_are_monotonic_preserve_source_and_alpha(self):
  a=np.arange(256,dtype=np.uint8)[None,:];original=a.copy();mask=a>10
  for palette in PALETTES:
   rgba=colorize(a,ImageSettings(palette=palette),mask);self.assertEqual(rgba.shape,(1,256,4));self.assertTrue(np.array_equal(rgba[...,3],mask*255));self.assertTrue(np.array_equal(a,original))
   luminance=rgba[0,:,:3]@np.array([.2126,.7152,.0722]);diff=np.diff(luminance[11:])
   self.assertTrue((diff<=.01).all() if palette=='Cinza invertido' else (diff>=-.01).all())
  with self.assertRaises(ValueError):palette_rgb(a/255,'invalid')
 def test_shorter_channel_still_reads_end_of_recording(self):
  rec=Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados')
  try:
   star=next(k for k in rec.channels if k.startswith('ss_star'));rec.channels[star]=rec.channels[star].iloc[:1000].copy()
   array,info=rec.waterfall(len(rec.data)-1,'both',False,64);self.assertEqual(len(array),64);self.assertGreater(array.shape[1],0);self.assertGreater(array.max(),0)
  finally:rec.close()
 def test_partial_metadata_import_is_retried(self):
  from recording import import_recording
  folder=Path(tempfile.mkdtemp(prefix='partial-import-',dir=ROOT/'work/tmp'));(folder/'meta').mkdir();(folder/'meta/DAT_meta.csv').write_text('incomplete')
  with patch('pingverter.hum2pingmapper') as decoder,patch('recording.Recording'):
   import_recording(ROOT/'work/input-rec00002/Rec00002.DAT',folder);decoder.assert_called_once()
 def test_workflow_export_retry_preserves_files_and_recovers_partial(self):
  dat=ROOT/'work/input-rec00002/Rec00002.DAT';project=ROOT/'outputs/Rec00002-beta/metadados'
  def make(*args):
   rec=Recording(dat,project);rec.data=rec.data.iloc[12000:12024].reset_index(drop=True).copy();rec.channels[rec.primary]=rec.data;rec.xy=rec.data[['e','n']].to_numpy();return rec
  folder=Path(tempfile.mkdtemp(prefix='exports-retry-',dir=ROOT/'work/tmp'));settings=WorkflowSettings(mosaic=False,bathymetry=False,ai=False,exports=True)
  with patch('beta_workflow.import_recording',side_effect=make):
   first=run_workflow(dat,folder,settings);exports=Path(first['products']['exports']);shp=exports/'levantamento.shp';stamp=shp.stat().st_mtime_ns
   second=run_workflow(dat,folder,settings);self.assertTrue(second['success']);self.assertEqual(second['products']['exports'],first['products']['exports']);self.assertEqual(shp.stat().st_mtime_ns,stamp)
   shp.with_suffix('.shx').unlink()
   third=run_workflow(dat,folder,settings);self.assertTrue(third['success']);self.assertNotEqual(third['products']['exports'],first['products']['exports']);self.assertEqual(shp.stat().st_mtime_ns,stamp)

if __name__=='__main__':unittest.main(verbosity=2)
