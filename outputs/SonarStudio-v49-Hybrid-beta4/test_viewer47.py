import unittest,math,hashlib,time,json
from pathlib import Path
from unittest.mock import patch
import app,numpy as np,pandas as pd
from image_processing import ImageSettings,enhance,colorize,PALETTES
from viewer_filters import structural,colors
from viewer_signature import relative_db,native_profile
from viewer_inference import roi_identity,inference_key
from test_viewer46 import Viewer46GuiTests as GuiBase

class Analysis47MathTests(unittest.TestCase):
 def test_legacy_one_pass_all_filters_and_palettes_exact(self):
  raw=np.random.default_rng(47).integers(0,256,(64,512),dtype=np.uint8);saved=raw.copy()
  for method in ['Original','Mediana','Lee — speckle','Wavelet','Non-local means']:
   for palette in PALETTES:
    opts=ImageSettings(method=method,destripe=.2,clahe=.25,sharpen=.3,black=10,white=230,gamma=1.5,palette=palette);result=structural(raw,opts);expected=enhance(raw,opts)
    np.testing.assert_array_equal(result['treated'],expected)
    np.testing.assert_array_equal(colors(result['treated'],ImageSettings(palette=palette)),colorize(raw,opts))
  np.testing.assert_array_equal(raw,saved)
 def test_downscan_keeps_legacy_destripe_axis(self):
  raw=np.arange(64*512,dtype=np.uint8).reshape(64,512);opts=ImageSettings(destripe=.2,clahe=.2,black=12,gamma=1.7)
  np.testing.assert_array_equal(structural(raw,opts,downscan=True)['treated'],enhance(raw.T,opts).T)
 def test_db_zero_undefined_and_not_calibrated(self):
  self.assertIsNone(relative_db(0));self.assertEqual(relative_db(255),0);self.assertAlmostEqual(relative_db(127.5),-6.020599913)
 def test_content_and_navigation_change_cache_even_same_dimensions(self):
  raw=np.zeros((2,4),np.uint8);rows=pd.DataFrame({'time_s':[0.,1.],'inst_dep_m':[5.,5.],'e':[1.,2.]});base=roi_identity(raw,rows);raw[0,0]=1
  self.assertNotEqual(base,roi_identity(raw,rows));raw[0,0]=0;rows.loc[0,'inst_dep_m']=6;self.assertNotEqual(base,roi_identity(raw,rows))
 def test_timeline_cursor_does_not_rebuild_static_path(self):
  from PySide6.QtWidgets import QApplication
  from viewer_timeline import IntensityTimeline
  qt=QApplication.instance() or QApplication([]);timeline=IntensityTimeline();timeline.resize(900,55);timeline.set_data(np.arange(2000),[[i,i%255] for i in range(1500)]);timeline.show();qt.processEvents();path=timeline.profile_path;timeline.index=600;timeline.update();qt.processEvents();self.assertIs(path,timeline.profile_path);timeline.close()

class Analysis47GuiTests(unittest.TestCase):
 setUpClass=classmethod(GuiBase.setUpClass.__func__);setUp=GuiBase.setUp;tearDown=GuiBase.tearDown;wait=GuiBase.wait
 def test_loupe_native_dn_not_affected_by_palette_filter_or_removed_water(self):
  c=self.w.viewer_controller;info=c.frame['info'];x=info['part_width']+1500;y=12000-info['start'];a=native_profile(self.rec,info,x,y)
  self.w.professional.black.setValue(30);self.w.gamma.setValue(240);self.w.palette.setCurrentText('Cinza invertido');b=native_profile(self.rec,info,x,y);self.assertEqual(a,b);self.assertFalse(a['calibrated'])
  ground=math.sqrt(a['slant_m']**2-float(self.rec.data.iloc[12000].inst_dep_m)**2);cinfo=dict(info,remove_water=True);g=native_profile(self.rec,cinfo,info['part_width']+ground/info['sampling_m'],y);self.assertEqual(a['sample'],g['sample']);self.assertEqual(a['dn'],g['dn'])
 def test_loupe_asynchronous_and_legacy_mode(self):
  from PySide6.QtCore import QPointF
  self.w.viewer_controller.hybrid.mode.setCurrentIndex(0);self.wait(lambda:self.w.viewer_controller.last_painted_token==self.w.viewer_controller.token)
  c=self.w.viewer_controller;c.loupe.setChecked(True);f=c.filtered;left=self.w.sonar_crop[0] if self.w.sonar_crop else 0;x=c.frame['info']['part_width']+1500;y=12000-c.frame['info']['start'];c.hover_signature(QPointF((x-left)/f['scale_x'],y/f['scale_y']));self.wait(lambda:c.signature.profile is not None);self.assertEqual(c.signature.profile['ping'],12000);self.assertEqual(c.shader.renderer,'Legacy deterministic CPU palette');self.assertEqual(c.shader.last_backend,'CPU legado')
 def test_manual_ai_accepts_stopped_window_but_rejects_stale_generation(self):
  c=self.w.viewer_controller;self.w.ai_mode.setCurrentIndex(2);c.nav.clear();c.analyze(True);self.wait(lambda:self.w.ai_worker is not None and not self.w.ai_worker.isRunning(),40);self.wait(lambda:bool(c.observations))
  observation=next(iter(c.observations));payload=c.cache.get(observation);c.nav.append((time.monotonic()-.1,11980));c.nav.append((time.monotonic(),12000));self.assertGreater(c.navigation_rate(),10)
  before=c.discarded_ai;c.accept_ai(payload,self.w.ai_epoch,c.identity,payload['window_start'],payload['window_end'],payload['window_center'],True,True,c.token);self.assertEqual(c.discarded_ai,before)
  c.accept_ai(payload,self.w.ai_epoch,c.identity,payload['window_start'],payload['window_end'],payload['window_center'],True,True,c.token-1);self.assertEqual(c.discarded_ai,before+1)
 def test_private_target_card_native_photo_and_pdf_preview(self):
  from target_evidence import export_pdf
  from private_pdf import read_secret,key_path
  from PySide6.QtCore import QPointF
  c=self.w.viewer_controller;f=c.filtered;left=self.w.sonar_crop[0] if self.w.sonar_crop else 0;c.hover_signature(QPointF((c.frame['info']['part_width']+1500-left)/f['scale_x'],(12000-c.frame['info']['start'])/f['scale_y']));c.quick_tag('Alvo — exemplo não validado');item=c.findings.list(self.rec.dat)[0];self.assertTrue(Path(item['metadata']['snapshot_path']).is_file());target=self.folder/'target.pdf';result=export_pdf(self.rec,item,target)
  self.assertTrue(result['encrypted']);self.assertEqual(result['pages'],1);self.assertEqual(result['native_pixels'],[513,65]);self.assertNotIn(b'/Title (Ficha',target.read_bytes());self.assertEqual(item['metadata']['target_point']['ping'],12000)
  import subprocess
  from private_pdf import pdf_python
  check=subprocess.run([str(pdf_python()),str(Path(__file__).parent/'card_preview.py')],input=json.dumps({'file':str(target),'key':str(key_path())}).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW)
  self.assertEqual(check.returncode,0,check.stderr.decode(errors='replace'));details=json.loads(check.stdout);self.assertEqual(details['pages'],1);self.assertTrue(details['wrong_password_rejected']);self.assertIn('Não medida',details['text'][0]);self.assertIn('eco-nativo.png',details['attachments']);self.assertIn('metadados.json',details['attachments'])
  # Keep one encrypted example in the existing personal documentation folder.
  destination=key_path().parent/'Ficha-Exemplo-SonarStudio-4.7.pdf';destination.write_bytes(target.read_bytes())

del GuiBase
if __name__=='__main__':unittest.main()
