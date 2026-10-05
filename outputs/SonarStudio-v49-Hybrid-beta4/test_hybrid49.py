import unittest,time,hashlib
from unittest.mock import patch
import numpy as np
from PySide6.QtWidgets import QApplication
from image_processing import ImageSettings,PALETTES,palette_rgb,colorize
from native_engine import ENGINE,NativeEngine
from viewer_gpu import ColorShader
from test_viewer46 import Viewer46GuiTests

class Hybrid49MathTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.qt=QApplication.instance() or QApplication([]);ENGINE.start();ENGINE.thread.join(30)
  if not ENGINE.ready:raise AssertionError(ENGINE.status())
 def test_compiled_gather_bounds_padding_and_readonly_source(self):
  a=np.arange(64,dtype=np.uint8);a.flags.writeable=False;offsets=np.array([0,30,64]);counts=np.array([4,6,0]);result=ENGINE.gather(a,offsets,counts,5)
  np.testing.assert_array_equal(result,[[0,1,2,3,0],[30,31,32,33,34],[0,0,0,0,0]]);self.assertFalse(a.flags.writeable)
  with self.assertRaises(ValueError):ENGINE.gather(a,[63],[2],4)
  with self.assertRaises(ValueError):ENGINE.gather(a,[-1],[2],4)
 def test_slant_exact_numpy_reference_and_invalid_depth(self):
  a=np.random.default_rng(49).integers(0,256,(16,512),dtype=np.uint8);depth=np.linspace(1,30,16);pixels=np.full(16,.12);ref=np.zeros_like(a);ground=np.arange(512)*.12
  for row in range(16):ref[row]=np.interp(np.sqrt(ground*ground+depth[row]**2)/pixels[row],np.arange(512),a[row],left=0,right=0).astype(np.uint8)
  np.testing.assert_array_equal(ENGINE.slant(a,depth,pixels,.12),ref)
  depths=depth.copy();depths[1]=np.nan;self.assertFalse(ENGINE.slant(a,depths,pixels,.12)[1].any())
 def test_all_palette_bytes_match_legacy_and_cpu_pointwise_fallback(self):
  a=np.arange(256,dtype=np.uint8)[None,:].repeat(3,axis=0);shader=ColorShader(legacy=True);shader.compiled=True
  for palette in PALETTES+['Personalizada:#000000,#112233,#334455,#778899,#ffffff']:
   opts=ImageSettings(palette=palette);image=shader.render(a,a,opts);rgba=np.frombuffer(image.bits(),np.uint8).reshape(image.height(),image.bytesPerLine())[:,:image.width()*4].reshape(image.height(),image.width(),4);np.testing.assert_array_equal(rgba,colorize(a,opts));self.assertIn('LLVM',shader.last_backend)
   opts=ImageSettings(palette=palette,gamma=1.5,black=10,white=230);image=shader.render(a,a,opts,quantize=True);rgba=np.frombuffer(image.bits(),np.uint8).reshape(image.height(),image.bytesPerLine())[:,:image.width()*4].reshape(image.height(),image.width(),4);np.testing.assert_array_equal(rgba,colorize(a,opts))
  shader.close()
 def test_not_ready_falls_back_without_compiling_in_paint(self):
  engine=NativeEngine();self.assertIsNone(engine.gather(np.arange(8,dtype=np.uint8),[0],[8],8));self.assertFalse(engine.started)
  shader=ColorShader(legacy=True);shader.compiled=True
  with patch.object(ENGINE,'ready',False):
   image=shader.render(np.zeros((8,8),np.uint8),np.zeros((8,8),np.uint8),ImageSettings());self.assertEqual(image.width(),8);self.assertEqual(shader.last_backend,'CPU legado')
  shader.close()
 def test_native_kernels_release_gil_and_disable_fastmath(self):
  for name in ['gather_rows','slant_rows','palette_rows']:
   kernel=getattr(ENGINE.module,name);self.assertTrue(kernel.nopython_signatures);self.assertTrue(kernel.targetoptions['nogil']);self.assertFalse(kernel.targetoptions.get('fastmath',False))
 def test_pointwise_comparison_keeps_original_levels_and_runtime_error_code(self):
  from dataclasses import replace
  from runtime_diagnostics import child_environment,exit_explanation
  a=np.arange(7,dtype=np.uint8)[None,:]*35;opts=ImageSettings(black=40,white=190,gamma=1.7);shader=ColorShader(legacy=True);image=shader.render(a,a,opts,True,.5,quantize=True);out=np.frombuffer(image.bits(),np.uint8).reshape(1,image.bytesPerLine())[:,:28].reshape(1,7,4);reference=colorize(a,opts);reference[:,:4]=colorize(a[:,:4],replace(opts,black=0.,white=255.));np.testing.assert_array_equal(out,reference);shader.close()
  self.assertIn('DLL',exit_explanation(0xc06d007f));self.assertIn('não presumir',exit_explanation(0xc06d007f));self.assertEqual(child_environment({'Path':'a','PATH':'b'})['PATH'],'b')

class Hybrid49GuiTests(Viewer46GuiTests):
 def test_cursor_toggle_shortcut_preserves_samples_and_state(self):
  c=self.w.viewer_controller;before=self.w.raw_array.copy();c.draw_overlays();self.assertTrue(c.show_cursor.isChecked());visible=len(self.w.sonar.overlay_items)
  c.cursor_action.trigger();self.wait(lambda:len(self.w.sonar.overlay_items)==visible-1);self.assertFalse(c.show_cursor.isChecked());self.assertEqual(len(self.w.sonar.overlay_items),visible-1);np.testing.assert_array_equal(before,self.w.raw_array)
  state=c.state();c.cursor_action.trigger();c.restore(state);self.assertFalse(c.show_cursor.isChecked());self.assertEqual(c.cursor_action.shortcut().toString(),'C')
 def test_waypoints_vessel_target_and_gpx(self):
  from viewer_metrology import point_from_pixel
  from viewer_waypoints import export_gpx
  import xml.etree.ElementTree as ET
  c=self.w.viewer_controller;info=c.frame['info'];point=point_from_pixel(self.rec,info,info['part_width']+4000,80,c.altitude_override)
  c.waypoints.save('Barco & GPS','vessel_gps');c.waypoints.save('Alvo estimado','target_estimated',point)
  rows=[r for r in c.findings.list(self.rec.dat) if r['metadata'].get('waypoint')];self.assertEqual(len(rows),2)
  vessel=next(r for r in rows if r['metadata']['waypoint_kind']=='vessel_gps');target=next(r for r in rows if r['metadata']['waypoint_kind']=='target_estimated')
  self.assertEqual(vessel['metadata']['longitude'],float(self.rec.data.iloc[self.w.slider.value()].lon));self.assertIsNone(vessel['metadata']['target_point']);self.assertEqual(target['metadata']['east'],point['east']);self.assertNotEqual(target['metadata']['coordinate_reference'],'vessel navigation, not object position')
  path=self.rec.project/'waypoints-test.gpx';export_gpx(rows,path);nodes=ET.parse(path).getroot().findall('{http://www.topografix.com/GPX/1/1}wpt');self.assertEqual(len(nodes),2);self.assertTrue(all(-90<=float(n.attrib['lat'])<=90 for n in nodes))
  c.waypoints.arm();self.assertEqual(self.w.sonar.tool_mode,'Waypoint')
 def test_visible_export_composes_selected_panels_and_optional_overlay(self):
  from viewer_screen_export import visible_capture
  c=self.w.viewer_controller;h=self.w.humviewer;h.layout.setCurrentIndex(0);self.wait(lambda:c.last_painted_token==c.token and not c.down.isVisible());im,meta=visible_capture(self.w,False);self.assertEqual(meta['panels'],1);self.assertEqual(im.height(),self.w.sonar.viewport().grab().height());self.assertEqual(meta['speed_kmh'],float(self.rec.data.iloc[self.w.frame_index].speed_ms)*3.6)
  h.layout.setCurrentIndex(1);self.wait(lambda:c.last_painted_token==c.token and c.down_filtered is not None and c.down.isVisible());self.qt.processEvents();clean,meta=visible_capture(self.w,False);over,legend=visible_capture(self.w,True);self.assertEqual(meta['panels'],2);self.assertEqual(clean.width(),self.w.sonar.viewport().grab().width()+c.down.viewport().grab().width());self.assertEqual(clean.size(),over.size());self.assertNotEqual(clean,over);self.assertTrue(legend['overlay']);self.assertFalse(meta['native_samples'])
  token=c.last_painted_token;c.last_painted_token=-1
  try:
   with self.assertRaises(ValueError):visible_capture(self.w)
  finally:c.last_painted_token=token
 def test_analysis_scale_retains_zoom_on_navigation_and_resize(self):
  c=self.w.viewer_controller;h=self.w.humviewer;h.layout.setCurrentIndex(1);self.wait(lambda:c.last_painted_token==c.token and c.down_filtered is not None);self.assertEqual(c.sonar_split.orientation().name,'Horizontal');self.assertTrue(h.stable.isChecked());self.assertFalse(self.w.sonar.auto_fit)
  transforms=[(v.transform().m11(),v.transform().m22()) for v in [self.w.sonar,c.down]];self.w.set_index(12300);self.wait(lambda:c.last_painted_token==c.token);self.w.resize(1450,950);self.qt.processEvents()
  self.assertEqual(transforms,[(v.transform().m11(),v.transform().m22()) for v in [self.w.sonar,c.down]]);self.assertTrue(h.state()['analysis_scale']);h.fit_views();self.assertFalse(c.down.auto_fit);self.assertFalse(self.w.sonar.auto_fit)
 def test_backend_switch_preserves_samples_and_persistent_mode(self):
  c=self.w.viewer_controller;before=self.w.raw_array.copy();h=c.hybrid;h.mode.setCurrentIndex(0);self.wait(lambda:c.last_painted_token==c.token and c.stream is not None);np.testing.assert_array_equal(before,self.w.raw_array)
  h.mode.setCurrentIndex(1);self.wait(lambda:c.last_painted_token==c.token and c.stream is not None);np.testing.assert_array_equal(before,self.w.raw_array);self.assertEqual(self.rec.processing_backend,'compiled');state=c.state();h.mode.setCurrentIndex(0);c.restore(state);self.assertEqual(h.mode.currentIndex(),1)
 def test_live_gpu_controls_and_freeze_retain_native_filter_path(self):
  c=self.w.viewer_controller;c.hybrid.mode.setCurrentIndex(2);self.wait(lambda:c.filtered and c.filtered.get('live_color') and c.frame['token']==c.token)
  before=c.filtered;self.w.gamma.setValue(170);self.wait(lambda:c.options().gamma==1.7);self.qt.processEvents();self.assertIs(c.filtered,before)
  c.freeze.setChecked(True);self.wait(lambda:c.filtered and not c.filtered.get('live_color',False) and not c.filtered['preview'] and c.frame['token']==c.token);self.assertIn('legacy_pipeline',c.filtered['filter_ms'])
 def test_compiled_rec_zero_copy_ping_readonly_and_memory_limit(self):
  channel=next(iter(self.rec.channels));raw,_=self.rec.raw_ping(channel,12000);self.assertTrue(np.shares_memory(raw,self.rec.maps[channel]));self.assertFalse(raw.flags.writeable)
  with self.assertRaises(ValueError):raw[0]=1
  before=hashlib.sha256(self.rec.dat.read_bytes()).hexdigest();self.rec.set_backend('compiled');self.rec.waterfall(12000,'both',True,3000);self.assertLessEqual(self.rec.cache_bytes,64*1024**2);self.assertEqual(before,hashlib.sha256(self.rec.dat.read_bytes()).hexdigest())
 def test_forced_legacy_does_not_promote_on_saved_project(self):
  import os,app
  with patch.dict(os.environ,{'SONARSTUDIO_FORCE_LEGACY':'1'}):window=app.Studio(autoload=False)
  try:
   h=window.viewer_controller.hybrid;h.restore({'processing_mode':2});self.assertEqual(h.mode.currentIndex(),0);self.assertFalse(h.mode.model().item(1).isEnabled());self.assertFalse(window.viewer_controller.shader.compiled)
  finally:window.close();self.qt.processEvents()

def load_tests(loader,tests,pattern):
 suite=loader.loadTestsFromTestCase(Hybrid49MathTests)
 for name in ['test_analysis_scale_retains_zoom_on_navigation_and_resize','test_visible_export_composes_selected_panels_and_optional_overlay','test_waypoints_vessel_target_and_gpx','test_cursor_toggle_shortcut_preserves_samples_and_state','test_backend_switch_preserves_samples_and_persistent_mode','test_live_gpu_controls_and_freeze_retain_native_filter_path','test_compiled_rec_zero_copy_ping_readonly_and_memory_limit','test_forced_legacy_does_not_promote_on_saved_project']:suite.addTest(Hybrid49GuiTests(name))
 return suite

