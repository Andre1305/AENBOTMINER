import unittest,time,json
from pathlib import Path
from unittest.mock import patch
import app
import numpy as np
from PySide6.QtWidgets import QApplication
from recording import ROOT,Recording,import_recording
from sonar_ai import decode_yolo,detect_yolo,locate,session
from object_review import export_objects
from mosaic_blend import recombine

class VideoAITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt=QApplication.instance() or QApplication([]);cls.folder=ROOT/'work/sonar-studio-tests/video-ai';cls.folder.mkdir(parents=True,exist_ok=True)
    def setUp(self):
        self.w=app.Studio(autoload=False);self.w.recording_loaded(Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado'));self.w.professional.pings.setValue(80)
    def tearDown(self):
        self.w.pause_playback()
        for worker in [self.w.sonar_worker,self.w.ai_worker]:
            if worker and worker.isRunning():worker.wait(30000);self.qt.processEvents()
        self.w.close();self.qt.processEvents()
    def test_play_pause_stop_speed_and_slow_fraction(self):
        self.w.speed.setCurrentIndex(self.w.speed.findData(.25));self.w.toggle_playback()
        for _ in range(10):self.w._play_last=time.monotonic()-.15;self.w.play_tick()
        self.assertGreater(self.w.slider.value(),0);position=self.w.slider.value();self.w.pause_playback();self.assertFalse(self.w.play_timer.isActive());self.assertEqual(position,self.w.slider.value())
        self.w.speed.setCurrentIndex(self.w.speed.findData(32));self.w.toggle_playback();self.w._play_last=time.monotonic()-.15;self.w.play_tick();self.assertGreater(self.w.slider.value(),position)
        self.w.stop_playback();self.assertEqual(self.w.slider.value(),0)
    def test_end_stops_and_seek_remains_usable(self):
        self.w.set_index(self.w.slider.maximum()-1);self.w.toggle_playback();self.w._play_last=time.monotonic()-.3;self.w.play_tick();self.assertFalse(self.w.play_timer.isActive());self.assertEqual(self.w.slider.value(),self.w.slider.maximum())
        self.w.set_index(12000);self.assertAlmostEqual(self.w._play_time,self.w.recording.data.iloc[12000].time_s)
    def test_row_cache_preserves_original_and_is_bounded(self):
        rec=self.w.recording
        a=rec.read_rows('ss_port',512,600,5000)
        raw,row=rec.raw_ping('ss_port',512)
        np.testing.assert_array_equal(a[0,:min(len(raw),5000)],raw[:5000])
        for first in range(0,12000,256):rec.read_rows('ss_port',first,first+128,5000)
        self.assertLessEqual(rec.cache_bytes,64*1024**2)
    def test_dat_cached_import_does_not_reprocess_sonar(self):
        with patch('pingverter.hum2pingmapper',side_effect=AssertionError('Reprocessamento indevido')):
            rec=import_recording(self.w.recording.dat,self.w.recording.project);rec.close()
    def test_yolo_decode_nms_and_real_model_inference(self):
        output=np.array([[[100,101],[100,101],[40,40],[40,40],[.9,.8]]],dtype=np.float32)
        boxes=decode_yolo(output);self.assertEqual(len(boxes),1);self.assertAlmostEqual(boxes[0]['score'],.9,places=5)
        raw,_=self.w.recording.waterfall(12000,'ss_port',True,count=64)
        boxes,meta=detect_yolo(raw)
        self.assertEqual(meta['class'],'Crab-Pot');self.assertTrue(all(0<=b['score']<=1 for b in boxes));self.assertEqual(session().get_inputs()[0].shape,[1,3,640,640])
    def test_generic_anomaly_detector_removed(self):
        import sonar_ai
        self.assertFalse(hasattr(sonar_ai,'detect_anomalies'));self.assertFalse(hasattr(sonar_ai,'anomaly_map'))
    def test_object_projection_and_review_export(self):
        _,info=self.w.recording.waterfall(12000,'both',False,count=80)
        items=locate([{'box':[10,20,40,40],'score':.8,'label':'Candidato a covo','model':'teste','status':'pendente'}],self.w.recording,info)
        self.assertTrue(items[0]['georeference'].startswith('aproximada'))
        self.assertFalse(items[0]['in_water_column']);self.assertTrue(-180<items[0]['longitude']<180)
        for ext in ['geojson','gpx','kml','shp']:
            count=export_objects(items,self.folder/('objects-'+str(time.time_ns())+'.'+ext));self.assertEqual(count,1)
        items[0]['status']='rejeitado';self.assertEqual(export_objects(items,self.folder/'rejected.geojson'),0)
        prior=dict(items[0],status='pendente',label='Objeto nomeado por mim',manual_label=True,score=.4)
        self.w.objects={prior['id']:prior}
        candidate=dict(prior,label='Candidato automático',manual_label=False,score=.9)
        with patch.object(self.w,'save_objects'):
            self.w.ai_ready(([candidate],{'model':'teste'},self.w.ai_epoch))
        self.assertEqual(self.w.objects[prior['id']]['label'],'Objeto nomeado por mim')
        self.assertTrue(self.w.objects[prior['id']]['manual_label'])
    def test_ai_merge_selects_recorded_pixels_without_synthesis(self):
        prepared=ROOT/'work/sonar-studio-tests/pro-v2/blend-source'
        fake=[{'box':[0,0,32,32],'score':.9,'label':'Candidato a covo'}]
        calls=[([{'box':[0,0,32,32],'score':.5}],{}),(fake,{})]
        with patch('sonar_ai.detect_yolo',side_effect=calls):result=recombine(prepared,self.folder/'ai-blend','ai_yolo')
        import rasterio
        with rasterio.open(result['mosaic']) as ds:
            a=ds.read(1);self.assertTrue((a[:5]==100).all());self.assertTrue((a[5:]==200).all())

    def test_viewport_fit_zoom_preserved_and_ai_button_real_worker(self):
        self.w.show();self.w.set_index(12000);self.w.show_sonar();self.qt.processEvents()
        self.assertTrue(self.w.sonar.fill_view)
        self.w.sonar.native_zoom();self.w.set_index(12010);self.w.show_sonar()
        self.assertFalse(self.w.sonar.auto_fit);self.assertEqual(self.w.sonar.transform().m11(),1.)
        self.w.ai_mode.setCurrentIndex(1);self.w.recording.project=self.folder
        errors=[];self.w.log.textChanged.connect(lambda:errors.append(self.w.log.toPlainText()))
        self.w.analyze_frame();self.assertIsNotNone(self.w.ai_worker)
        deadline=time.monotonic()+30
        while self.w.ai_worker.isRunning() and time.monotonic()<deadline:self.qt.processEvents();time.sleep(.01)
        self.qt.processEvents();self.assertFalse(self.w.ai_worker.isRunning())
        self.assertTrue((self.folder/'objects-ai.json').is_file())
        self.assertTrue(all('Traceback' not in e for e in errors))

if __name__=='__main__':unittest.main(verbosity=2)
