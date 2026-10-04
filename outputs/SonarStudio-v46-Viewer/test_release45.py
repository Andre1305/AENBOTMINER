import unittest,tempfile,json,time,shutil
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
import app
import numpy as np
import rasterio
from PIL import Image
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication
from recording import Recording,ROOT
from active_learning import candidate_crop,save_feedback,training_class
from sonar_triage import triage_regions
from ai_heatmap import points,score_grid,export_heatmap
from batch_ai import BatchSettings,analyze_window,run_batch,fingerprint,tile_count

class Release45Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt=QApplication.instance() or QApplication([])
    def setUp(self):
        self.folder=Path(tempfile.mkdtemp(prefix='sonar45-',dir=ROOT/'work/tmp'))
        self.rec=Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados')
    def tearDown(self):self.rec.close()
    def candidate(self):
        a,info=self.rec.waterfall(12000,'both',False,64)
        return dict(id='feedback45',label='Candidato a covo',score=.8,model='GhostVision YOLO12',status='confirmado',
                    ping=12000,channel='ss_port',box=[100,8,180,24],frame_start=info['start'],frame_channel='both',
                    frame_water=False,frame_part_width=info['part_width'],source_dat=str(self.rec.dat),east=330000.,north=7300000.)
    def test_feedback_original_pixels_and_labels(self):
        item=self.candidate();crop,geometry=candidate_crop(self.rec,item)
        a,info=self.rec.waterfall(12000,'both',False,64)
        x,y=geometry['crop_origin'];np.testing.assert_array_equal(crop,a[y:y+len(crop),x:x+crop.shape[1]])
        out=save_feedback(self.rec,item,self.folder/'dataset/treino');record=json.loads(Path(out['record']).read_text(encoding='utf-8'))
        np.testing.assert_array_equal(np.asarray(Image.open(Path(out['folder'])/record['image'])),crop)
        values=(Path(out['folder'])/record['proposed_label']).read_text().split();self.assertEqual(values[0],'0')
        self.assertTrue(all(0<float(v)<=1 for v in values[1:]));self.assertFalse(record['training_ready'])
    def test_rejected_feedback_is_not_background_and_keeps_history(self):
        item=self.candidate();first=save_feedback(self.rec,item,self.folder)
        second=save_feedback(self.rec,dict(item,status='rejeitado'),self.folder)
        record=json.loads(Path(second['record']).read_text(encoding='utf-8'))
        self.assertNotIn('proposed_label',record);self.assertFalse(record['rejection_is_background'])
        self.assertTrue(Path(first['record']).exists());latest=json.loads((Path(second['folder'])/'latest.json').read_text());self.assertEqual(latest['decision'],'rejeitado')
    def test_crop_wider_original_viewer_keeps_nadir_alignment(self):
        item=self.candidate();a,info=self.rec.waterfall(12000,'both',False,64)
        extra=500;item['frame_part_width']=info['part_width']+extra;item['box']=[100+extra,8,180+extra,24]
        crop,geometry=candidate_crop(self.rec,item);np.testing.assert_array_equal(crop,a[0:32,84:196])
    def test_crop_single_sides_and_bad_metadata(self):
        item=self.candidate()
        for channel in ['ss_port','ss_star']:
            a,info=self.rec.waterfall(12000,channel,True,64)
            obj=dict(item,frame_channel=channel,channel=channel,frame_water=True,frame_part_width=info['part_width'])
            crop,metadata=candidate_crop(self.rec,obj);np.testing.assert_array_equal(crop,a[:32,84:196])
        with self.assertRaises(ValueError):candidate_crop(self.rec,dict(item,box=[0,0,float('nan'),4]))
        with self.assertRaises(ValueError):candidate_crop(self.rec,dict(item,source_dat='outra.DAT'))
        with self.assertRaises(ValueError):candidate_crop(self.rec,dict(item,box=[0,0,10,1000000]))
        with self.assertRaises(InterruptedError):save_feedback(self.rec,item,self.folder,cancel=lambda:True)
    def test_class_namespaces_and_manual_name(self):
        self.assertEqual(training_class(self.candidate()),('ghostvision',0,'Crab-Pot'))
        self.assertEqual(training_class(dict(self.candidate(),model='SonarVision v6',class_id=3))[1],3)
        self.assertIsNone(training_class(dict(self.candidate(),manual_label=True)))
    def test_heatmap_max_score_not_probability_or_sum(self):
        items=[dict(east=5.,north=5.,score=.8,status='confirmado'),dict(east=5.,north=5.,score=.4,status='pendente'),dict(east=5.,north=5.,score=.99,status='rejeitado')]
        a=score_grid(items,(0,0,10,10),11,11,1)
        self.assertAlmostEqual(float(a[5,5]),.8,places=6);self.assertEqual(len(points(items)),2)
        self.assertLessEqual(a.max(),.8+1e-6);self.assertGreater(a[5,5],a[0,0])
        self.assertEqual(len(points([dict(east=float('nan'),north=1,score=.8),dict(east=1,north=1,score=1.2)])),0)
    def test_heatmap_geotiff_crs_nodata_recipe(self):
        target=self.folder/'calor.tif';items=[dict(east=330000.,north=7300000.,score=.8,status='pendente')]
        export_heatmap(items,target,self.rec.crs)
        with rasterio.open(target) as ds:
            self.assertEqual(ds.crs,self.rec.crs);self.assertEqual(ds.nodata,-9999);self.assertAlmostEqual(ds.res[0],2)
            self.assertIn('not_probability',ds.tags()['score_semantics']);self.assertTrue(ds.read(1,masked=True).max()<=.8)
        self.assertFalse(json.loads(Path(str(target)+'.recipe.json').read_text())['calibrated_probability'])
        with self.assertRaises(ValueError):score_grid([], (0,0,1,1),100000,100000)
        with self.assertRaises(InterruptedError):score_grid(items,(0,0,10,10),10,10,cancel=lambda:True)
    def test_triage_uniform_falls_back_empty_and_cancel(self):
        regions,meta=triage_regions(np.full((128,512),70,np.uint8));self.assertTrue(meta['fallback']);self.assertEqual(regions,[[0,0,512,128]])
        self.assertEqual(triage_regions(np.zeros((64,512),np.uint8))[0],[])
        with self.assertRaises(InterruptedError):triage_regions(np.ones((64,512),np.uint8),cancel=lambda:True)
    def test_triage_deterministic_texture_regions(self):
        rng=np.random.default_rng(45);a=rng.integers(50,100,(256,1024),dtype=np.uint8);a[64:112,300:370]=230;a[64:112,370:420]=10
        regions,meta=triage_regions(a);self.assertGreater(meta['patches'],16);self.assertGreater(meta['anomalous_patches'],0)
        self.assertEqual(regions,triage_regions(a)[0]);self.assertTrue(all(0<=x0<x1<=1024 and 0<=y0<y1<=256 for x0,y0,x1,y1 in regions))
    def test_triage_roi_offsets_and_semantic_models_only(self):
        a,info=self.rec.waterfall(12000,'both',True,64)
        detection={'box':[2,2,12,8],'score':.9,'model':'GhostVision YOLO12','label':'Candidato a covo','status':'pendente'}
        with patch('batch_ai.physical_image',return_value=(np.full((128,1000),80,np.uint8),2)),patch('sonar_triage.triage_regions',return_value=([[100,20,300,100]],{})),patch('batch_ai.detect_yolo',return_value=([detection],{})),patch('batch_ai.detect_multiclass',return_value=([],{})),patch('batch_ai.locate',side_effect=lambda items,rec,frame:[dict(i,in_water_column=False) for i in items]),patch('sonar_radiometry.shadow_evidence',return_value={}):
            items,stats=analyze_window(self.rec,a,info,BatchSettings(anomaly_triage=True))
        self.assertEqual(items[0]['box'],[102.,11.,112.,14.]);self.assertEqual(stats['anomaly_calls'],1);self.assertEqual(stats['yolo_calls'],1)
    def test_triage_gate_and_periodic_full_audit(self):
        a,info=self.rec.waterfall(12000,'both',True,64)
        with patch('sonar_triage.triage_regions',return_value=([],{})),patch('batch_ai.detect_yolo',return_value=([],{})) as detector:
            items,stats=analyze_window(self.rec,a,info,BatchSettings(model='covos',anomaly_triage=True));self.assertEqual(stats['gated_windows'],1);detector.assert_not_called()
            items,stats=analyze_window(self.rec,a,dict(info,triage_audit=True),BatchSettings(model='covos',anomaly_triage=True));detector.assert_called_once();self.assertEqual(stats['triage_full_audits'],1)
    def test_triage_checkpoint_identity_and_coverage_semantics(self):
        settings=BatchSettings(model='covos',first_ping=12000,last_ping=12127,remove_water=True,anomaly_triage=True)
        with patch('batch_ai.detect_yolo',return_value=([],{})),patch('sonar_triage.triage_regions',return_value=([],{})):
            result=run_batch(self.rec,self.folder/'batch',settings)
            self.assertTrue(result['success']);self.assertTrue(result['triage_may_miss_objects']);self.assertEqual(result['covered_pings'],128)
            self.assertGreater(result['model_calls']['triage_full_audits'],0);self.assertGreater(result['model_calls']['gated_windows'],0)
            again=run_batch(self.rec,self.folder/'batch',settings);self.assertEqual(again['model_calls'],result['model_calls'])
            with self.assertRaises(ValueError):run_batch(self.rec,self.folder/'batch',replace(settings,anomaly_triage=False))
        with self.assertRaises(ValueError):run_batch(self.rec,self.folder/'invalid',replace(settings,triage_audit_every=0))
    def test_triage_cost_fallback_preserves_full_input_scale(self):
        a,info=self.rec.waterfall(12000,'both',True,64)
        physical=np.full((128,1000),80,np.uint8)
        with patch('batch_ai.physical_image',return_value=(physical,2)),patch('sonar_triage.triage_regions',return_value=([[0,0,400,128],[600,0,1000,128]],{})),patch('batch_ai.detect_yolo',return_value=([],{'tiles':2})) as detector:
            items,stats=analyze_window(self.rec,a,info,BatchSettings(model='covos',anomaly_triage=True))
        self.assertEqual(stats['triage_cost_fallbacks'],1);detector.assert_called_once();self.assertEqual(detector.call_args.args[0].shape,physical.shape)
        self.assertEqual(tile_count((128,1000),2048,640,480),2)
    def test_ensemble_deduplication_keeps_model_namespaces(self):
        a,info=self.rec.waterfall(12000,'both',True,64)
        covo={'box':[100,4,200,12],'score':.9,'model':'GhostVision YOLO12','class_id':0,'label':'Candidato a covo'}
        debris=dict(covo,model='SonarVision v6',label='Detrito / obstáculo',score=.8)
        with patch('batch_ai.detect_yolo',return_value=([covo],{})),patch('batch_ai.detect_multiclass',return_value=([debris],{})),patch('batch_ai.locate',side_effect=lambda items,rec,frame:[dict(i,in_water_column=False) for i in items]),patch('sonar_radiometry.shadow_evidence',return_value={}):
            items,stats=analyze_window(self.rec,a,info,BatchSettings())
        self.assertEqual(len(items),2);self.assertEqual(len({item['model'] for item in items}),2)
    def test_gui_feedback_is_async_and_heatmap_renders(self):
        project=self.folder/'meta-project';shutil.copytree(self.rec.project/'meta',project/'meta')
        w=app.Studio(autoload=False);w.learning_root=self.folder/'dataset/treino'
        w.recording_loaded(Recording(self.rec.dat,project));w.resize(1280,900);w.show();self.qt.processEvents()
        item=self.candidate();item['east']=float(w.recording.xy[12000,0]);item['north']=float(w.recording.xy[12000,1]);w.objects={item['id']:item}
        w.object_review.refresh();w.object_review.table.selectRow(0);w.heat_toggle.setChecked(True)
        ticks=[];timer=QTimer();timer.setInterval(10);timer.timeout.connect(lambda:ticks.append(time.monotonic()));timer.start()
        try:
            w.object_review.mark('confirmado');self.assertIsNotNone(w.learning_worker)
            deadline=time.monotonic()+20
            while w.learning_worker.isRunning() and time.monotonic()<deadline:self.qt.processEvents();time.sleep(.003)
            self.qt.processEvents();self.assertFalse(w.learning_worker.isRunning());self.assertTrue(list(w.learning_root.rglob('latest.json')))
            w.map.center=w.recording.xy[12000].copy();w.map.span=100;w.map.update();self.qt.processEvents();self.assertIsNotNone(w.map.heat_cache_key)
            self.assertTrue(w.grab().save(str(self.folder/'gui45.png')))
        finally:
            timer.stop();w.pause_playback();w.sonar_timer.stop()
            for worker in [w.learning_worker,w.sonar_worker,w.object_review.crop_worker]:
                if worker and worker.isRunning():worker.wait(30000);self.qt.processEvents()
            w.close();self.qt.processEvents()

if __name__=='__main__':unittest.main(verbosity=2)
