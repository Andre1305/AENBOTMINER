import unittest,time,json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
import app
import numpy as np,pandas as pd,rasterio
from PySide6.QtCore import Qt,QTimer
from PySide6.QtWidgets import QApplication
from recording import Recording,ROOT
from image_processing import ImageSettings
from sonar_ai import decode_multi,detect_multiclass,multi_session
from bathy_review import SoundingModel,save_edits,relief_data,ReliefView
from bathymetry import soundings,generate,BathySettings

class OptimizedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.qt=QApplication.instance() or QApplication([])
    def setUp(self):
        self.w=app.Studio(autoload=False);self.w.recording_loaded(Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado'))
        self.folder=ROOT/'work/sonar-studio-tests/v4'/str(time.time_ns());self.folder.mkdir(parents=True);self.w.recording.project=self.folder
    def tearDown(self):
        self.w.pause_playback();self.w.sonar_timer.stop()
        for worker in [self.w.sonar_worker,self.w.ai_worker,self.w.object_review.crop_worker]:
            if worker and worker.isRunning():worker.wait(30000);self.qt.processEvents()
        self.w.close();self.qt.processEvents()
    def wait(self,worker):
        deadline=time.monotonic()+30
        while worker.isRunning() and time.monotonic()<deadline:self.qt.processEvents();time.sleep(.005)
        self.qt.processEvents();self.assertFalse(worker.isRunning())
    def test_multiclass_decode_and_real_inference(self):
        output=np.zeros((1,8,3),np.float32);output[0,:4,:]=np.array([[100,100,100],[100,100,100],[40,40,40],[40,40,40]])
        output[0,4,0]=.9;output[0,4,1]=.8;output[0,7,2]=.95
        boxes=decode_multi(output,.5);self.assertEqual(len(boxes),2);self.assertEqual({b['class_id'] for b in boxes},{0,3})
        array,_=self.w.recording.waterfall(12000,'ss_port',True,64)
        boxes,meta=detect_multiclass(array,max_side=512)
        self.assertEqual(multi_session().get_inputs()[0].shape,[1,3,256,256]);self.assertEqual(len(meta['classes']),4)
        self.assertTrue(all(0<=b['class_id']<4 and 0<=b['score']<=1 for b in boxes))
    def test_filters_clear_undo_and_crop(self):
        _,info=self.w.recording.waterfall(12000,'both',False,64)
        base=dict(id='a',ping=12000,label='Naufrágio',score=.9,status='pendente',channel='ss_port',model='teste',box=[100,15,160,40],frame_start=info['start'],frame_channel='both',frame_water=False,frame_part_width=info['part_width'],east=330000,north=7290000)
        self.w.objects={'a':base,'b':dict(base,id='b',label='Detrito',score=.2)};review=self.w.object_review
        review.score.setValue(.5);self.assertEqual(review.ids,['a']);self.assertEqual(list(self.w.map.objects),['a'])
        review.table.selectRow(0);review.filter_crop();self.assertIsNotNone(review.crop_worker);self.wait(review.crop_worker);self.assertFalse(review.preview.pixmap().isNull())
        epoch=self.w.ai_epoch;review.clear_filtered();self.assertEqual(list(self.w.objects),['b']);self.assertGreater(self.w.ai_epoch,epoch)
        review.undo_clear();self.assertEqual(len(self.w.objects),2);review.clear_all();self.assertEqual(len(self.w.objects),0)
        review.undo_clear();self.assertEqual(len(self.w.objects),2)
        review.search.setText('Detrito');review.score.setValue(0);self.assertEqual(review.ids,['b'])
    def test_screen_preview_retains_extent_and_native(self):
        a=np.tile(np.arange(256,dtype=np.uint8),(400,16))
        result=self.w.process_sonar((a,ImageSettings(),False,2.,1.,1,False,(800,200)))
        self.assertEqual(result[0].shape,(200,800));self.assertEqual(result[2]*200,800);self.assertEqual(result[3]*800,4096)
        original=self.w.process_sonar((a,ImageSettings(),False,2.,1.,1,False,None));self.assertEqual(original[0].shape,a.shape)
    def test_playback_ignores_busy_ai_and_reads_off_ui(self):
        self.w.show();self.w.set_index(12000);self.qt.processEvents();self.w.sonar_timer.stop()
        self.w.ai_continuous.setChecked(True);self.w.ai_mode.setCurrentIndex(2);self.w.toggle_playback()
        real=self.w.ai_worker;self.w.ai_worker=SimpleNamespace(isRunning=lambda:True)
        before=self.w.slider.value();self.w._play_last=time.monotonic()-.2;self.w.play_tick();self.assertGreater(self.w.slider.value(),before)
        self.w.ai_worker=real;self.w.ai_continuous.setChecked(False);self.w.sonar_timer.stop()
        started=time.perf_counter();self.w.show_sonar();elapsed=time.perf_counter()-started
        self.assertLess(elapsed,.1);self.assertIsNotNone(self.w.sonar_worker);self.wait(self.w.sonar_worker);self.w.pause_playback()
        self.assertGreater(self.w.last_render_ms,0)
    def test_sounding_edits_exclusion_level_and_original_preserved(self):
        model=SoundingModel(self.w.recording);original=float(self.w.recording.data.iloc[10].inst_dep_m)
        self.assertTrue(model.setData(model.index(10,3),'30.5'));self.assertFalse(model.setData(model.index(10,3),'nan'))
        self.assertTrue(model.setData(model.index(11,4),Qt.CheckState.Unchecked,Qt.ItemDataRole.CheckStateRole))
        times=self.w.recording.data.time_s;model.edits['water_levels']=[[float(times.iloc[0]),2.],[float(times.iloc[-1]),2.]];save_edits(self.w.recording,model.edits)
        _,z,ids,raw=soundings(self.w.recording,BathySettings(median_window=1,start_ping=10,end_ping=12,draft=1,water_level=.5))
        self.assertEqual(ids.tolist(),[10,12]);self.assertAlmostEqual(z[0],29.);self.assertEqual(raw[0],original);self.assertEqual(float(self.w.recording.data.iloc[10].inst_dep_m),original)
    def test_tin_smoothing_mask_and_3d(self):
        x,y=np.meshgrid(np.arange(0,50,5),np.arange(0,50,5));xy=np.c_[x.ravel()+330000,y.ravel()+7291000];depth=10+x.ravel()*.1+y.ravel()*.05
        rec=SimpleNamespace(dat=self.w.recording.dat,project=self.folder,xy=xy,crs=self.w.recording.crs,data=pd.DataFrame({'inst_dep_m':depth,'time_s':np.arange(len(depth),dtype=float)}))
        settings=BathySettings(resolution=2,max_distance=6,median_window=1,interpolation='TIN')
        base=generate(rec,self.folder/'tin',settings);smooth=generate(rec,self.folder/'smooth',replace(settings,smoothing_m=2))
        with rasterio.open(base['raster']) as a,rasterio.open(smooth['raster']) as b:
            np.testing.assert_array_equal(a.read_masks(1),b.read_masks(1));self.assertEqual(a.crs,b.crs)
            self.assertTrue(np.isfinite(b.read(1,masked=True).compressed()).all())
        from osgeo import ogr,osr
        boundary=self.folder/'water.gpkg';dst=ogr.GetDriverByName('GPKG').CreateDataSource(str(boundary));srs=osr.SpatialReference();srs.ImportFromEPSG(32723);layer=dst.CreateLayer('water',srs,ogr.wkbPolygon)
        f=ogr.Feature(layer.GetLayerDefn());f.SetGeometry(ogr.CreateGeometryFromWkt('POLYGON ((330000 7291000,330020 7291000,330020 7291045,330000 7291045,330000 7291000))'));layer.CreateFeature(f);f=None;layer=None;dst=None
        clipped=generate(rec,self.folder/'bounded',replace(settings,boundary_path=str(boundary)))
        with rasterio.open(clipped['raster']) as ds:
            mask=ds.read_masks(1);x=ds.transform.c+(np.arange(ds.width)+.5)*ds.res[0]
            self.assertTrue(mask[:,x<330020].any());self.assertFalse(mask[:,x>330020].any())
        data=relief_data(smooth['raster'],40);self.assertLessEqual(max(data[2].shape),40);self.assertLess(np.nanmax(data[2]),0)
        view=ReliefView(self.w,data);view.show();view.canvas.draw();view.close()

if __name__=='__main__':unittest.main(verbosity=2)
