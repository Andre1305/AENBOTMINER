import unittest,time,json
from pathlib import Path
from unittest.mock import patch
import app
from PySide6.QtWidgets import QApplication,QFileDialog
from PySide6.QtCore import QTimer
from recording import ROOT,Recording
from image_processing import ImageSettings
from native_mosaic import rectify_project

class AdvancedGuiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.qt=QApplication.instance() or QApplication([])
    def setUp(self):
        self.window=app.Studio(autoload=False)
        self.window.recording_loaded(Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado'))
        self.window.professional.pings.setValue(80)
        self.window.show();self.qt.processEvents()
    def tearDown(self):
        if self.window.sonar_worker and self.window.sonar_worker.isRunning():
            self.window.sonar_worker.wait(30000);self.qt.processEvents()
        self.window.close();self.qt.processEvents()
    def test_default_copper_and_downscan_orientation(self):
        self.assertEqual(self.window.palette.currentText(),'Âmbar')
        index=self.window.channel.findData('ds_vhighfreq');self.assertGreaterEqual(index,0)
        self.window.channel.setCurrentIndex(index);self.window.show_sonar()
        self.assertEqual(self.window.raw_array.shape[1],80)
        self.assertGreater(self.window.raw_array.shape[0],80)
        self.assertIsNotNone(self.window.sonar.image)
    def test_expensive_filter_runs_off_ui_and_state_roundtrip(self):
        p=self.window.professional;p.method.setCurrentText('Non-local means');p.compare.setChecked(True)
        p.apply_image();self.assertIsNotNone(self.window.sonar_worker)
        self.assertTrue(self.window.sonar_worker.wait(30000));self.qt.processEvents()
        self.assertIsNotNone(self.window.sonar.image)
        state=p.state();p.method.setCurrentText('Original');p.restore(state)
        self.assertEqual(p.method.currentText(),'Non-local means');self.assertTrue(p.compare.isChecked())
    def test_float_depth_raster_and_contours_are_separate_layer(self):
        base=ROOT/'work/sonar-studio-tests/pro-v2/bathy'
        self.window.open_mosaic(base/'batimetria-profundidade.tif');self.window.load_contours(base/'isobatas.gpkg')
        self.window.map.render_raster();self.assertIsNotNone(self.window.map.image)
        self.assertEqual(self.window.map_layer.currentIndex(),1);self.assertTrue(self.window.map.contours)
        self.window.map_layer.setCurrentIndex(0)
        self.assertEqual(self.window.map.dataset.dtypes[0],'uint8')
    def test_pre_rectification_filtered_fixture(self):
        base=ROOT/'work/sonar-studio-tests'
        result=rectify_project(base/'fresh-project-mapped',base/'pro-v2/treated-fixture',.25,progress=lambda x:None,treatment=ImageSettings(method='Lee — speckle',strength=.2,palette='Cinza'))
        self.assertTrue(result['success']);self.assertEqual(result['tile_count'],4)
        self.assertEqual(result['treatment']['method'],'Lee — speckle')

    def test_compare_contains_same_full_extent_and_async_export(self):
        import numpy as np
        from image_processing import enhance
        from osgeo import ogr
        array=np.tile(np.arange(128,dtype='uint8'),(64,1))
        options=ImageSettings(method='Mediana',strength=.3)
        result=app.Studio.process_sonar((array,options,True,1,1,9,False))
        self.assertEqual(result[0].shape,(64,256))
        np.testing.assert_array_equal(result[0][:,:128],enhance(array,ImageSettings()))
        p=self.window.professional;p.start.setValue(101);p.end.setValue(123);p.format.setCurrentText('Sondagens Shapefile')
        target=ROOT/'work/sonar-studio-tests/pro-v2'/('gui-export-'+str(time.time_ns())+'.shp')
        with patch.object(QFileDialog,'getSaveFileName',return_value=(str(target),'')):p.export()
        self.assertTrue(self.window.worker.wait(30000));self.qt.processEvents()
        ds=ogr.Open(str(target));self.assertIsNotNone(ds);self.assertEqual(ds.GetLayer(0).GetFeatureCount(),23)

if __name__=='__main__':unittest.main(verbosity=2)
