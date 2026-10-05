"""Integration checks against this real recording and Qt's offscreen UI."""
import os
os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pathlib
import time
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
import app as gui
from PySide6.QtCore import Qt, QPoint
from PySide6.QtGui import QFontDatabase
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QFileDialog

from recording import ROOT, Recording, cache_project, source_files
from triangle_renderer import triangle_warp
from native_mosaic import stripe_warp

SCRATCH = ROOT / 'work' / 'sonar-studio-tests'


class RecordingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SCRATCH.mkdir(parents=True, exist_ok=True)
        cls.recording = Recording(ROOT/'work/input/Rec00001.DAT', ROOT/'outputs/Rec00001-processado')

    @classmethod
    def tearDownClass(cls):
        cls.recording.close()

    def test_four_channels_and_actual_ping_counts(self):
        self.assertEqual(len(self.recording.channels), 4)
        self.assertEqual(len(self.recording.data), 77533)
        self.assertAlmostEqual(self.recording.data.pixM.median(), .01922445992924)

    def test_direct_ping_matches_original_pingmapper_reader(self):
        from pingmapper.class_rectObj import rectObj
        original = rectObj(str(ROOT/'outputs/Rec00001-processado/meta/B002_ss_port_meta.meta'))
        original._getScanChunkSingle(0, True)
        raw, row = self.recording.raw_ping('ss_port', 0)
        np.testing.assert_array_equal(raw, original.sonDat[:len(raw), 0])

    def test_last_ping_is_readable(self):
        values, row = self.recording.raw_ping('ss_star', len(self.recording.data)-1)
        self.assertEqual(len(values), int(row.ping_cnt))
        self.assertGreater(int(values.max()), 0)

    def test_combined_waterfall_and_water_removal(self):
        wet, info = self.recording.waterfall(12000, 'both', False, count=24)
        dry, _ = self.recording.waterfall(12000, 'both', True, count=24)
        self.assertEqual(wet.shape, dry.shape)
        self.assertEqual(wet.shape[0], 24)
        self.assertGreater(np.count_nonzero(wet != dry), 100)
        self.assertGreater(info['along_track_m'], 0)
        self.assertLess(info['along_track_m'], 3)

    def test_nearest_ping_and_temperature_cache(self):
        xy = self.recording.xy[12000]
        index, distance = self.recording.nearest(*xy)
        self.assertAlmostEqual(distance, 0)
        np.testing.assert_allclose(self.recording.xy[index], xy)
        dat = self.recording.dat
        self.assertNotEqual(cache_project(dat, 10), cache_project(dat, 22))

    def test_missing_companion_folder_is_actionable(self):
        path = SCRATCH/'Incomplete.DAT'; path.write_bytes(b'test')
        with self.assertRaisesRegex(ValueError, 'SON'):
            source_files(path)

    def test_csv_preserves_original_and_processed_depths(self):
        path = SCRATCH/'depths.csv'; self.recording.export_csv(path)
        data = pd.read_csv(path)
        self.assertEqual(len(data), 77533)
        self.assertIn('inst_dep_m', data)
        self.assertIn('dep_m_interp', data)


class RendererTests(unittest.TestCase):
    def test_native_adapter_can_be_used_for_multiple_gui_operations(self):
        from native_mosaic import install_stripe_renderer
        first=install_stripe_renderer()
        method=first._rectSonRubber
        second=install_stripe_renderer()
        self.assertIs(method,second._rectSonRubber)

    def test_native_triangle_renderer_matches_pingmapper_on_deformed_meshes(self):
        from pingmapper.funcs_common import FastPiecewiseAffineTransform
        rng = np.random.default_rng(14)
        source = np.array([[0,0],[99,0],[0,79],[99,79],[45,35],[20,58],[80,40]], float)
        for scale in [1.0, 1.7, 3.1]:
            image = rng.integers(0,256,(80,100), dtype=np.uint8)
            target = source*scale + rng.uniform(-2,2,source.shape) + [8,8]
            transform = FastPiecewiseAffineTransform(); transform.estimate(source,target)
            shape = (int(80*scale+20), int(100*scale+20))
            expected = stripe_warp(image,transform.inverse,shape)
            actual = triangle_warp(image,transform.inverse,shape)
            self.assertLessEqual(int(np.abs(actual.astype(int)-expected.astype(int)).max()),1)


class GuiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SCRATCH.mkdir(parents=True,exist_ok=True)
        cls.application = QApplication.instance() or QApplication([])
        QFontDatabase.addApplicationFont(str(pathlib.Path(gui.__file__).parent/'assets/NotoSans.ttf'))
        gui.STYLE = gui.STYLE.replace('Segoe UI','Noto Sans').replace('Consolas','Noto Sans')

    def setUp(self):
        self.window = gui.Studio(autoload=False)
        self.errors = []
        self.window.notify_error = self.errors.append
        self.window.show()
        rec = Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado')
        self.window.recording_loaded(rec)
        QTest.qWait(180)

    def tearDown(self):
        self.window.close()
        self.application.processEvents()
        self.assertEqual(self.errors,[])

    def test_map_native_zoom_and_raster_read(self):
        self.window.map.native_zoom()
        self.window.map.render_raster()
        self.assertIsNotNone(self.window.map.image)
        self.assertAlmostEqual(self.window.map.meters_per_pixel(), self.window.map.dataset.res[1])

    def test_selection_updates_sonar_and_depth_marker(self):
        self.window.set_index(12000); self.window.show_sonar()
        self.assertEqual(self.window.map.index,12000)
        self.assertEqual(self.window.depth.index,12000)
        self.assertIsNotNone(self.window.sonar.image)
        self.assertIn('12,001',self.window.ping_label.text())

    def test_measure_two_map_points(self):
        self.window.toggle_measure(True)
        QTest.mouseClick(self.window.map,Qt.MouseButton.LeftButton,pos=QPoint(100,100))
        QTest.mouseClick(self.window.map,Qt.MouseButton.LeftButton,pos=QPoint(200,100))
        self.assertEqual(len(self.window.map.measure_points),2)
        distance=np.linalg.norm(self.window.map.measure_points[1]-self.window.map.measure_points[0])
        self.assertAlmostEqual(distance,100*self.window.map.meters_per_pixel())

    def test_visible_viewport_png_export(self):
        self.window.show_sonar()
        path=SCRATCH/'raw-sonar.png'
        with patch.object(QFileDialog,'getSaveFileName',return_value=(str(path),'PNG')):
            self.window.export_sonar()
        self.assertTrue(self.window.worker.wait(30000));self.application.processEvents()
        from PIL import Image
        with Image.open(path) as image:
            self.assertEqual(image.size,(self.window.sonar.viewport().grab().width(),self.window.sonar.viewport().grab().height()))

    def test_map_crop_export_uses_source_pixels(self):
        self.window.map.native_zoom()
        path=SCRATCH/'map-crop.png'
        with patch.object(QFileDialog,'getSaveFileName',return_value=(str(path),'PNG')):
            self.window.export_map_area()
        from PIL import Image
        with Image.open(path) as image:
            self.assertLessEqual(abs(image.width-self.window.map.width()),2)
            self.assertLessEqual(abs(image.height-self.window.map.height()),2)

    def test_project_save_and_restore_selected_ping_and_palette(self):
        self.window.set_index(15000);self.window.palette.setCurrentText('Âmbar')
        path=SCRATCH/'project.json'
        with patch.object(QFileDialog,'getSaveFileName',return_value=(str(path),'JSON')):
            self.window.save_project()
        self.window.set_index(0);self.window.palette.setCurrentText('Cinza')
        with patch.object(QFileDialog,'getOpenFileName',return_value=(str(path),'JSON')):
            self.window.open_project()
        deadline=time.monotonic()+20
        while self.window.worker.isRunning() and time.monotonic()<deadline:
            self.application.processEvents();time.sleep(.01)
        self.application.processEvents()
        self.assertFalse(self.window.worker.isRunning())
        self.assertEqual(self.window.slider.value(),15000)
        self.assertEqual(self.window.palette.currentText(),'Âmbar')


if __name__=='__main__':
    unittest.main(verbosity=2)
