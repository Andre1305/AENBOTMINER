"""Numerical and format roundtrip checks, independent of image previews."""
import sys,unittest,tempfile,json,zipfile
from pathlib import Path
from dataclasses import replace
import numpy as np
import rasterio
from rasterio.transform import from_origin
from osgeo import ogr
import xml.etree.ElementTree as ET
from image_processing import ImageSettings,enhance,copper
from pro_exports import export_raster,raster_google,vectors
from bathymetry import BathySettings,generate,soundings
from recording import ROOT,Recording

class ProfessionalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.folder=ROOT/'work/sonar-studio-tests/pro-v2';cls.folder.mkdir(parents=True,exist_ok=True)
        cls.rec=Recording(ROOT/'work/input/Rec00001.DAT',ROOT/'outputs/Rec00001-processado')
    @classmethod
    def tearDownClass(cls):cls.rec.close()
    def test_copper_reference(self):
        np.testing.assert_allclose(copper(np.array([0.,.5,1.])),[[0,0,0],[.625,.3906,.24875],[1,.7812,.4975]])
    def test_all_filters_preserve_source_and_mask(self):
        rng=np.random.default_rng(17);a=np.clip(100+rng.normal(0,20,(128,128)),0,255).astype('uint8');saved=a.copy()
        valid=np.ones(a.shape,bool);valid[:4]=False
        for method in ['Original','Mediana','Lee — speckle','Non-local means','Wavelet']:
            out=enhance(a,ImageSettings(method=method,strength=.7),valid)
            self.assertEqual(out.dtype,np.uint8);self.assertFalse(out[:4].any())
            if method!='Original':self.assertLess(out[10:-10,10:-10].std(),a[10:-10,10:-10].std())
        np.testing.assert_array_equal(a,saved)
    def test_depth_correction_and_ping_interval(self):
        base=BathySettings(median_window=1,start_ping=5,end_ping=15)
        xy,z,ids,raw=soundings(self.rec,base)
        xy2,z2,ids2,raw2=soundings(self.rec,replace(base,draft=1.2,water_level=.4))
        np.testing.assert_allclose(z2-z,.8);self.assertEqual(len(ids),11)
        np.testing.assert_allclose(raw,self.rec.data.iloc[ids].inst_dep_m)
    def test_vectors_roundtrip(self):
        settings=BathySettings(median_window=1,start_ping=500,end_ping=520)
        folder=self.folder/'vectors';folder.mkdir(exist_ok=True)
        for ext in ['shp','gpx','kml','gpkg','kmz']:
            path=folder/('points-'+str(__import__('time').time_ns())+'.'+ext)
            vectors(self.rec,path,settings)
            if ext=='gpx':
                tree=ET.parse(path);points=tree.findall('.//{http://www.topografix.com/GPX/1/1}trkpt');self.assertEqual(len(points),21)
                self.assertAlmostEqual(float(points[0].attrib['lat']),self.rec.data.iloc[500].lat,places=8)
            elif ext=='kmz':
                with zipfile.ZipFile(path) as archive:self.assertIn('doc.kml',archive.namelist())
            else:
                ds=ogr.Open(str(path));layer=ds.GetLayer(0);self.assertEqual(layer.GetFeatureCount(),21)
                feature=layer.GetNextFeature();self.assertGreater(feature.GetField('depth_m'),0)
                if ext=='shp':self.assertEqual(layer.GetSpatialRef().GetAuthorityCode(None),'32723')
                if ext=='kml':self.assertAlmostEqual(feature.GetGeometryRef().GetX(),self.rec.data.iloc[500].lon,places=5)
                ds=None
    def test_bathymetry_support_mask_and_contours(self):
        settings=BathySettings(resolution=1,max_distance=8,median_window=1,start_ping=10000,end_ping=12000,contour_interval=.5)
        result=generate(self.rec,self.folder/'bathy',settings)
        with rasterio.open(result['raster']) as ds:
            a=ds.read(1,masked=True);self.assertTrue(np.ma.getmaskarray(a).any());self.assertTrue((~np.ma.getmaskarray(a)).any())
            self.assertEqual(ds.crs.to_epsg(),32723);self.assertEqual(ds.res,(1.,1.))
            self.assertGreater(float(a.min()),0)
        contours=ogr.Open(result['contours']);self.assertGreater(contours.GetLayer(0).GetFeatureCount(),0)
    def test_raster_native_grid_and_google_overlay(self):
        source=self.folder/'source.tif';transform=from_origin(330000,7291000,.25,.25)
        a=np.tile(np.arange(256,dtype='uint8'),(256,1));a[:20]=0
        with rasterio.open(source,'w',driver='GTiff',width=256,height=256,count=1,dtype='uint8',crs='EPSG:32723',transform=transform,nodata=0) as ds:ds.write(a,1)
        options=ImageSettings()
        target=self.folder/'rgb.tif';export_raster(source,target,options)
        with rasterio.open(target) as ds:
            self.assertEqual(ds.transform,transform);self.assertEqual(ds.count,4)
            self.assertFalse(ds.read(4)[:20].any());self.assertGreater(ds.read(1).max(),200)
        path=self.folder/'overlay.kmz';raster_google(source,path,options)
        with zipfile.ZipFile(path) as archive:
            self.assertIn('doc.kml',archive.namelist());self.assertTrue(any(n.endswith('.png') for n in archive.namelist()))

    def test_overlap_mean_ignores_nodata_and_preserves_grid(self):
        from mosaic_blend import recombine
        prepared=self.folder/'blend-source';prepared.mkdir(exist_ok=True)
        (prepared/'resultado.json').write_text(json.dumps({'resolution_m':1.,'success':True}))
        transform=from_origin(330000,7291000,1,1)
        for channel,value in [('ss_port',100),('ss_star',200)]:
            folder=prepared/channel/'rect_wcr';folder.mkdir(parents=True,exist_ok=True)
            a=np.full((32,32),value,'uint8')
            if channel=='ss_star':a[:5]=0
            with rasterio.open(folder/'tile.tif','w',driver='GTiff',width=32,height=32,count=1,dtype='uint8',crs='EPSG:32723',transform=transform,nodata=0) as ds:ds.write(a,1)
        for method,expected in [('mean',150),('first',100),('last',200)]:
            result=recombine(prepared,self.folder/('blend-'+method),method)
            with rasterio.open(result['mosaic']) as ds:
                a=ds.read(1);self.assertEqual(ds.transform,transform)
                self.assertTrue((a[5:]==expected).all());self.assertTrue((a[:5]==100).all())

if __name__=='__main__':unittest.main(verbosity=2)
