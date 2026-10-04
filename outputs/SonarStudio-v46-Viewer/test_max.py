import unittest,tempfile,json,hashlib
from pathlib import Path
from types import SimpleNamespace
import app
import numpy as np,cv2,pandas as pd,rasterio
from rasterio.transform import from_origin
from PIL import Image
from recording import ROOT,Recording
from overlap_alignment import estimate_rigid,transformed,align_tiles
from tide_datum import import_tides,tide_at,suggested_anchor
from bathymetry import soundings,BathySettings
from finetune_head import supervision

class MaxMetrologyTests(unittest.TestCase):
 def setUp(self):self.folder=Path(tempfile.mkdtemp(prefix='max-validation-',dir=ROOT/'work/tmp'))
 def test_known_rotation_translation_recovered(self):
  rng=np.random.default_rng(45);a=cv2.GaussianBlur(rng.integers(20,230,(192,256),dtype=np.uint8),(0,0),1).astype(np.float32)
  m=cv2.getRotationMatrix2D((127.5,95.5),1.4,1);m[:,2]+=[6,-4];b=transformed(a,m)
  result=estimate_rigid(a,b);self.assertTrue(result['accepted']);self.assertGreater(result['after']['ncc'],.85);self.assertLess(result['after']['rmse'],result['before']['rmse']*.5)
  recovered=np.vstack([result['matrix_pixels'],[0,0,1]])@np.vstack([m,[0,0,1]])
  corners=np.array([[20,20,1],[220,20,1],[20,170,1],[220,170,1]]).T
  self.assertLess(np.max(np.linalg.norm((recovered@corners-corners)[:2],axis=0)),1.)
 def test_flat_overlap_rejected(self):self.assertFalse(estimate_rigid(np.full((40,100),70,np.float32),np.full((40,100),70,np.float32))['accepted'])
 def test_alignment_cancellation(self):
  with self.assertRaises(InterruptedError):estimate_rigid(np.ones((40,40)),np.ones((40,40)),cancel=lambda:True)
 def test_tide_utc_csv_json_and_coverage(self):
  csv=self.folder/'tide.csv';csv.write_text('timestamp,height_m\n2026-09-26T12:00:00-03:00,0.2\n2026-09-26T13:00:00-03:00,0.8\n')
  table=import_tides(csv,'2026-09-26T15:00:00Z',10,'CONTROL ONLY');np.testing.assert_allclose(tide_at([10,1810,3610],table),[.2,.5,.8])
  with self.assertRaises(ValueError):tide_at([9],table)
  with self.assertRaises(ValueError):import_tides(csv,'2026-09-26T15:00:00',10,'CONTROL')
  js=self.folder/'tide.json';js.write_text(json.dumps({'tides':[{'timestamp':v[0],'height_m':v[1]} for v in table['values_utc']]}));self.assertEqual(import_tides(js,table['recording_anchor_utc'],10,'CONTROL')['values_utc'],table['values_utc'])
 def test_real_soundings_tide_formula_controlled(self):
  rec=Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados')
  try:
   times=rec.data.time_s.to_numpy();anchor=suggested_anchor(rec)
   from tide_datum import utc_seconds
   stamp=utc_seconds(anchor);table={'values_utc':[[stamp-1,.25],[stamp+times[-1]-times[0]+1,.75]],'recording_anchor_utc':stamp,'recording_anchor_time_s':times[0]}
   base={'pings':{},'water_levels':[]};settings=BathySettings(draft=.2,water_level=.1)
   _,relative,i,_=soundings(rec,settings,base);_,corrected,j,_=soundings(rec,settings,{**base,'tide_table':table})
   np.testing.assert_array_equal(i,j);np.testing.assert_allclose(relative-corrected,tide_at(times[i],table),atol=1e-10)
   with self.assertRaises(ValueError):soundings(rec,settings,{**base,'water_levels':[[0,1]],'tide_table':table})
  finally:rec.close()
 def test_negative_mask_only_rejected_class_region(self):
  s={'class_id':2,'decision':'rejeitado','box_xywh':[.5,.5,.2,.2]};target,mask=supervision(s,(1,4,32,32),256,256,256,4)
  self.assertEqual(target.sum(),0);self.assertGreater(mask.sum(),0);self.assertEqual(mask[:,[0,1,3]].sum(),0);self.assertLess(mask.sum(),100)
 def test_integrated_tile_transform_original_preserved(self):
  prepared=self.folder/'prepared';folder=prepared/'ss_port/rect_wcr';folder.mkdir(parents=True);(prepared/'resultado.json').write_text(json.dumps({'resolution_m':.25}))
  rng=np.random.default_rng(45);a=cv2.GaussianBlur(rng.integers(20,230,(192,80),dtype=np.uint8),(0,0),1)
  m=cv2.getRotationMatrix2D((39.5,95.5),1.2,1);m[:,2]+=[3,-4];b=transformed(a,m)
  before=[]
  for name,data in [('a',a),('b',b)]:
   path=folder/(name+'.tif')
   with rasterio.open(path,'w',driver='GTiff',height=192,width=80,count=1,dtype='uint8',crs='EPSG:32723',transform=from_origin(330000,7300000,.25,.25),nodata=0) as ds:ds.write(data,1)
   before.append(hashlib.sha256(path.read_bytes()).hexdigest())
  out=align_tiles(prepared,self.folder/'aligned');evidence=json.loads((out/'alignment-evidence.json').read_text());self.assertEqual(evidence['accepted'],1)
  self.assertEqual(before,[hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(folder.glob('*.tif'))])
  self.assertTrue((out/'ss_port/rect_wcr/b.tif').exists())

 def test_export_latest_decisions_and_physical_domain(self):
  from active_learning import save_feedback
  from dataset_export import export_dataset
  rec=Recording(ROOT/'work/input-rec00002/Rec00002.DAT',ROOT/'outputs/Rec00002-beta/metadados')
  try:
   a,info=rec.waterfall(12000,'both',False,64)
   item=dict(id='MAX-export-test',model='GhostVision YOLO12',label='Candidato a covo',status='confirmado',ping=12000,channel='ss_port',box=[100,8,180,24],frame_start=info['start'],frame_channel='both',frame_water=False,frame_part_width=info['part_width'],source_dat=str(rec.dat),inference_row_ratio=3.)
   save_feedback(rec,item,self.folder/'feedback');save_feedback(rec,dict(item,id='MAX-negative',status='rejeitado'),self.folder/'feedback')
   result=export_dataset(self.folder/'feedback',self.folder/'dataset','ghostvision');self.assertEqual(result['positive'],1);self.assertEqual(result['partial_negative'],1)
   manifest=json.loads(Path(result['manifest']).read_text());self.assertFalse(manifest['independent_validation_available'])
   for sample in manifest['samples']:
    label=(self.folder/'dataset'/sample['label']).read_text()
    self.assertEqual(bool(label),sample['decision']=='confirmado');self.assertEqual(sample['inference_row_ratio'],3.)
    self.assertFalse(sample['whole_image_background_verified'])
   save_feedback(rec,dict(item,status='rejeitado'),self.folder/'feedback')
   new=export_dataset(self.folder/'feedback',self.folder/'dataset2','ghostvision');self.assertEqual(new['positive'],0)
  finally:rec.close()
 def test_selected_weights_hash_guard(self):
  from unittest.mock import patch
  import model_registry
  target=self.folder/'fake.onnx';target.write_bytes(b'controlled');config=self.folder/'selection.json';config.write_text(json.dumps({'sonarvision':{'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest()}}))
  with patch.object(model_registry,'CONFIG',config):
   self.assertEqual(model_registry.model_path('sonarvision'),target);target.write_bytes(b'changed model')
   with self.assertRaises(ValueError):model_registry.model_path('sonarvision')
if __name__=='__main__':unittest.main()

