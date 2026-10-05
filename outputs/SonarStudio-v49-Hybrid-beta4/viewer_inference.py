from contextlib import contextmanager
"""Local window inference cache and display-only associated interpolation."""
from pathlib import Path
import json,sqlite3,hashlib,time
import numpy as np

def source_identity(rec):
 from recording import source_files
 _,files=source_files(rec.dat)
 payload={'dat':hashlib.sha256(rec.dat.read_bytes()).hexdigest(),'son':[(str(p),p.stat().st_size,p.stat().st_mtime_ns) for p in files],'navigation':[(str(p),p.stat().st_size,p.stat().st_mtime_ns) for p in sorted((rec.project/'meta').glob('*_meta.csv'))]}
 return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()

def roi_identity(array,rows):
 digest=hashlib.sha256(np.ascontiguousarray(array,dtype=np.uint8).tobytes())
 digest.update(str(array.shape).encode())
 cols=[c for c in ['time_s','e','n','lat','lon','inst_dep_m','instr_heading','speed_ms','pixM'] if c in rows]
 digest.update(rows[cols].to_csv(index=False,float_format='%.17g').encode())
 return digest.hexdigest()

def inference_key(identity,info,model_hash,threshold,detailed,content_hash=None):
 pipeline_hash=hashlib.sha256(b''.join((Path(__file__).parent/name).read_bytes() for name in ['sonar_ai.py','batch_ai.py','recording.py','viewer_controller.py','image_processing.py','sonar_radiometry.py','viewer_inference.py','native_kernels.py','native_engine.py'])).hexdigest()
 payload={'pipeline_sha256':pipeline_hash,'source':identity,'roi_sha256':content_hash,'channel':info['channel'],'start':info['start'],'end':info['end'],'part_width':info['part_width'],'water':info['remove_water'],'model_sha256':model_hash,'threshold':threshold,'detailed':detailed,'preprocess':'original_gray-watermask-physical-aspect-v3'}
 return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()

class ResultCache:
 def __init__(self,path,max_entries=1000):
  self.path=Path(path);self.max_entries=max_entries;self.path.parent.mkdir(parents=True,exist_ok=True)
  with self.connection() as db:db.execute('CREATE TABLE IF NOT EXISTS inference(key TEXT PRIMARY KEY, payload TEXT NOT NULL, used REAL NOT NULL)')
 @contextmanager
 def connection(self):
  db=sqlite3.connect(self.path,timeout=5);db.execute('PRAGMA journal_mode=WAL')
  try:
   with db:yield db
  finally:db.close()
 def get(self,key):
  with self.connection() as db:
   row=db.execute('SELECT payload FROM inference WHERE key=?',(key,)).fetchone()
   if row:db.execute('UPDATE inference SET used=? WHERE key=?',(time.time(),key))
  return json.loads(row[0]) if row else None
 def put(self,key,value):
  payload=json.dumps(value,ensure_ascii=False,allow_nan=False)
  with self.connection() as db:
   db.execute('INSERT OR REPLACE INTO inference VALUES (?,?,?)',(key,payload,time.time()));db.execute('DELETE FROM inference WHERE key IN (SELECT key FROM inference ORDER BY used DESC LIMIT -1 OFFSET ?)',(self.max_entries,))
   # Bound payloads even if unusually dense detections arrive.
   while db.execute('SELECT COALESCE(SUM(LENGTH(payload)),0) FROM inference').fetchone()[0]>32*1024**2:db.execute('DELETE FROM inference WHERE key=(SELECT key FROM inference ORDER BY used LIMIT 1)')

def interpolated_boxes(left,right,index,max_gap=32):
 """Only bracketed observations; no extrapolation, new detections or invented scores."""
 p0=left['index'];p1=right['index']
 if not p0<index<p1 or p1-p0>max_gap:return []
 out=[];used=set();t=(index-p0)/(p1-p0)
 def absolute(obj):
  box=np.array(obj['box'],float);box[[1,3]]+=obj['frame_start'];return box
 for a in left['items']:
  if a.get('status','pendente')!='pendente':continue
  ba=absolute(a);best=None
  for j,b in enumerate(right['items']):
   if j in used or a.get('class_id')!=b.get('class_id') or a.get('model')!=b.get('model') or a['channel']!=b['channel']:continue
   bb=absolute(b);size=np.maximum(0,np.minimum(ba[2:],bb[2:])-np.maximum(ba[:2],bb[:2]));intersection=size.prod();union=np.prod(ba[2:]-ba[:2])+np.prod(bb[2:]-bb[:2])-intersection;iou=intersection/max(1e-9,union)
   if iou<.25 or abs(a.get('ground_range_m',0)-b.get('ground_range_m',0))>2:continue
   if best is None or iou>best[0]:best=(iou,j,b,bb)
  if best:
   _,j,b,bb=best;used.add(j);obj=dict(a);obj.update(box=((1-t)*ba+t*bb).tolist(),frame_start=0,score=None,visual_interpolation=True,observation_scores=[a['score'],b['score']],label=a['label']+' · posição interpolada');out.append(obj)
 return out
