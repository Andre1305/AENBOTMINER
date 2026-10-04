"""Bounded-memory sonar movie export; acoustic samples are never rewritten."""
from pathlib import Path
import json,os
import numpy as np
from recording import Recording
from image_processing import colorize

def movie_indices(times,first,last,speed=1.,fps=10,reverse=False):
 times=np.asarray(times,float)
 if not 0<=first<=last<len(times) or not np.isfinite(times).all() or np.any(np.diff(times)<0) or speed<=0 or fps<=0:raise ValueError('Intervalo ou relógio inválido para vídeo.')
 duration=(times[last]-times[first])/speed
 count=max(1,int(np.ceil(duration*fps))+1)
 wanted=np.linspace(times[first],times[last],count)
 indices=np.clip(np.searchsorted(times,wanted),first,last)
 return indices[::-1] if reverse else indices

def export_movie(dat,project,path,first,last,channel,water,count,settings,speed=1.,reverse_time=False,reverse_cascade=False,progress=lambda value:None,cancel=lambda:False):
 import cv2
 from PIL import Image
 from dataclasses import asdict
 rec=Recording(dat,project);path=Path(path);temporary=path.with_name(path.stem+'.incomplete.avi');writer=None
 try:
  indices=movie_indices(rec.data.time_s.to_numpy(),first,last,speed);side=channel=='both' or str(channel).startswith('ss_')
  for number,index in enumerate(indices):
   if cancel():raise InterruptedError('Exportação de vídeo cancelada.')
   raw,info=rec.waterfall(int(index),channel,water and side,count)
   if not side:raw=raw.T
   rgb=colorize(raw,settings)[...,:3]
   if reverse_cascade and side:rgb=rgb[::-1]
   # Fixed video frame size; original source samples remain available separately.
   if writer is None:
    ratio=min(1.,1280/rgb.shape[1],720/rgb.shape[0]);size=(max(2,int(rgb.shape[1]*ratio)//2*2),max(2,int(rgb.shape[0]*ratio)//2*2));writer=cv2.VideoWriter(str(temporary),cv2.VideoWriter_fourcc(*'MJPG'),10.,size)
    if not writer.isOpened():raise RuntimeError('Codec MJPEG indisponível neste ambiente.')
   rgb=np.asarray(Image.fromarray(rgb).resize(size,Image.Resampling.BOX));writer.write(np.ascontiguousarray(rgb[...,::-1]));progress(round((number+1)*100/len(indices)))
  writer.release();writer=None
  if not temporary.exists() or temporary.stat().st_size==0:raise RuntimeError('Vídeo vazio.')
  os.replace(temporary,path);metadata=dict(source_dat=str(Path(dat).resolve()),first_ping=first,last_ping=last,channel=channel,fps=10,speed=speed,reverse_time=reverse_time,reverse_cascade=reverse_cascade,display_settings=asdict(settings),frame_size=list(size),georeferenced=False,preview_video=True)
  Path(str(path)+'.json').write_text(json.dumps(metadata,indent=2,ensure_ascii=False),encoding='utf-8');return dict(path=str(path),frames=len(indices))
 finally:
  if writer is not None:writer.release()
  temporary.unlink(missing_ok=True);rec.close()
