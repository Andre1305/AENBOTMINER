"""Structural preview filters in bounded CPU arrays, colors applied separately."""
from dataclasses import replace
import time
import numpy as np
from PIL import Image
from image_processing import enhance,ImageSettings,palette_rgb

def structural(array,settings,limit=None,downscan=False):
 start=time.perf_counter();raw=np.asarray(array,np.uint8);h,w=raw.shape
 if limit:
  nw=min(w,max(1,limit[0]));nh=min(h,max(1,limit[1]));preview=(nw,nh)!=(w,h)
  a=np.asarray(Image.fromarray(raw).resize((nw,nh),Image.Resampling.BOX)) if preview else raw
 else:a=raw;preview=False
 original=a.copy();timings={};x=a.T if downscan else a
 for name,opts in [('destripe',ImageSettings(destripe=settings.destripe)),('denoise',ImageSettings(method=settings.method,strength=settings.strength)),('clahe',ImageSettings(clahe=settings.clahe)),('sharpen',ImageSettings(sharpen=settings.sharpen))]:
  if name=='destripe' and settings.destripe<=0 or name=='denoise' and settings.method=='Original' or name=='clahe' and settings.clahe<=0 or name=='sharpen' and settings.sharpen<=0:continue
  before=time.perf_counter();x=enhance(x,opts);timings[name]=(time.perf_counter()-before)*1000
 if downscan:x=x.T
 return {'original':original,'treated':x,'preview':preview,'raw_shape':[h,w],'shape':list(x.shape),'scale_x':w/x.shape[1],'scale_y':h/x.shape[0],'filter_ms':timings,'total_ms':(time.perf_counter()-start)*1000}

def colors(array,settings):
 if settings.white<=settings.black:raise ValueError('Branco deve ser maior que preto.')
 x=np.clip((np.asarray(array,np.float32)-settings.black)/(settings.white-settings.black),0,1)**(1/max(.1,settings.gamma));rgb=palette_rgb(x,settings.palette)
 return np.dstack([np.round(rgb*255).astype('uint8'),np.full(x.shape,255,np.uint8)])
