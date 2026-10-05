"""Pointwise-only GPU preview. Native freeze/export retain legacy filter order."""
import time
import numpy as np
from PIL import Image

def structural(array,limit=None):
 start=time.perf_counter();raw=np.asarray(array,np.uint8);h,w=raw.shape
 nw=min(w,max(1,limit[0])) if limit else w;nh=min(h,max(1,limit[1])) if limit else h
 preview=(nw,nh)!=(w,h);a=np.asarray(Image.fromarray(raw).resize((nw,nh),Image.Resampling.BOX)) if preview else raw
 return dict(original=a,treated=a,preview=preview,raw_shape=[h,w],shape=list(a.shape),scale_x=w/a.shape[1],scale_y=h/a.shape[0],filter_ms={'live_pointwise':0.},total_ms=(time.perf_counter()-start)*1000,live_color=True)
