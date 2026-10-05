"""LLVM CPU kernels. No fastmath: retain acoustic interpolation semantics."""
import math
import numpy as np
from numba import njit

@njit(cache=True,nogil=True)
def gather_rows(source,offsets,lengths,width):
 result=np.zeros((len(offsets),width),dtype=np.uint8)
 for row in range(len(offsets)):
  offset=offsets[row];length=lengths[row]
  if offset<0 or length<0 or offset>len(source) or length>len(source)-offset:
   raise ValueError('Ping fora dos limites do arquivo SON.')
  count=min(width,length)
  for column in range(count):result[row,column]=source[offset+column]
 return result

@njit(cache=True,nogil=True)
def slant_rows(source,depths,pixel_sizes,sampling):
 rows,width=source.shape;result=np.zeros_like(source)
 for row in range(rows):
  depth=depths[row];pixel=pixel_sizes[row]
  if not math.isfinite(depth) or not math.isfinite(pixel) or pixel<=0:continue
  for column in range(width):
   ground=column*sampling;position=math.sqrt(ground*ground+depth*depth)/pixel
   if position<0 or position>width-1:continue
   left=int(position)
   if left==width-1:result[row,column]=source[row,left]
   else:
    fraction=position-left;result[row,column]=int(source[row,left]+fraction*(float(source[row,left+1])-float(source[row,left])))
 return result

@njit(cache=True,nogil=True)
def palette_rows(source,lookup):
 rows,width=source.shape;result=np.empty((rows,width,4),np.uint8)
 for row in range(rows):
  for column in range(width):
   value=source[row,column]
   for color in range(4):result[row,column,color]=lookup[value,color]
 return result

def warmup():
 source=np.arange(64,dtype=np.uint8);source.flags.writeable=False
 offsets=np.array([0,16],dtype=np.int64);lengths=np.array([16,16],dtype=np.int64)
 rows=gather_rows(source,offsets,lengths,16)
 slant_rows(rows,np.array([1.,2.]),np.array([.1,.1]),.1)
 palette_rows(rows,np.zeros((256,4),dtype=np.uint8))

