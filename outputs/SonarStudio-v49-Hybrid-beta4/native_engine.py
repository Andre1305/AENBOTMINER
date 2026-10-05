"""Lazy, warmed native engine; compilation never starts in Qt paint callbacks."""
import threading,time
import numpy as np

class NativeEngine:
 def __init__(self):
  self.lock=threading.Lock();self.ready=False;self.started=False;self.error='';self.module=None;self.warmup_ms=None;self.thread=None
 def start(self):
  with self.lock:
   if self.started:return
   self.started=True;self.thread=threading.Thread(target=self._warm,name='Sonar LLVM warmup',daemon=True);self.thread.start()
 def _warm(self):
  begin=time.perf_counter()
  try:
   import native_kernels
   native_kernels.warmup();self.module=native_kernels;self.warmup_ms=(time.perf_counter()-begin)*1000;self.ready=True
  except Exception as error:self.error=f'{type(error).__name__}: {error}'
 def status(self):
  return {'ready':self.ready,'state':'LLVM CPU' if self.ready else 'Legado — aquecendo LLVM' if self.started and not self.error else 'Legado — '+(self.error or 'LLVM inativo'),'warmup_ms':self.warmup_ms,'gil_released':self.ready,'fastmath':False}
 def gather(self,source,offsets,lengths,width):
  source=np.asarray(source)
  if source.ndim!=1 or source.dtype!=np.uint8 or not source.flags.c_contiguous:raise ValueError('Buffer SON precisa ser bytes contíguos.')
  offsets=np.ascontiguousarray(offsets,dtype=np.int64);lengths=np.ascontiguousarray(lengths,dtype=np.int64)
  # Validate before entering code without bounds checking. Never silently pad corrupt records.
  size=len(source)
  if len(offsets)!=len(lengths) or width<1 or np.any(offsets<0) or np.any(lengths<0) or np.any(offsets>size) or np.any(lengths>size-offsets):raise ValueError('Ping fora dos limites do arquivo SON.')
  if not self.ready:return None
  return self.module.gather_rows(source,offsets,lengths,int(width))
 def slant(self,source,depths,pixels,sampling):
  if not self.ready:return None
  a=np.ascontiguousarray(source,dtype=np.uint8);depths=np.ascontiguousarray(depths,dtype=np.float64);pixels=np.ascontiguousarray(pixels,dtype=np.float64)
  if a.ndim!=2 or depths.ndim!=1 or pixels.ndim!=1:raise ValueError('Matrizes inválidas para correção de alcance.')
  if len(depths)!=len(a) or len(pixels)!=len(a) or not np.isfinite(sampling) or sampling<=0:raise ValueError('Geometria inválida para correção de alcance.')
  return self.module.slant_rows(a,depths,pixels,float(sampling))
 def palette(self,source,lookup):
  if not self.ready:return None
  source=np.ascontiguousarray(source,dtype=np.uint8);lookup=np.ascontiguousarray(lookup,dtype=np.uint8)
  if source.ndim!=2 or lookup.shape!=(256,4):raise ValueError('Tabela de paleta inválida.')
  return self.module.palette_rows(source,lookup)

ENGINE=NativeEngine()
