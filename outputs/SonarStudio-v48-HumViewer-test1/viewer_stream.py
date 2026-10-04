"""Latest-request-only reader and bounded sliding/prefetch buffers."""
from collections import OrderedDict
import threading,time
from PySide6.QtCore import QObject,Signal

class WindowStream(QObject):
 ready=Signal(object);failed=Signal(str)
 def __init__(self,recording,budget=64*1024**2):
  super().__init__();self.rec=recording;self.budget=budget;self.cache=OrderedDict();self.bytes=0;self.condition=threading.Condition();self.pending=None;self.closed=False;self.serial=0;self.stats={'hits':0,'misses':0,'prefetches':0,'discarded':0};self.thread=threading.Thread(target=self.loop,daemon=True,name='Sonar sliding window');self.thread.start()
 def request(self,index,channel,water,count,range_mode,split,token):
  with self.condition:self.serial+=1;self.pending=(self.serial,index,channel,water,count,range_mode,split,token);self.condition.notify()
 def current(self,serial):return not self.closed and serial==self.serial
 def fetch(self,index,channel,water,count,mode):
  key=(int(index),channel,bool(water),int(count),mode)
  if key in self.cache:self.stats['hits']+=1;value=self.cache.pop(key);self.cache[key]=value;return value
  self.stats['misses']+=1;value=self.rec.waterfall(index,channel,water,count,range_mode=mode);self.cache[key]=value;self.bytes+=value[0].nbytes
  while self.bytes>self.budget and self.cache:
   _,old=self.cache.popitem(last=False);self.bytes-=old[0].nbytes
  return value
 def loop(self):
  while True:
   with self.condition:
    self.condition.wait_for(lambda:self.closed or self.pending is not None)
    if self.closed:return
    serial,index,channel,water,count,mode,split,token=self.pending;self.pending=None
   try:
    start=time.perf_counter();array,info=self.fetch(index,channel,water,count,mode);down=None;third=None
    if split:
     selection=split.get('channel',True) if isinstance(split,dict) else split
     name=selection if isinstance(selection,str) and selection in self.rec.channels else next((k for k in self.rec.channels if k.startswith('ds_vhighfreq')),None) or next((k for k in self.rec.channels if k.startswith('ds_')),None)
     if name:down=self.fetch(index,name,False,count,mode)
     if isinstance(split,dict) and split.get('third'):
      other=next((k for k in self.rec.channels if not k.startswith('ss_') and k!=name),None)
      if other:third=self.fetch(index,other,False,count,mode)
    if self.current(serial):self.ready.emit({'array':array,'info':info,'down':down,'third':third,'index':index,'token':token,'read_ms':(time.perf_counter()-start)*1000,'stats':dict(self.stats),'buffer_bytes':self.bytes})
    else:self.stats['discarded']+=1
    # read_rows already caches 256-ping blocks, including adjacent samples.
    # Do not render full future windows: that competes with incoming requests.
   except Exception as error:
    if self.current(serial):self.failed.emit(str(error))
 def close(self):
  with self.condition:self.closed=True;self.pending=None;self.serial+=1;self.condition.notify()
  self.thread.join(timeout=15)
  if self.thread.is_alive():raise RuntimeError('Leitor ainda ativo; gravação deve permanecer aberta.')
  self.cache.clear();self.bytes=0
