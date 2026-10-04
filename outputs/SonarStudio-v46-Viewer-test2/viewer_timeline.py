"""Sampled acoustic timeline; timestamps are the master clock."""
import numpy as np
from PySide6.QtCore import Signal,Qt
from PySide6.QtGui import QPainter,QPainterPath,QPen,QColor
from PySide6.QtWidgets import QWidget

class IntensityTimeline(QWidget):
 picked=Signal(int)
 def __init__(self):
  super().__init__();self.setMinimumHeight(48);self.setMaximumHeight(64);self.samples=None;self.times=None;self.index=0;self.markers=[];self.setToolTip('Intensidade amostrada: clique para navegar. Picos não são identificações de objetos.')
 def set_data(self,times,samples):self.times=np.asarray(times,float);self.samples=samples;self.update()
 def paintEvent(self,event):
  p=QPainter(self);p.fillRect(self.rect(),QColor('#09111a'));w=self.width();h=self.height()
  if self.times is None or not len(self.times):p.setPen(QColor('#9fb7c9'));p.drawText(8,22,'Linha do tempo acústica — preparando amostragem…');return
  start=self.times[0];span=max(1e-6,self.times[-1]-start)
  if self.samples is not None and len(self.samples):
   values=np.asarray(self.samples);lo,hi=np.percentile(values[:,1],[5,99]);path=QPainterPath()
   for j,(index,value) in enumerate(values):
    x=(self.times[int(index)]-start)/span*w;y=h-5-np.clip((value-lo)/max(1,hi-lo),0,1)*(h-12)
    path.moveTo(float(x),float(y)) if j==0 else path.lineTo(float(x),float(y))
   p.setPen(QPen(QColor('#d9954e'),1));p.drawPath(path)
  p.setPen(QPen(QColor('#70e0d3'),2));x=(self.times[self.index]-start)/span*w;p.drawLine(round(x),0,round(x),h)
  p.setPen(QPen(QColor('#f3d7a5'),2))
  for index in self.markers:x=(self.times[int(index)]-start)/span*w;p.drawLine(round(x),0,round(x),8)
 def mousePressEvent(self,event):
  if self.times is not None:
   time=self.times[0]+np.clip(event.position().x()/max(1,self.width()),0,1)*(self.times[-1]-self.times[0]);index=int(np.clip(np.searchsorted(self.times,time),0,len(self.times)-1));self.picked.emit(index)

def intensity_profile(dat,project,cancel=lambda:False,max_samples=1500,collect_preview=False):
 from recording import Recording
 rec=Recording(dat,project)
 try:
  name=next((k for k in rec.channels if k.startswith('ss_port')),rec.primary);channel=rec.channels[name];samples=[];preview=[];star=next((k for k in rec.channels if k.startswith('ss_star')),None)
  for index in np.unique(np.linspace(0,len(rec.data)-1,min(max_samples,len(rec.data))).astype(int)):
   if cancel():return None
   wanted=float(rec.data.iloc[index].time_s);j=int(np.clip(np.searchsorted(channel.time_s.to_numpy(),wanted),0,len(channel)-1));raw,row=rec.raw_ping(name,j)
   bottom=round(float(row.inst_dep_m)/float(row.pixM));values=raw[max(0,bottom):]
   samples.append([int(index),float(np.percentile(values,95)) if len(values) else 0.])
   if collect_preview:
    shrink=lambda a:np.interp(np.linspace(0,max(0,len(a)-1),64),np.arange(len(a)),a).astype(np.uint8) if len(a) else np.zeros(64,np.uint8)
    right=np.zeros(64,np.uint8)
    if star:
     table=rec.channels[star];k=int(np.clip(np.searchsorted(table.time_s.to_numpy(),wanted),0,len(table)-1));other,_=rec.raw_ping(star,k);right=shrink(other)
    preview.append(np.concatenate([shrink(raw)[::-1],right]))
  return {'samples':samples,'preview':np.asarray(preview,np.uint8)} if collect_preview else samples
 finally:rec.close()
