"""Native digital echo diagnostics; no calibrated acoustic/material inference."""
import math
import numpy as np
from PySide6.QtWidgets import QWidget
from PySide6.QtGui import QPainter,QColor,QPen

DB_DEFINITION='20 log10(DN / 255): dB digitais relativos, sem calibração acústica'

def relative_db(dn):
 return 20*math.log10(float(dn)/255) if float(dn)>0 else None

def native_profile(rec,info,x,y,downscan=False,radius=32):
 offset=x if downscan else y
 ping=int(np.clip(math.floor(info['start']+offset),info['start'],info['end']-1))
 sample=y if downscan else x;channel=info['channel'];part=info['part_width']
 if channel=='both':
  port=sample<part;sample=part-1-sample if port else sample-part
  channel=next(k for k in rec.channels if k.startswith('ss_port' if port else 'ss_star'))
 elif channel.startswith('ss_port'):sample=part-1-sample
 master=rec.data.iloc[ping];table=rec.channels[channel];times=table.time_s.to_numpy();j=int(np.clip(np.searchsorted(times,float(master.time_s)),0,len(times)-1))
 if j>0 and abs(times[j-1]-master.time_s)<abs(times[j]-master.time_s):j-=1
 timing=info.get('channel_timing',{}).get(channel,{})
 if timing and not timing['valid_rows'][ping-info['start']]:raise ValueError('Canal sem retorno sincronizado neste ping.')
 with rec.read_lock:
  raw,row=rec.raw_ping(channel,j);step=float(row.pixM);distance=max(0,float(sample))*info['sampling_m']
  if info['remove_water']:distance=math.hypot(distance,float(row.inst_dep_m))
  index=int(round(distance/step))
  if not 0<=index<len(raw):raise ValueError('Ponto fora do retorno nativo; não há amplitude para o preenchimento.')
  lo=max(0,index-radius);hi=min(len(raw),index+radius+1);values=raw[lo:hi].copy()
 dn=int(raw[index])
 return {'ping':ping,'channel':channel,'sample':index,'slant_m':index*step,'dn':dn,'db_relative':relative_db(dn),'definition':DB_DEFINITION,'calibrated':False,'values':values.tolist(),'sample_start':lo,'native_sampling_m':step,'date':str(row.get('date','')),'time':str(row.get('time',''))}

class SignatureChart(QWidget):
 def __init__(self):
  super().__init__();self.profile=None;self.setMinimumHeight(98);self.setMaximumHeight(110)
 def set_profile(self,value):self.profile=value;self.update()
 def paintEvent(self,event):
  p=QPainter(self);p.fillRect(self.rect(),QColor('#09111a'));p.setPen(QColor('#ffe0ac'))
  if self.profile is None:p.drawText(8,18,'Passe o mouse sobre o eco: amostras nativas, anteriores aos filtros.');return
  v=self.profile;db='indefinido (DN zero)' if v['db_relative'] is None else f"{v['db_relative']:.2f} dB rel."
  p.drawText(8,17,f"Ping {v['ping']+1} | {v['channel']} | amostra {v['sample']} | {v['slant_m']:.3f} m inclinado | DN {v['dn']}/255 | {db}")
  values=v['values'];width=max(1,self.width()-16)/max(1,len(values));bottom=self.height()-22
  for i,dn in enumerate(values):
   p.fillRect(int(8+i*width),int(bottom-dn/255*48),max(1,int(width)-1),max(1,int(dn/255*48)),QColor('#e8a257' if v['sample_start']+i==v['sample'] else '#956338'))
  p.setPen(QColor('#9fb7c9'));p.drawText(8,self.height()-5,'DN 0–255 | '+DB_DEFINITION+' | Intensidade não identifica metal/rocha.')
