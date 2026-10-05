"""Global acoustic preview with a draggable visible-window indicator."""
import numpy as np
from PySide6.QtCore import Signal,Qt,QRectF
from PySide6.QtGui import QPainter,QColor,QPen,QImage
from PySide6.QtWidgets import QWidget

class SonarOverview(QWidget):
 picked=Signal(int)
 def __init__(self):
  super().__init__();self.setFixedWidth(76);self.setMinimumHeight(120);self.total=0;self.image=None;self.start=0;self.end=0;self.drag_offset=0
  self.setToolTip('Gravação inteira: início no topo. Arraste a janela ou clique para saltar. Prévia acústica amostrada, sem caixas de IA.')
 def set_data(self,total,preview=None):
  self.total=total
  if preview is not None:
   a=np.ascontiguousarray(preview,np.uint8);self.image=QImage(a.data,a.shape[1],a.shape[0],a.strides[0],QImage.Format.Format_Grayscale8).copy()
  else:self.image=None
  self.update()
 def set_window(self,start,end):self.start=start;self.end=end;self.update()
 def band(self):
  height=max(1,self.height()-32);top=16+self.start/max(1,self.total)*height;bottom=16+self.end/max(1,self.total)*height
  return QRectF(1,top,self.width()-2,max(4,bottom-top))
 def paintEvent(self,event):
  p=QPainter(self);p.fillRect(self.rect(),QColor('#09111a'));p.setPen(QColor('#b9c7d4'));p.drawText(3,12,'INÍCIO')
  if self.image:p.drawImage(QRectF(1,16,self.width()-2,max(1,self.height()-32)),self.image)
  elif self.total:p.drawText(3,32,'Amostrando…')
  p.fillRect(self.band(),QColor(255,185,95,65));p.setPen(QPen(QColor('#ffbc68'),2));p.drawRect(self.band());p.drawText(3,self.height()-3,'FIM')
 def navigate(self,event):
  if self.total:self.picked.emit(int(np.clip((event.position().y()-16)/max(1,self.height()-32)*self.total-self.drag_offset,0,self.total-1)))
 def mousePressEvent(self,event):
  if event.button()==Qt.MouseButton.LeftButton:
   center=(self.start+self.end)/2;self.drag_offset=(event.position().y()-16)/max(1,self.height()-32)*self.total-center if self.band().contains(event.position()) else 0
   self.navigate(event)
 def mouseMoveEvent(self,event):
  if event.buttons() & Qt.MouseButton.LeftButton:self.navigate(event)
