"""SonarStudio: local Humminbird viewer and PINGMapper desktop workflow."""
from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import sys
import threading
import traceback
import time
from dataclasses import replace

ROOT = pathlib.Path(__file__).resolve().parents[2]
APP_VERSION = '4.4-test.1'
os.environ.setdefault('MPLBACKEND', 'Agg')
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'work' / 'mpl'))
os.environ.setdefault('NUMBA_CACHE_DIR', str(ROOT / 'work' / 'numba-cache'))

import numpy as np
# Resolve SciPy's BLAS/LAPACK DLLs before the GIS/Qt runtimes on Windows.
import skimage.io
import rasterio
from rasterio.windows import Window
from rasterio.enums import Resampling
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QRectF, QPointF
from PySide6.QtGui import QColor, QPainter, QPen, QImage, QPixmap, QPainterPath, QAction, QFontDatabase, QFont, QTransform
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLabel, QPushButton, QFileDialog, QSplitter, QComboBox, QCheckBox, QSlider,
    QDoubleSpinBox, QProgressBar, QPlainTextEdit, QMessageBox, QGroupBox,
    QGraphicsView, QGraphicsScene, QFormLayout, QFrame,QStyle,QScrollArea,QGraphicsItem)

from recording import ROOT, Recording, import_recording, prepare_mapping, cache_project
from image_processing import ImageSettings, enhance, colorize, PALETTES

STYLE = '''
QWidget { background: #111923; color: #dbe8f2; font: 10pt "Segoe UI"; }
QMainWindow { background: #111923; }
QLabel#brand { color: #ffc078; font: 700 20pt "Segoe UI"; }
QLabel#muted { color: #a1b3c4; font-size: 9pt; }
QLabel#metric { font: 600 16pt "Segoe UI"; color: #f1f7fc; }
QGroupBox { border: 1px solid #283848; border-radius: 8px; margin-top: 14px; padding: 13px 8px 8px; }
QGroupBox::title { subcontrol-origin: margin; left: 10px; color: #b5c8da; }
QPushButton { background: #1c2b39; border: 1px solid #354858; border-radius: 6px; padding: 8px 12px; }
QPushButton:hover { background: #294155; }
QPushButton#primary { background: #edac62; color: #241506; border: none; font-weight: 600; }
QPushButton:disabled { color: #8d9eae; background: #14212d; }
QComboBox, QDoubleSpinBox, QSpinBox, QLineEdit { background: #182835; border: 1px solid #354858; border-radius: 4px; padding: 5px; }
QPlainTextEdit { background: #0b1119; border: none; font: 9pt "Consolas"; }
QProgressBar { border: none; border-radius: 3px; background: #20303d; text-align: center; height: 16px; }
QProgressBar::chunk { background: #edac62; border-radius: 3px; }
QTableView { background: #111923; alternate-background-color: #192735; gridline-color: #354858; selection-background-color: #80562e; selection-color: #ffffff; }
QHeaderView::section { background: #253648; color: #e9f0f7; border: 1px solid #354858; padding: 7px; }
QToolTip { background: #253648; color: #ffffff; border: 1px solid #edac62; }
QWidget:focus { outline: none; }
QPushButton:focus, QComboBox:focus, QLineEdit:focus { border: 1px solid #ffc078; }
QSplitter::handle { background: #283848; }
QSlider::groove:horizontal { height: 4px; background: #344a5c; border-radius: 2px; }
QSlider::handle:horizontal { background: #ffc078; width: 13px; margin: -5px 0; border-radius: 6px; }
QStatusBar { background: #152330; color: #9fb7c9; }
'''


def rgba_image(gray, valid=None, gamma=1.0, palette='Âmbar', settings=None):
    rgba = colorize(gray,settings or ImageSettings(gamma=gamma,palette=palette),valid)
    return qimage_rgba(rgba)

def qimage_rgba(rgba):
    rgba = np.ascontiguousarray(rgba)
    return QImage(rgba.data, rgba.shape[1], rgba.shape[0], rgba.strides[0], QImage.Format.Format_RGBA8888).copy()


class Worker(QThread):
    note = Signal(str)
    progress = Signal(object)
    ready = Signal(object)
    failed = Signal(str)

    def __init__(self, function):
        super().__init__()
        self.function = function
        self.stop = threading.Event()

    def run(self):
        try:
            result = self.function(self.note.emit, self.progress.emit, self.stop.is_set)
            self.ready.emit(result)
        except (Exception,SystemExit) as error:
            self.note.emit(traceback.format_exc())
            self.failed.emit(str(error) or 'A operação não terminou. Consulte o registro de operações.')


class MapView(QWidget):
    object_selected=Signal(str)
    selected = Signal(int)
    coordinates = Signal(str)

    def __init__(self):
        super().__init__()
        self.setMinimumSize(400, 120)
        self.setMouseTracking(True)
        self.dataset = None
        self.recording = None
        self.center = np.array([0., 0.])
        self.span = 1000.
        self.gamma = 1.
        self.palette = 'Âmbar'
        self.processing = None
        self.depth_range = (0.,50.)
        self.depth_palette='turbo_r'
        self.depth_fixed_range=None
        self.contours=[]
        self.contours_visible=True
        self.objects={}
        self.image = None
        self.index = 0
        self.track_visible = True
        self.measure = False
        self.measure_points = []
        self.drag = None
        self.was_dragged = False
        self.timer = QTimer(self)
        self.timer.setSingleShot(True)
        self.timer.timeout.connect(self.render_raster)

    def open_raster(self, path):
        new = rasterio.open(path)
        if new.crs is None or not new.crs.is_projected or abs(new.transform.b) >= 1e-8 or abs(new.transform.d) >= 1e-8:
            new.close()
            raise ValueError('Abra um mosaico georreferenciado projetado, sem rotação.')
        if self.recording:
            expected = self.recording.crs
            if new.crs != expected:
                new.close()
                raise ValueError('O CRS do mosaico não coincide com as coordenadas desta gravação.')
        if self.dataset:
            self.dataset.close()
        self.dataset = new
        if new.dtypes[0].startswith('float'):
            thumb=new.read(1,out_shape=(min(512,new.height),min(512,new.width)),masked=True).compressed()
            if len(thumb):self.depth_range=(float(np.percentile(thumb,1)),float(np.percentile(thumb,99)))
            if self.depth_fixed_range:self.depth_range=self.depth_fixed_range
        self.fit()

    def set_recording(self, recording):
        if self.dataset:
            self.dataset.close()
        self.dataset = None
        self.image = None
        self.recording = recording
        self.index = 0
        self.measure_points = []
        self.fit()

    def fit(self):
        if self.dataset:
            b = self.dataset.bounds
            self.center = np.array([(b.left + b.right)/2, (b.bottom + b.top)/2])
            self.span = max(b.top - b.bottom, (b.right - b.left) / max(.1, self.width()/max(1, self.height()))) * 1.08
        elif self.recording and self.recording.tree is not None:
            xy = self.recording.xy[self.recording.valid_indices]
            lo, hi = xy.min(axis=0), xy.max(axis=0)
            self.center = (lo + hi) / 2
            self.span = max(hi[1] - lo[1], (hi[0]-lo[0]) / max(.1, self.width()/max(1, self.height())), 100) * 1.12
        self.refresh()

    def native_zoom(self):
        if self.dataset:
            self.span = self.height() * self.dataset.res[1]
            self.refresh()

    def refresh(self):
        self.timer.start(60)
        self.update()

    def meters_per_pixel(self):
        return self.span / max(1, self.height())

    def screen_to_world(self, point):
        s = self.meters_per_pixel()
        return self.center + np.array([(point.x() - self.width()/2)*s, -(point.y()-self.height()/2)*s])

    def world_to_screen(self, xy):
        s = self.meters_per_pixel()
        return QPointF((xy[0]-self.center[0])/s + self.width()/2, (self.center[1]-xy[1])/s + self.height()/2)

    def render_raster(self):
        if not self.dataset:
            self.image = None
            self.update()
            return
        try:
            width, height = max(1, self.width()), max(1, self.height())
            p0 = self.screen_to_world(QPointF(0, 0))
            p1 = self.screen_to_world(QPointF(width, height))
            inv = ~self.dataset.transform
            c0, r0 = inv * tuple(p0)
            c1, r1 = inv * tuple(p1)
            window = Window(c0, r0, max(1e-9, c1-c0), max(1e-9, r1-r0))
            if self.dataset.count==4:
                bands=self.dataset.read(window=window,out_shape=(4,height,width),boundless=True,resampling=Resampling.bilinear)
                self.image=self.raster_image(bands)
            else:
                data = self.dataset.read(1, window=window, out_shape=(height, width), boundless=True, masked=True, resampling=Resampling.bilinear)
                self.image=self.raster_image(data)
        except Exception as error:
            self.coordinates.emit(f'Falha ao ler o mosaico: {error}')
            self.image = None
        self.update()

    def raster_image(self,data):
        if data.ndim==3:return qimage_rgba(np.moveaxis(data,0,-1))
        valid=~np.ma.getmaskarray(data)
        if self.dataset.dtypes[0].startswith('float'):
            lo,hi=self.depth_range;x=np.clip((data.filled(lo)-lo)/max(.01,hi-lo),0,1)
            from matplotlib import colormaps
            colors=(colormaps[self.depth_palette](x)*255).astype(np.uint8);colors[...,3]=valid.astype('uint8')*255
            return qimage_rgba(colors)
        options=replace(self.processing,gamma=self.gamma,palette=self.palette) if self.processing else None
        return rgba_image(data.filled(0),valid,self.gamma,self.palette,options)

    def resizeEvent(self, event):
        self.refresh()

    def paintEvent(self, event):
        p = QPainter(self)
        p.fillRect(self.rect(), QColor('#142331'))
        if self.image:
            p.drawImage(self.rect(), self.image)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        if self.contours_visible:
            key=(id(self.contours),tuple(self.center),self.span,self.width(),self.height())
            if getattr(self,'contour_cache_key',None)!=key:
                self.contour_cache=[]
                for depth,points in self.contours:
                    path=QPainterPath()
                    for i,point in enumerate(points):
                        screen=self.world_to_screen(point)
                        path.moveTo(screen) if i==0 else path.lineTo(screen)
                    self.contour_cache.append((depth,path,path.pointAtPercent(.5)))
                self.contour_cache_key=key
            occupied=set()
            for depth,path,mid in self.contour_cache:
                major=abs(depth/5-round(depth/5))<1e-6;p.setPen(QPen(QColor('#e5f0c9'),1.3 if major else .6))
                p.drawPath(path)
                cell=(int(mid.x()//85),int(mid.y()//30))
                if major and self.rect().contains(mid.toPoint()) and cell not in occupied:
                    p.drawText(mid,f'{depth:g} m');occupied.add(cell)
        if self.recording and self.track_visible:
            key=(id(self.recording),tuple(self.center),self.span,self.width(),self.height())
            if getattr(self,'track_cache_key',None)!=key:
                xy=self.recording.xy;stride=max(1,len(xy)//4500);path=QPainterPath();connected=False
                for point in xy[::stride]:
                    if not np.isfinite(point).all():connected=False;continue
                    screen=self.world_to_screen(point)
                    if connected:path.lineTo(screen)
                    else:path.moveTo(screen);connected=True
                self.track_cache=path;self.track_cache_key=key
            p.setPen(QPen(QColor('#4ce0c0'), 1.0))
            p.drawPath(self.track_cache)
        if self.recording and self.index < len(self.recording.xy):
            marker = self.world_to_screen(self.recording.xy[self.index])
            p.setBrush(QColor('#ffe18b'))
            p.setPen(QPen(QColor('#122332'), 2))
            p.drawEllipse(marker, 6, 6)
        for obj in self.objects.values():
            if obj.get('status')=='rejeitado' or obj.get('in_water_column'):continue
            point=self.world_to_screen((obj['east'],obj['north']))
            p.setPen(QPen(QColor('#ffc078' if obj.get('status')=='confirmado' else '#ffaa50'),1.5));p.setBrush(Qt.BrushStyle.NoBrush);p.drawRect(QRectF(point.x()-4,point.y()-4,8,8))
        if self.measure_points:
            p.setPen(QPen(QColor('#ffa55e'), 2, Qt.PenStyle.DashLine))
            for point in self.measure_points:
                p.drawEllipse(self.world_to_screen(point), 4, 4)
            if len(self.measure_points) == 2:
                a, b = map(self.world_to_screen, self.measure_points)
                p.drawLine(a, b)
                distance = float(np.linalg.norm(self.measure_points[1]-self.measure_points[0]))
                p.drawText((a+b)/2 + QPointF(5, -8), f'{distance:.2f} m')
        if not self.recording and not self.dataset:
            p.setPen(QColor('#9ab0c3'))
            p.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, 'Abra uma gravação .DAT para começar\n\nMapa, profundidade e ecos brutos em uma janela')
        scale = self.meters_per_pixel()
        length = 10 ** math.floor(math.log10(max(1e-6, scale*100)))
        pixels = length/scale
        p.setPen(QPen(QColor('#ffffff'), 2))
        p.drawLine(QPointF(20, self.height()-24), QPointF(20+pixels, self.height()-24))
        p.drawText(QPointF(20, self.height()-34), f'{length:g} m  •  {scale:.3f} m/pixel na tela')
        p.drawText(QPointF(self.width()-25, 25), 'N')
        if self.dataset and self.dataset.dtypes[0].startswith('float'):
            p.drawText(QPointF(20,24),'Profundidade (m) · positiva para baixo')
            from matplotlib import colormaps
            ramp=(colormaps[self.depth_palette](np.linspace(0,1,160))*255).astype('uint8')[:,None,:]
            p.drawImage(QRectF(self.width()-82,52,14,160),qimage_rgba(ramp))
            p.drawText(QPointF(self.width()-65,64),f'{self.depth_range[0]:.1f}')
            p.drawText(QPointF(self.width()-65,211),f'{self.depth_range[1]:.1f}')
            p.drawText(QPointF(self.width()-85,231),'Escala (m)')
        p.drawLine(QPointF(self.width()-35, 27), QPointF(self.width()-35, 12))
        p.drawLine(QPointF(self.width()-35, 12), QPointF(self.width()-39, 17))
        p.drawLine(QPointF(self.width()-35, 12), QPointF(self.width()-31, 17))

    def wheelEvent(self, event):
        before = self.screen_to_world(event.position())
        factor = .78 if event.angleDelta().y() > 0 else 1/.78
        self.span = min(1e7, max(.01, self.span*factor))
        self.center += before-self.screen_to_world(event.position())
        self.refresh()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.drag = event.position()
            self.was_dragged = False

    def mouseMoveEvent(self, event):
        if self.drag is not None and event.buttons() & Qt.MouseButton.LeftButton:
            delta = event.position()-self.drag
            self.center += np.array([-delta.x(), delta.y()])*self.meters_per_pixel()
            self.drag = event.position()
            self.was_dragged = True
            self.refresh()
        xy = self.screen_to_world(event.position())
        self.coordinates.emit(f'E {xy[0]:,.2f} m   N {xy[1]:,.2f} m')

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton and not self.was_dragged:
            xy = self.screen_to_world(event.position())
            if self.measure:
                if len(self.measure_points) == 2:
                    self.measure_points = []
                self.measure_points.append(xy)
                self.update()
            elif self.recording:
                for obj in self.objects.values():
                    if obj.get('status')=='rejeitado' or obj.get('in_water_column'):continue
                    if obj.get('east') is None or obj.get('north') is None:continue
                    point=self.world_to_screen(np.array([obj['east'],obj['north']]))
                    if np.hypot(point.x()-event.position().x(),point.y()-event.position().y())<=10:
                        self.object_selected.emit(obj['id']);self.drag=None;return
                nearest = self.recording.nearest(*xy)
                if nearest:
                    self.selected.emit(nearest[0])
        self.drag = None


class SonarView(QGraphicsView):
    def __init__(self):
        super().__init__()
        self.image_item=None;self.image=None;self.fill_view=True;self.auto_fit=True;self._fitting=False;self.overlay_items=[]
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setScene(QGraphicsScene(self))
        self.setBackgroundBrush(QColor('#060d13'))
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.image_item = None
        self.image = None

    def display(self, array, gamma, palette, y_scale=1.0, settings=None,x_scale=1.0,reverse=False):
        self.image = rgba_image(array, gamma=gamma, palette=palette,settings=settings)
        size_changed=self.image_item is None or self.image_item.pixmap().size()!=self.image.size()
        if self.image_item is None:self.image_item=self.scene().addPixmap(QPixmap.fromImage(self.image))
        else:self.image_item.setPixmap(QPixmap.fromImage(self.image))
        ys=min(100,max(.01,y_scale));xs=min(100,max(.01,x_scale))
        self.image_item.setTransform(QTransform.fromScale(xs,-ys if reverse else ys))
        self.image_item.setPos(0,self.image.height()*ys if reverse else 0)
        self.scene().setSceneRect(self.image_item.sceneBoundingRect())
        if size_changed and self.auto_fit:self.fit()
        for item in getattr(self,'overlay_items',[]):self.scene().removeItem(item)
        self.overlay_items=[]

    def overlays(self,items,ys=1.,xs=1.,cursor=None,downscan=False):
        for item in getattr(self,'overlay_items',[]):self.scene().removeItem(item)
        self.overlay_items=[]
        for obj in items:
            x0,y0,x1,y1=obj['box'];pen=QPen(QColor('#ffc078' if obj.get('status')=='confirmado' else '#ffb65e'),2);pen.setCosmetic(True);rect=self.scene().addRect(QRectF(x0*xs,y0*ys,(x1-x0)*xs,(y1-y0)*ys),pen)
            self.overlay_items.append(rect)
            text=self.scene().addSimpleText(f'{obj.get("label","Objeto")} {obj.get("score",0):.2f}');text.setBrush(QColor('#ffe0ac'));text.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIgnoresTransformations);text.setPos(x0*xs,y0*ys);self.overlay_items.append(text)
        if cursor is not None and self.image:
            pen=QPen(QColor('#63dfd0'),1);pen.setCosmetic(True)
            if not downscan:line=self.scene().addLine(0,cursor*ys,self.image_item.sceneBoundingRect().width(),cursor*ys,pen)
            else:line=self.scene().addLine(cursor*xs,0,cursor*xs,self.image_item.sceneBoundingRect().height(),pen)
            self.overlay_items.append(line)

    def fit(self):
        if getattr(self,'image_item',None) and not self._fitting:
            self._fitting=True
            try:
                self.auto_fit=True
                self.fitInView(self.image_item, Qt.AspectRatioMode.IgnoreAspectRatio if self.fill_view else Qt.AspectRatioMode.KeepAspectRatio)
            finally:self._fitting=False

    def native_zoom(self):
        self.auto_fit=False;self.resetTransform()

    def resizeEvent(self,event):
        super().resizeEvent(event)
        if getattr(self,'auto_fit',False):self.fit()

    def wheelEvent(self, event):
        self.auto_fit=False
        factor = 1.25 if event.angleDelta().y() > 0 else .8
        self.scale(factor, factor)


class DepthView(QWidget):
    picked = Signal(int)

    def __init__(self):
        super().__init__()
        self.setFixedHeight(74)
        self.recording = None
        self.index = 0

    def paintEvent(self, event):
        p = QPainter(self)
        p.fillRect(self.rect(), QColor('#11202d'))
        if not self.recording:
            return
        d = self.recording.data.inst_dep_m.to_numpy();key=(id(self.recording),self.width(),self.height())
        if getattr(self,'profile_key',None)!=key:
            stride=max(1,len(d)//max(1,self.width()));lo,hi=np.nanmin(d),np.nanmax(d);path=QPainterPath()
            for j,i in enumerate(range(0,len(d),stride)):
                point=QPointF(i/max(1,len(d)-1)*self.width(),10+(d[i]-lo)/max(1,hi-lo)*(self.height()-24))
                path.moveTo(point) if j==0 else path.lineTo(point)
            self.profile_key=key;self.profile_path=path;self.profile_range=(lo,hi)
        path=self.profile_path;lo,hi=self.profile_range
        p.setPen(QPen(QColor('#62bee9'), 1.4))
        p.drawPath(path)
        x = self.index/max(1, len(d)-1)*self.width()
        p.setPen(QPen(QColor('#ffe18b'), 2))
        p.drawLine(QPointF(x, 0), QPointF(x, self.height()))
        p.setPen(QColor('#91afc5'))
        p.drawText(QPointF(8, 13), f'Profundidade do aparelho • {lo:.1f}–{hi:.1f} m')

    def mousePressEvent(self, event):
        if self.recording:
            self.picked.emit(int(np.clip(event.position().x()/max(1, self.width()), 0, 1)*(len(self.recording.data)-1)))


class Studio(QMainWindow):
    def __init__(self, autoload=True):
        super().__init__()
        self.setWindowTitle(f'SonarStudio {APP_VERSION} — Humminbird / PINGMapper')
        self.resize(1450, 950)
        self.setMinimumSize(1000, 650)
        self.recording = None
        self.worker = None
        self.last_mosaic = None
        self.raw_array = None
        self.processing = ImageSettings()
        self.bathy_result = None
        self.sidescan_path=None
        self.bathymetry_path=None
        self.sonar_x_scale=1.
        self.sonar_worker=None
        self.sonar_pending=None
        self.sonar_generation=0
        self.sonar_is_down=False
        self.frame_info=None
        self.sonar_crop=None
        self.frame_index=0
        self.last_render_ms=0.
        self.objects={}
        self.ai_worker=None
        self.ai_epoch=0
        self.last_ai_time=0.
        self.play_times=None
        self._play_time=0.
        self._play_last=0.
        self._advancing=False
        self.sonar_y_scale = 1.0
        self.pending_project = None
        self.close_when_finished = False
        self.setStyleSheet(STYLE)
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(20, 14, 20, 10)
        header = QHBoxLayout()
        brand = QLabel('SONARSTUDIO'); brand.setObjectName('brand')
        header.addWidget(brand)
        subtitle = QLabel('GRAVAÇÕES • SONAR LATERAL • MAPAS'); subtitle.setObjectName('muted')
        header.addWidget(subtitle); header.addStretch()
        self.open_button = QPushButton('Abrir .DAT'); self.open_button.setObjectName('primary'); self.open_button.clicked.connect(self.choose_dat)
        header.addWidget(self.open_button)
        mosaic_button = QPushButton('Abrir mosaico'); mosaic_button.clicked.connect(self.choose_mosaic); header.addWidget(mosaic_button)
        layout.addLayout(header)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(splitter, 1)
        sidebar = QWidget(); sidebar.setMinimumWidth(240); sidebar.setMaximumWidth(315)
        side = QVBoxLayout(sidebar); side.setContentsMargins(0, 0, 12, 0)
        self.name = QLabel('Nenhuma gravação'); self.name.setObjectName('metric'); side.addWidget(self.name)
        self.summary = QLabel('Selecione o .DAT extraído\ncom a pasta .SON ao lado.'); self.summary.setWordWrap(True); self.summary.setObjectName('muted'); side.addWidget(self.summary)
        group = QGroupBox('VISUALIZAÇÃO'); form = QFormLayout(group)
        self.channel = QComboBox(); self.channel.currentIndexChanged.connect(self.schedule_sonar); form.addRow('Canal', self.channel)
        self.palette = QComboBox(); self.palette.addItems(PALETTES); self.palette.setCurrentText('Âmbar'); self.palette.currentIndexChanged.connect(self.change_display); form.addRow('Paleta', self.palette)
        self.gamma = QSlider(Qt.Orientation.Horizontal); self.gamma.setRange(25, 300); self.gamma.setValue(150); self.gamma.valueChanged.connect(self.change_display); form.addRow('Gamma', self.gamma)
        self.water = QCheckBox('Remover coluna d’água'); self.water.toggled.connect(self.schedule_sonar); form.addRow(self.water)
        self.track = QCheckBox('Mostrar trajetória'); self.track.setChecked(True); self.track.toggled.connect(self.toggle_track); form.addRow(self.track)
        side.addWidget(group)
        group = QGroupBox('GERAR MOSAICO'); form = QFormLayout(group)
        self.resolution = QComboBox(); self.resolution.addItems(['Nativa — maior detalhe', '5 cm/pixel', '10 cm/pixel', '25 cm/pixel', '1 m/pixel']); form.addRow('Resolução', self.resolution)
        self.temperature = QDoubleSpinBox(); self.temperature.setRange(-2, 45); self.temperature.setSuffix(' °C'); self.temperature.setValue(10); form.addRow('Temperatura', self.temperature)
        self.temperature_note = QLabel('10 °C é o padrão inicial. Ajuste para\na temperatura da água no levantamento.'); self.temperature_note.setObjectName('muted'); self.temperature_note.setWordWrap(True); form.addRow(self.temperature_note)
        self.generate_button = QPushButton('Gerar mosaico'); self.generate_button.clicked.connect(self.generate_mosaic); self.generate_button.setEnabled(False); form.addRow(self.generate_button)
        self.cancel_button = QPushButton('Cancelar operação'); self.cancel_button.clicked.connect(self.cancel_job); self.cancel_button.setEnabled(False); form.addRow(self.cancel_button)
        self.progress = QProgressBar(); self.progress.setRange(0, 100); form.addRow(self.progress)
        side.addWidget(group)
        self.info = QLabel(''); self.info.setWordWrap(True); side.addWidget(self.info)
        advanced=QPushButton('Imagem • Batimetria • GIS');advanced.clicked.connect(self.show_professional);side.addWidget(advanced)
        export = QPushButton('Exportar sondagens CSV'); export.clicked.connect(self.export_csv); side.addWidget(export)
        save = QPushButton('Salvar projeto'); save.clicked.connect(self.save_project); side.addWidget(save)
        load = QPushButton('Abrir projeto'); load.clicked.connect(self.open_project); side.addWidget(load)
        side.addStretch()
        self.log = QPlainTextEdit(); self.log.setReadOnly(True); self.log.setMaximumHeight(140); self.log.setPlaceholderText('Registro de operações'); side.addWidget(self.log)
        side_scroll=QScrollArea();side_scroll.setWidgetResizable(True);side_scroll.setWidget(sidebar);side_scroll.setMinimumWidth(265);side_scroll.setMaximumWidth(335);side_scroll.setFrameShape(QFrame.Shape.NoFrame);splitter.addWidget(side_scroll)
        right = QWidget(); content = QVBoxLayout(right); content.setContentsMargins(0, 0, 0, 0)
        controls = QHBoxLayout()
        self.map_layer=QComboBox();self.map_layer.addItems(['Sidescan Mosaic','Batimetria']);self.map_layer.currentIndexChanged.connect(self.change_map_layer);controls.addWidget(self.map_layer)
        self.contour_toggle=QCheckBox('Isóbatas');self.contour_toggle.toggled.connect(self.toggle_contours);controls.addWidget(self.contour_toggle)
        fit = QPushButton('Enquadrar'); fit.clicked.connect(lambda: self.map.fit()); controls.addWidget(fit)
        native = QPushButton('Zoom 1:1'); native.clicked.connect(lambda: self.map.native_zoom()); controls.addWidget(native)
        self.measure_button = QPushButton('Medir'); self.measure_button.setCheckable(True); self.measure_button.toggled.connect(self.toggle_measure); controls.addWidget(self.measure_button)
        crop = QPushButton('Exportar área PNG'); crop.clicked.connect(self.export_map_area); controls.addWidget(crop)
        controls.addStretch(); self.map_label = QLabel('Sem mosaico'); self.map_label.setObjectName('muted'); content.addLayout(controls);content.addWidget(self.map_label)
        vertical = QSplitter(Qt.Orientation.Vertical);self.vertical_splitter=vertical
        self.map = MapView(); self.map.object_selected.connect(self.inspect_object); self.map.selected.connect(self.set_index); self.map.coordinates.connect(self.statusBar().showMessage); vertical.addWidget(self.map)
        self.map.contours_visible=False
        bottom = QWidget(); bl = QVBoxLayout(bottom); bl.setContentsMargins(0, 8, 0, 0)
        sonar_controls = QHBoxLayout();self.sonar_header=QLabel('SONAR VIEWER');sonar_controls.addWidget(self.sonar_header)
        expand=QPushButton('Expandir');expand.setCheckable(True);expand.toggled.connect(lambda checked:self.vertical_splitter.setSizes([0,1000] if checked else [320,440]));sonar_controls.addWidget(expand)
        sonar_fit = QPushButton('Enquadrar sonar'); sonar_fit.clicked.connect(lambda: self.sonar.fit()); sonar_controls.addWidget(sonar_fit)
        sonar_native = QPushButton('Pixels 1:1'); sonar_native.clicked.connect(self.native_sonar); sonar_controls.addWidget(sonar_native)
        shot = QPushButton('Exportar trecho PNG'); shot.clicked.connect(self.export_sonar); sonar_controls.addWidget(shot)
        self.cascade_direction=QComboBox();self.cascade_direction.addItem('Cascata: baixo',False);self.cascade_direction.addItem('Cascata: cima',True)
        self.cascade_direction.setToolTip('Inverte somente o sentido vertical do SideScan; mantém bombordo e boreste.');self.cascade_direction.currentIndexChanged.connect(lambda:self.render_sonar() if self.raw_array is not None else None);sonar_controls.addWidget(self.cascade_direction)
        sonar_controls.addStretch(); bl.addLayout(sonar_controls)
        transport=QHBoxLayout()
        self.play_button=QPushButton('Play');self.play_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay));self.play_button.clicked.connect(self.toggle_playback);self.play_button.setShortcut('Space');transport.addWidget(self.play_button)
        pause=QPushButton('Pausa');pause.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPause));pause.clicked.connect(self.pause_playback);transport.addWidget(pause)
        stop=QPushButton('Parar');stop.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaStop));stop.clicked.connect(self.stop_playback);transport.addWidget(stop)
        self.speed=QComboBox()
        for value in [.25,.5,1,2,4,8,16,32]:self.speed.addItem(f'{value:g}×',value)
        self.speed.setCurrentIndex(2);transport.addWidget(self.speed)
        self.play_clock=QLabel('00:00:00 / 00:00:00');transport.addWidget(self.play_clock)
        transport.addStretch();bl.addLayout(transport)
        ai_controls=QHBoxLayout();ai_controls.addWidget(QLabel('IA LOCAL'))
        self.ai_mode=QComboBox();self.ai_mode.addItems(['IA desligada','GhostVision — covos (sonar)','SonarVision — objetos SSS (experimental)']);ai_controls.addWidget(self.ai_mode)
        self.ai_mode.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon);self.ai_mode.setMinimumContentsLength(12);self.ai_mode.setMaximumWidth(230)
        self.ai_threshold=QDoubleSpinBox();self.ai_threshold.setRange(.05,.99);self.ai_threshold.setValue(.35);self.ai_threshold.setSingleStep(.05);self.ai_threshold.setPrefix('Score: ');ai_controls.addWidget(self.ai_threshold)
        self.ai_mode.currentIndexChanged.connect(lambda index:self.ai_threshold.setValue(.5 if index==2 else .35))
        self.ai_quality=QComboBox();self.ai_quality.addItems(['Rápida','Detalhada']);ai_controls.addWidget(self.ai_quality)
        self.ai_continuous=QCheckBox('IA no play');ai_controls.addWidget(self.ai_continuous)
        self.analyze_button=QPushButton('Analisar trecho');self.analyze_button.clicked.connect(self.analyze_frame);ai_controls.addWidget(self.analyze_button)
        objects=QPushButton('Objetos');objects.clicked.connect(self.show_objects);ai_controls.addWidget(objects);ai_controls.addStretch();bl.addLayout(ai_controls)
        self.sonar = SonarView(); self.sonar.setMinimumHeight(100); bl.addWidget(self.sonar, 1)
        self.depth = DepthView(); self.depth.picked.connect(self.set_index); bl.addWidget(self.depth)
        self.slider = QSlider(Qt.Orientation.Horizontal); self.slider.setRange(0, 0); self.slider.valueChanged.connect(self.index_changed); bl.addWidget(self.slider)
        self.ping_label = QLabel('Clique na trajetória ou use o controle para escolher um trecho.'); self.ping_label.setObjectName('muted'); bl.addWidget(self.ping_label)
        bottom.setMinimumHeight(350);vertical.addWidget(bottom); vertical.setSizes([320, 440]); content.addWidget(vertical, 1)
        splitter.addWidget(right); splitter.setSizes([280, 1130])
        self.sonar_timer = QTimer(self); self.sonar_timer.setSingleShot(True); self.sonar_timer.timeout.connect(self.show_sonar)
        self.play_timer=QTimer(self);self.play_timer.setInterval(50);self.play_timer.timeout.connect(self.play_tick)
        from object_review import ObjectReview
        self.object_review=ObjectReview(self)
        from batch_dialog import BatchDialog
        self.batch_dialog=BatchDialog(self)
        from beta_dialog import BetaDialog
        self.beta_dialog=BetaDialog(self)
        beta_action=QAction("Novo levantamento — processar tudo…",self)
        beta_action.triggered.connect(self.beta_dialog.show)
        self.menuBar().addAction(beta_action)
        batch_action=QAction("IA — varredura total / retomar…",self)
        batch_action.triggered.connect(self.batch_dialog.show)
        self.menuBar().addAction(batch_action)
        self.statusBar().showMessage('Roda do mouse: zoom • Arraste: mover • Clique na trajetória: selecionar ping')
        menu = self.menuBar().addMenu('Arquivo')
        for title, callback in [('Abrir .DAT…', self.choose_dat), ('Abrir mosaico…', self.choose_mosaic), ('Salvar projeto…', self.save_project), ('Exportar CSV…', self.export_csv)]:
            action = QAction(title, self); action.triggered.connect(callback); menu.addAction(action)
        from pro_dialog import Professional
        self.professional=Professional(self)
        screen=QApplication.primaryScreen()
        if screen:self.resize(min(1450,screen.availableGeometry().width()-20),min(950,screen.availableGeometry().height()-50))
        action=QAction('Imagem, batimetria e exportações GIS…',self);action.triggered.connect(self.show_professional);self.menuBar().addAction(action)
        self.map.palette='Âmbar'
        self.map.gamma=self.gamma.value()/100
        if autoload:
            self.statusBar().showMessage('Use Novo levantamento — processar tudo para importar seu DAT e gerar os produtos.')

    def notify_error(self, text):
        self.pending_project = None
        self.log.appendPlainText(text)
        QMessageBox.critical(self, 'SonarStudio', text[-1600:])

    def run_job(self, function, ready):
        if self.worker and self.worker.isRunning():
            QMessageBox.information(self, 'Operação em andamento', 'Aguarde ou cancele a operação atual.')
            return
        self.worker = Worker(function)
        self.worker.note.connect(self.log.appendPlainText)
        self.worker.progress.connect(self.job_progress)
        self.worker.ready.connect(ready)
        self.worker.failed.connect(self.notify_error)
        self.worker.finished.connect(self.job_finished)
        self.open_button.setEnabled(False); self.generate_button.setEnabled(False); self.cancel_button.setEnabled(True)
        self.progress.setRange(0, 0)
        self.worker.start()

    def job_finished(self):
        self.open_button.setEnabled(True); self.generate_button.setEnabled(self.recording is not None and any(k.startswith('ss_port') for k in self.recording.channels)); self.cancel_button.setEnabled(False)
        self.progress.setRange(0, 100)
        if self.close_when_finished:
            QTimer.singleShot(0,self.close)

    def job_progress(self, value):
        self.progress.setRange(0, 100)
        if value.get('stage') == 'rectification':
            self.progress.setValue(int(value['done']/max(1, value['total'])*100))
            self.log.appendPlainText(f"{value['channel']} · bloco {value['done']}/{value['total']}")
        else:
            self.progress.setValue(int(value.get('percent', 0)))

    def cancel_job(self):
        if self.worker:
            self.worker.stop.set(); self.log.appendPlainText('Cancelamento solicitado; aguarde o bloco atual terminar.')

    def choose_dat(self):
        path, _ = QFileDialog.getOpenFileName(self, 'Abrir gravação Humminbird', str(ROOT/'work/input'), 'Humminbird (*.DAT *.dat)')
        if path:
            self.load_dat(path)

    def load_dat(self, dat, project=None):
        self.pause_playback()
        self.sonar_generation+=1;self.sonar_pending=None;self.sonar_timer.stop()
        self.ai_epoch+=1
        self.batch_dialog.cancel()
        if self.ai_worker and self.ai_worker.isRunning():self.ai_worker.stop.set()
        temperature = self.temperature.value()
        self.run_job(lambda note, progress, cancel: import_recording(dat, project, note, temperature), self.recording_loaded)

    def recording_loaded(self, recording):
        busy=[w for w in [self.sonar_worker,self.object_review.crop_worker] if w and w.isRunning()]
        if busy:
            QTimer.singleShot(30,lambda:self.recording_loaded(recording));return
        if self.recording:
            self.recording.close()
        self.sidescan_path=None;self.bathymetry_path=None;self.bathy_result=None;self.map.contours=[]
        self.last_mosaic=None;self.map_label.setText("Nenhum mosaico gerado para esta gravação")
        self.raw_array=None;self.frame_info=None;self.sonar_crop=None
        self.objects={};self.map.objects=self.objects;self.object_review.removed={}
        self.recording = recording
        self.batch_dialog.recording_changed()
        self.map.set_recording(recording)
        self.depth.recording = recording
        self.channel.blockSignals(True); self.channel.clear()
        if any(k.startswith('ss_port') for k in recording.channels) and any(k.startswith('ss_star') for k in recording.channels):
            self.channel.addItem('Lateral combinado', 'both')
        for key in recording.channels:
            label = key.replace('ss_port', 'Bombordo').replace('ss_star', 'Estibordo').replace('ds_highfreq', 'Descendente 200 kHz').replace('ds_vhighfreq', 'Descendente alta frequência')
            self.channel.addItem(label, key)
        self.channel.blockSignals(False)
        d = recording.data
        self.play_times=np.maximum.accumulate(d.time_s.to_numpy(dtype=float))
        objects_file=recording.project/'objects-ai.json'
        if objects_file.exists():
            saved_objects=json.loads(objects_file.read_text(encoding='utf-8'))
            if saved_objects.get('dat')==str(recording.dat):self.objects={o['id']:o for o in saved_objects.get('objects',[])};self.map.objects=self.objects
        self.resolution.setItemText(0, f'Nativa · {d.pixM.median()*100:.2f} cm')
        self.name.setText(recording.dat.stem)
        duration = int(d.time_s.max()-d.time_s.min())
        self.summary.setText(f'{len(recording.channels)} canais · {len(d):,} pings na trajetória\n{duration//3600}h {(duration%3600)//60}min · {d.inst_dep_m.min():.1f}–{d.inst_dep_m.max():.1f} m\nAmostra nativa: {d.pixM.median()*100:.2f} cm')
        self.slider.setRange(0, len(d)-1); self.set_index(0)
        self.log.appendPlainText(f'Gravação aberta: {recording.dat}')
        candidates = [ROOT/'outputs/Rec00001-resolucao-nativa/mosaico-nativo.tif', ROOT/'outputs/mosaico-sonar.tif'] if recording.dat == (ROOT/'work/input/Rec00001.DAT').resolve() else []
        candidates = [recording.project/'mosaico-nativo.tif'] + candidates
        default_bathy=ROOT/'outputs/Rec00001-profissional/v4/batimetria/resultado-batimetria.json'
        if not default_bathy.exists():default_bathy=ROOT/'outputs/Rec00001-profissional/batimetria/resultado-batimetria.json'
        if default_bathy.exists():
            result=json.loads(default_bathy.read_text(encoding='utf-8'))
            if pathlib.Path(result.get('source_dat','')).resolve()==recording.dat:
                self.bathy_result=result;self.bathymetry_path=result['raster'];self.load_contours(result['contours'])
        for path in candidates:
            if path.is_file():
                if path.name == 'mosaico-nativo.tif' and not (path.parent/'resultado.json').exists():
                    continue
                self.open_mosaic(path); break
        if self.pending_project:
            saved = self.pending_project
            self.pending_project = None
            if saved.get('mosaic'):
                self.open_mosaic(saved['mosaic'])
            self.set_index(int(saved.get('selected_ping', 0)))
            self.gamma.setValue(int(saved.get('gamma', 100)))
            self.water.setChecked(bool(saved.get('remove_water', False)))
            self.palette.setCurrentText(saved.get('palette', 'Cinza'))
            self.track.setChecked(bool(saved.get('track_visible', True)))
            self.bathy_result=saved.get('bathymetry_result')
            self.sidescan_path=saved.get('sidescan_path',self.sidescan_path)
            self.bathymetry_path=saved.get('bathymetry_path',self.bathymetry_path)
            if self.bathy_result:self.load_contours(self.bathy_result.get('contours'))
            if saved.get('professional'):self.professional.restore(saved['professional'])
            player=saved.get('player',{})
            self.cascade_direction.setCurrentIndex(player.get('cascade_direction',0))
            speed_index=self.speed.findData(player.get('speed',1.))
            if speed_index>=0:self.speed.setCurrentIndex(speed_index)
            self.ai_mode.setCurrentIndex({'ghostvision':1,'sonarvision':2}.get(player.get('ai_model'),{0:0,1:1,2:0,3:2}.get(player.get('ai_mode',0),0)));self.ai_threshold.setValue(player.get('ai_threshold',.35));self.ai_quality.setCurrentIndex(player.get('ai_quality',0));self.ai_continuous.setChecked(player.get('ai_continuous',False))
        self.temperature.setValue(recording.temperature)

    def set_index(self, index):
        old = self.slider.value()
        index = int(np.clip(index, self.slider.minimum(), self.slider.maximum()))
        self.slider.setValue(index)
        if old == index:
            self.index_changed(index)

    def index_changed(self, index):
        if not self.recording:
            return
        self.map.index = index; self.map.update(); self.depth.index = index; self.depth.update()
        row = self.recording.data.iloc[index]
        if not self._advancing:self._play_time=float(row.time_s);self._play_last=time.monotonic()
        start=float(self.play_times[0]);total=float(self.play_times[-1]-start);elapsed=max(0,float(row.time_s)-start)
        def clock(seconds):
            seconds=int(seconds);return f'{seconds//3600:02d}:{seconds//60%60:02d}:{seconds%60:02d}'
        self.play_clock.setText(clock(elapsed)+' / '+clock(total))
        self.ping_label.setText(f'Ping {index+1:,}/{len(self.recording.data):,}   •   {row.get("time", "")}   •   Profundidade {row.inst_dep_m:.1f} m   •   {row.lat:.6f}, {row.lon:.6f}')
        self.schedule_sonar()

    def native_sonar(self):
        self.pause_playback();self.sonar.native_zoom();self.show_sonar()

    def schedule_sonar(self, *args):
        if hasattr(self, 'sonar_timer'):
            if hasattr(self,'play_timer') and self.play_timer.isActive():
                if not self.sonar_timer.isActive():self.sonar_timer.start(0)
            else:self.sonar_timer.start(90)

    def show_sonar(self):
        if self.recording:
            try:
                side_scan=str(self.channel.currentData()).startswith('ss_') or self.channel.currentData()=='both'
                self.water.setEnabled(side_scan)
                self.analyze_button.setEnabled(side_scan);self.ai_mode.setEnabled(side_scan);self.cascade_direction.setEnabled(side_scan)
                self.sonar_is_down=not side_scan
                fill=self.professional.aspect.currentIndex()==2
                if self.sonar.fill_view!=fill:self.sonar.fill_view=fill;self.sonar.auto_fit=True
                if self.play_timer.isActive():
                    if self.sonar_worker and self.sonar_worker.isRunning():return
                    rec=self.recording;index=self.slider.value();channel=self.channel.currentData();water=self.water.isChecked() and side_scan;count=self.professional.pings.value();aspect=self.professional.aspect.currentIndex();range_mode=self.professional.range_mode.currentIndex()
                    options=replace(self.processing,gamma=self.gamma.value()/100,palette=self.palette.currentText());compare=self.professional.compare.isChecked();limit=self.screen_limit()
                    self.sonar_generation+=1;generation=self.sonar_generation
                    def work(note,progress,cancel):
                        started=time.perf_counter();array,info=rec.waterfall(index,channel,water,count,range_mode='fixed' if range_mode==2 else 'local')
                        from viewer_geometry import display_crop
                        crop=display_crop(info,range_mode)
                        ratio=info['along_track_m']/info['sampling_m'] if aspect==0 else 1.;ys=ratio if side_scan else 1.;xs=1. if side_scan else ratio
                        if not side_scan:array=array.T
                        result=self.process_sonar((array,options,compare,ys,xs,generation,not side_scan,limit,crop))
                        return {'result':result,'array':array,'info':info,'crop':crop,'ys':ys,'xs':xs,'index':index,'ms':(time.perf_counter()-started)*1000}
                    self.sonar_worker=Worker(work);self.sonar_worker.ready.connect(self.present_sonar);self.sonar_worker.failed.connect(self.log.appendPlainText);self.sonar_worker.finished.connect(self.sonar_filter_finished);self.sonar_worker.start();return
                self.raw_array, info = self.recording.waterfall(self.slider.value(), self.channel.currentData(), self.water.isChecked() and side_scan,self.professional.pings.value(),range_mode='fixed' if self.professional.range_mode.currentIndex()==2 else 'local')
                from viewer_geometry import display_crop
                self.sonar_crop=display_crop(info,self.professional.range_mode.currentIndex())
                self.frame_info=info
                self.frame_index=self.slider.value()
                ratio=info['along_track_m']/info['sampling_m'] if self.professional.aspect.currentIndex()==0 else 1.
                self.sonar_y_scale = ratio if side_scan else 1.
                self.sonar_x_scale = 1. if side_scan else ratio
                if not side_scan:self.raw_array=self.raw_array.T
                self.render_sonar()
            except Exception as error:
                self.log.appendPlainText(f'Erro no trecho de sonar: {error}')

    def change_display(self, *args):
        self.map.gamma = self.gamma.value()/100; self.map.palette = self.palette.currentText(); self.map.refresh()
        if self.raw_array is not None:
            self.render_sonar()

    def show_professional(self):
        self.professional.show();self.professional.raise_();self.professional.activateWindow()

    def render_sonar(self):
        options=replace(self.processing,gamma=self.gamma.value()/100,palette=self.palette.currentText())
        self.sonar_generation+=1
        snapshot=(self.raw_array.copy(),options,self.professional.compare.isChecked(),self.sonar_y_scale,self.sonar_x_scale,self.sonar_generation,self.sonar_is_down,self.screen_limit(),self.sonar_crop)
        expensive=options.method in ['Non-local means','Wavelet'] or options.clahe>0 or self.play_timer.isActive()
        if expensive or (self.sonar_worker and self.sonar_worker.isRunning()):
            self.sonar_pending=snapshot
            self.start_sonar_filter()
        else:
            self.present_sonar(self.process_sonar(snapshot))

    @staticmethod
    def process_sonar(snapshot):
        array,options,compare,ys,xs,generation,downscan=snapshot[:7]
        if len(snapshot)>8 and snapshot[8] is not None:
            left,right=snapshot[8];array=array[:,left:right]
        if len(snapshot)>7 and snapshot[7]:
            from PIL import Image
            width,height=snapshot[7];width=max(1,width//(2 if compare else 1));h,w=array.shape
            nw=min(w,width);nh=min(h,height)
            if (nw,nh)!=(w,h):
                array=np.asarray(Image.fromarray(array).resize((nw,nh),Image.Resampling.BOX));xs*=w/nw;ys*=h/nh
        if downscan and options.destripe>0:
            treated=enhance(array.T,options).T
        else:treated=enhance(array,options)
        if compare:
            original=enhance(array,ImageSettings(gamma=options.gamma,palette=options.palette))
            treated=np.concatenate([original,treated],axis=1)
        return treated,options.palette,ys,xs,generation

    def present_sonar(self,result):
        if isinstance(result,dict):
            if result['result'][4]!=self.sonar_generation:return
            self.raw_array=result['array'];self.frame_info=result['info'];self.sonar_crop=result.get('crop');self.sonar_y_scale=result['ys'];self.sonar_x_scale=result['xs'];self.frame_index=result['index'];self.last_render_ms=result['ms'];result=result['result']
        treated,palette,ys,xs,generation=result
        if generation!=self.sonar_generation:return
        self.sonar_header.setText('ORIGINAL | TRATADO' if self.professional.compare.isChecked() else 'SONAR VIEWER')
        self.sonar.display(treated,1.,palette,ys,x_scale=xs,reverse=bool(self.cascade_direction.currentData()) and not self.sonar_is_down)
        if self.sonar.auto_fit:self.sonar.fit()
        self.draw_objects()
        if self.play_timer.isActive() and self.ai_continuous.isChecked() and time.monotonic()-self.last_ai_time>=1.:self.analyze_frame()

    def screen_limit(self):
        if self.sonar.auto_fit or self.play_timer.isActive():
            ratio=self.sonar.devicePixelRatioF();return (max(320,round(self.sonar.viewport().width()*ratio)),max(100,round(self.sonar.viewport().height()*ratio)))
        return None

    def toggle_playback(self):
        if self.play_timer.isActive():self.pause_playback();return
        if self.recording is None:return
        if self.slider.value()>=self.slider.maximum():self.set_index(0)
        self._play_time=float(self.play_times[self.slider.value()]);self._play_last=time.monotonic();self.play_timer.start();self.play_button.setText('Reproduzindo')

    def pause_playback(self):
        if hasattr(self,'play_timer'):self.play_timer.stop()
        if hasattr(self,'play_button'):self.play_button.setText('Play')

    def stop_playback(self):
        self.pause_playback()
        if self.recording:self.set_index(0)

    def play_tick(self):
        if self.recording is None:self.pause_playback();return
        now=time.monotonic();dt=min(.3,max(0,now-self._play_last));self._play_last=now
        self._play_time+=dt*float(self.speed.currentData());index=min(len(self.play_times)-1,int(np.searchsorted(self.play_times,self._play_time)))
        self._advancing=True
        try:self.set_index(index)
        finally:self._advancing=False
        if index==len(self.play_times)-1:self.pause_playback()

    def analyze_frame(self):
        if self.batch_dialog.worker and self.batch_dialog.worker.isRunning():return
        if self.ai_mode.currentIndex()==0 or self.raw_array is None or self.sonar_is_down:return
        if self.ai_worker and self.ai_worker.isRunning():return
        if self.play_timer.isActive() and self.professional.compare.isChecked():
            self.statusBar().showMessage('IA usa os ecos originais; comparação de imagem é somente visual.')
        self.last_ai_time=time.monotonic();info=dict(self.frame_info);rec=self.recording;epoch=self.ai_epoch;mode=self.ai_mode.currentIndex();threshold=self.ai_threshold.value();detailed=self.ai_quality.currentIndex()==1
        center=self.frame_index-info['start'];first=max(0,min(len(self.raw_array)-64,center-32));last=min(len(self.raw_array),first+64)
        array=self.raw_array[first:last].copy();info['start']+=first;info['end']=info['start']+len(array)
        self.statusBar().showMessage('IA analisando o trecho em segundo plano…')
        def work(note,progress,cancel):
            from sonar_ai import detect_yolo,detect_multiclass,locate
            if mode in [1,2]:
                from PIL import Image
                rows=rec.data.iloc[info['start']:info['end']]
                if not info['remove_water']:
                    columns=np.arange(array.shape[1]);part=info['part_width'];channel=info['channel']
                    distance=np.where(columns<part,part-1-columns,columns-part) if channel=='both' else part-1-columns if channel.startswith('ss_port') else columns
                    depths=rows['dep_m' if 'dep_m' in rows else 'inst_dep_m'].to_numpy(dtype=float)
                    array[distance[None,:]*info['sampling_m']<depths[:,None]]=0
                intervals=np.diff(rows.time_s.to_numpy());positive=intervals[intervals>0]
                travel=float(rows.speed_ms.median())*(float(np.median(positive)) if len(positive) else .1)
                ratio=max(.1,min(40.,travel/info['sampling_m'])) if travel>0 else 1.
                physical_height=max(1,round(len(array)*ratio));ratio=physical_height/len(array)
                physical=np.asarray(Image.fromarray(array).resize((array.shape[1],physical_height),Image.Resampling.BILINEAR))
                if mode==1:items,metadata=detect_yolo(physical,threshold,max_tiles=192,cancel=cancel,max_side=8192 if detailed else 4096)
                else:items,metadata=detect_multiclass(physical,threshold,max_tiles=192,cancel=cancel,max_side=2048 if detailed else 1024)
                for item in items:item['box'][1]/=ratio;item['box'][3]/=ratio
                metadata['along_track_resize']=ratio
            else:raise ValueError('Selecione um modelo treinado com sonar.')
            located=locate(items,rec,info)
            if mode in [1,2]:
                metadata['discarded_water_column']=sum(bool(item.get('in_water_column')) for item in located)
                located=[item for item in located if not item.get('in_water_column')]
            for item in located:item.update(frame_start=info['start'],frame_channel=info['channel'],frame_part_width=info['part_width'],frame_water=info['remove_water'],source_dat=str(rec.dat))
            return located,metadata,epoch
        self.ai_worker=Worker(work);self.ai_worker.ready.connect(self.ai_ready);self.ai_worker.note.connect(self.log.appendPlainText);self.ai_worker.failed.connect(self.log.appendPlainText);self.ai_worker.finished.connect(self.ai_finished);self.ai_worker.start()

    def ai_ready(self,result):
        items,metadata,epoch=result
        if epoch!=self.ai_epoch:return
        for item in items:
            old=self.objects.get(item['id'])
            if old and old.get('status')!='pendente':continue
            if not old or item['score']>old['score']:
                if old and old.get('manual_label'):
                    item['label']=old['label'];item['manual_label']=True
                self.objects[item['id']]=item
        self.save_objects();self.map.objects={item['id']:item for item in self.object_review.filtered()};self.draw_objects();self.map.update()
        if self.object_review.isVisible():self.object_review.refresh()
        self.statusBar().showMessage(f'IA: {len(items)} candidatos neste trecho · {metadata["model"]} · confirme em Objetos')

    def ai_finished(self):
        if self.close_when_finished:QTimer.singleShot(0,self.close)

    def save_objects(self):
        if not self.recording:return
        target=self.recording.project/'objects-ai.json'
        temporary=target.with_suffix('.json.tmp')
        temporary.write_text(json.dumps({'dat':str(self.recording.dat),'objects':list(self.objects.values())},indent=2,ensure_ascii=False),encoding='utf-8')
        temporary.replace(target)

    def inspect_object(self,identity):
        item=self.objects.get(identity)
        if not item:return
        self.pause_playback()
        channel=self.channel.findData(item.get("frame_channel",item.get("channel")))
        if channel>=0:self.channel.setCurrentIndex(channel)
        self.water.setChecked(bool(item.get("frame_water",False)))
        self.set_index(int(item["ping"]))
        if item.get("east") is not None and item.get("north") is not None:
            self.map.center=np.array([item["east"],item["north"]]);self.map.refresh()
        if identity in self.object_review.ids:
            selection=self.object_review.table.selectionModel();selection.blockSignals(True)
            self.object_review.table.selectRow(self.object_review.ids.index(identity));selection.blockSignals(False)
        self.show_sonar()

    def draw_objects(self):
        info=self.frame_info
        if info is None:return
        from viewer_geometry import display_box
        visible=[];reverse=bool(self.cascade_direction.currentData()) and not self.sonar_is_down
        height=len(self.raw_array);crop_left=self.sonar_crop[0] if self.sonar_crop else 0
        if not self.sonar_is_down:
            for item in self.objects.values():
                if not self.object_review.matches(item) or item.get('status')=='rejeitado' or item.get('frame_channel')!=info['channel'] or item.get('frame_part_width')!=info['part_width'] or item.get('frame_water')!=info['remove_water']:continue
                if not info['start']<=item['ping']<info['end']:continue
                obj=dict(item);box=list(item['box']);delta=item['frame_start']-info['start'];box[1]+=delta;box[3]+=delta;obj['box']=display_box(box,crop_left,height,reverse);visible.append(obj)
        cursor=self.frame_index-info['start']
        if reverse:cursor=height-1-cursor
        self.sonar.overlays(visible,self.sonar_y_scale,self.sonar_x_scale,cursor,self.sonar_is_down)

    def show_objects(self):
        self.object_review.refresh();self.object_review.show();self.object_review.raise_()

    def start_sonar_filter(self):
        if self.sonar_worker and self.sonar_worker.isRunning():return
        if self.sonar_pending is None or self.close_when_finished:return
        snapshot=self.sonar_pending;self.sonar_pending=None
        self.sonar_worker=Worker(lambda note,progress,cancel:self.process_sonar(snapshot))
        self.sonar_worker.ready.connect(self.present_sonar);self.sonar_worker.failed.connect(self.log.appendPlainText)
        self.sonar_worker.finished.connect(self.sonar_filter_finished);self.sonar_worker.start()

    def sonar_filter_finished(self):
        if self.close_when_finished:QTimer.singleShot(0,self.close)
        else:QTimer.singleShot(0,self.start_sonar_filter)

    def change_map_layer(self,index):
        if not hasattr(self,'map'):return
        path=self.sidescan_path if index==0 else self.bathymetry_path
        if path:self.open_mosaic(path)

    def toggle_contours(self,checked):
        self.map.contours_visible=checked;self.map.update()

    def load_contours(self,path):
        self.map.contours=[]
        if not path or not pathlib.Path(path).is_file():return
        from osgeo import ogr
        ogr.UseExceptions()
        ds=ogr.Open(str(path))
        if ds:
            for feature in ds.GetLayer(0):
                geom=feature.GetGeometryRef()
                if geom and geom.GetPointCount()>1:
                    points=np.array(geom.GetPoints())[:,:2];stride=max(1,len(points)//400)
                    self.map.contours.append((feature.GetField('depth_m'),points[::stride]))
            ds=None
        self.map.update()

    def toggle_track(self, checked):
        self.map.track_visible = checked; self.map.update()

    def toggle_measure(self, checked):
        self.map.measure = checked; self.map.measure_points = []; self.map.update()
        self.statusBar().showMessage('Clique em dois pontos no mapa para medir a distância.' if checked else 'Clique na trajetória para selecionar um ping.')

    def choose_mosaic(self):
        path, _ = QFileDialog.getOpenFileName(self, 'Abrir mosaico georreferenciado', str(ROOT/'outputs'), 'Rasters (*.tif *.tiff *.vrt)')
        if path:
            self.open_mosaic(path)

    def open_mosaic(self, path):
        try:
            self.map.open_raster(path); self.last_mosaic = str(path)
            d = self.map.dataset
            depth=d.dtypes[0].startswith('float')
            if depth:self.bathymetry_path=str(path)
            else:self.sidescan_path=str(path)
            self.map_layer.blockSignals(True);self.map_layer.setCurrentIndex(1 if depth else 0);self.map_layer.blockSignals(False)
            self.map_label.setText(f'{d.width:,} × {d.height:,} px · {d.res[0]:.4f} m/pixel')
            self.log.appendPlainText(f'Mosaico aberto: {path}')
        except Exception as error:
            self.notify_error(str(error))

    def generate_mosaic(self, checked=False, filtered=False):
        if not self.recording:
            return
        project = self.recording.project
        dat = self.recording.dat
        temperature = self.temperature.value()
        if abs(temperature-self.recording.temperature) > .001:
            project = cache_project(dat, temperature)
        levels = [None, .05, .10, .25, 1.0]
        resolution = levels[self.resolution.currentIndex()]
        native = float(self.recording.data.pixM.median())
        resolution = None if resolution is None else max(native, resolution)
        target = project.parent / (project.name + '-mosaico-' + ('nativo' if resolution is None else str(resolution).replace('.', '_')))
        if resolution is None and project.resolve() == (ROOT/'outputs/Rec00001-processado').resolve():
            target = ROOT/'outputs/Rec00001-resolucao-nativa'
        try:depth_limits=self.professional.mosaic_depth_limits()
        except ValueError as error:self.notify_error(str(error));return
        if depth_limits:target=target.with_name(target.name+'-profundidade-'+str(depth_limits[0])+'-'+str(depth_limits[1]))
        treatment=None
        if filtered:
            from dataclasses import asdict
            import hashlib
            treatment=replace(self.professional.image_settings(),gamma=1.,palette='Cinza')
            digest=hashlib.sha256(json.dumps(asdict(treatment),sort_keys=True).encode()).hexdigest()[:10]
            target=target.with_name(target.name+'-tratado-'+digest)
        def work(note, progress, cancel):
            from native_mosaic import rectify_project
            note('Preparando geração do mosaico. Os dados originais serão preservados.')
            rec = import_recording(dat, project, note, temperature)
            try:
                prepared = prepare_mapping(rec, temperature, note)
                return rectify_project(prepared, target, resolution, progress, cancel,treatment,*(depth_limits or (None,None)))
            finally:
                rec.close()
        self.run_job(work, lambda result: self.open_mosaic(result['mosaic']))

    def export_csv(self):
        if not self.recording:
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Exportar sondagens', str(ROOT/'outputs'/f'{self.recording.dat.stem}-sondagens.csv'), 'CSV (*.csv)')
        if path:
            self.recording.export_csv(path); self.log.appendPlainText(f'CSV salvo: {path}')

    def export_sonar(self):
        if self.raw_array is None:
            return
        self.pause_playback()
        path, _ = QFileDialog.getSaveFileName(self, 'Exportar trecho na resolução dos ecos', str(ROOT/'outputs/trecho-sonar.png'), 'PNG (*.png)')
        if path:
            options=replace(self.processing,gamma=self.gamma.value()/100,palette=self.palette.currentText());compare=self.professional.compare.isChecked()
            snapshot=(self.raw_array.copy(),options,compare,1.,1.,0,self.sonar_is_down,None);info=dict(self.frame_info);dat=self.recording.dat;index=self.frame_index;reverse=bool(self.cascade_direction.currentData()) and not self.sonar_is_down
            def work(note,progress,cancel):
                from PIL import Image
                from pro_exports import recipe
                treated=self.process_sonar(snapshot)[0]
                if reverse:treated=treated[::-1].copy()
                if cancel():raise InterruptedError('Exportação cancelada.')
                Image.fromarray(colorize(treated,ImageSettings(palette=options.palette))).save(path)
                recipe(path,dat,options,selected_ping=index,channel=info['channel'],remove_water=info['remove_water'],comparison=compare,georeferenced=False,native_samples=True,cascade_direction='up' if reverse else 'down',display_crop_applied=False)
                return path
            self.run_job(work,lambda result:self.log.appendPlainText(f'Trecho nativo salvo: {result}'))

    def export_map_area(self):
        d = self.map.dataset
        if d is None:
            return
        upper = self.map.screen_to_world(QPointF(0, 0))
        lower = self.map.screen_to_world(QPointF(self.map.width(), self.map.height()))
        inverse = ~d.transform
        c0, r0 = inverse*tuple(upper); c1, r1 = inverse*tuple(lower)
        c0, r0 = max(0, int(math.floor(c0))), max(0, int(math.floor(r0)))
        c1, r1 = min(d.width, int(math.ceil(c1))), min(d.height, int(math.ceil(r1)))
        if c1 <= c0 or r1 <= r0:
            QMessageBox.information(self, 'Exportar área', 'A área visível não cruza o mosaico.'); return
        if (c1-c0)*(r1-r0) > 100_000_000:
            QMessageBox.information(self, 'Exportar área', 'Aproxime o mapa para exportar uma área menor em PNG nativo. O GeoTIFF conserva o mosaico completo.'); return
        path, _ = QFileDialog.getSaveFileName(self, 'Exportar área na resolução do mosaico', str(ROOT/'outputs/area-mosaico-nativa.png'), 'PNG (*.png)')
        if path:
            array = d.read(window=Window(c0,r0,c1-c0,r1-r0)) if d.count==4 else d.read(1, window=Window(c0, r0, c1-c0, r1-r0), masked=True)
            image = self.map.raster_image(array)
            if not image.save(path):
                self.notify_error('Não foi possível salvar o PNG.'); return
            self.log.appendPlainText(f'Área salva em resolução nativa: {path} ({c1-c0} × {r1-r0} pixels)')
            from rasterio.windows import transform as window_transform
            t=window_transform(Window(c0,r0,c1-c0,r1-r0),d.transform)
            center=t*(.5,.5)
            pathlib.Path(path).with_suffix('.pgw').write_text('\n'.join(str(v) for v in [t.a,t.d,t.b,t.e,center[0],center[1]])+'\n',encoding='ascii')
            pathlib.Path(path).with_suffix('.prj').write_text(d.crs.to_wkt(),encoding='utf-8')

    def save_project(self):
        if not self.recording:
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Salvar projeto SonarStudio', str(ROOT/'outputs'/f'{self.recording.dat.stem}.sonar.json'), 'Projeto (*.json)')
        if path:
            data = {'format': 'SonarStudio-v1', 'dat': str(self.recording.dat), 'project': str(self.recording.project), 'mosaic': self.last_mosaic, 'temperature_c': self.temperature.value(), 'selected_ping': self.slider.value(), 'gamma': self.gamma.value(), 'remove_water': self.water.isChecked(), 'palette': self.palette.currentText(), 'track_visible': self.track.isChecked()}
            data['professional']=self.professional.state();data['bathymetry_result']=self.bathy_result
            data['sidescan_path']=self.sidescan_path;data['bathymetry_path']=self.bathymetry_path
            data['player']={'speed':self.speed.currentData(),'cascade_direction':self.cascade_direction.currentIndex(),'ai_mode':self.ai_mode.currentIndex(),'ai_model':['off','ghostvision','sonarvision'][self.ai_mode.currentIndex()],'ai_threshold':self.ai_threshold.value(),'ai_quality':self.ai_quality.currentIndex(),'ai_continuous':self.ai_continuous.isChecked()}
            pathlib.Path(path).write_text(json.dumps(data, indent=2), encoding='utf-8'); self.log.appendPlainText(f'Projeto salvo: {path}')

    def open_project(self):
        path, _ = QFileDialog.getOpenFileName(self, 'Abrir projeto', str(ROOT/'outputs'), 'Projeto (*.json)')
        if path:
            try:
                data = json.loads(pathlib.Path(path).read_text(encoding='utf-8-sig'))
                if data.get('format') != 'SonarStudio-v1':
                    raise ValueError('Arquivo não é um projeto SonarStudio.')
                self.temperature.setValue(float(data.get('temperature_c', 10)))
                self.pending_project = data
                self.load_dat(data['dat'], data['project'])
            except Exception as error:
                self.notify_error(str(error))

    def closeEvent(self, event):
        self.pause_playback()
        if self.batch_dialog.worker and self.batch_dialog.worker.isRunning():
            self.close_when_finished=True;self.batch_dialog.cancel();event.ignore();return
        if self.object_review.crop_worker and self.object_review.crop_worker.isRunning():
            self.close_when_finished=True;self.object_review.crop_worker.stop.set();self.object_review.crop_worker.finished.connect(self.close);event.ignore();return
        if self.ai_worker and self.ai_worker.isRunning():
            self.close_when_finished=True;self.ai_worker.stop.set();event.ignore();return
        if self.sonar_worker and self.sonar_worker.isRunning():
            self.close_when_finished=True;self.sonar_pending=None;event.ignore();return
        if self.worker and self.worker.isRunning():
            self.close_when_finished = True
            self.worker.stop.set(); self.statusBar().showMessage('Cancelando a operação antes de fechar. Aguarde o bloco atual.'); event.ignore(); return
        if self.recording:
            self.recording.close()
        if self.map.dataset:
            self.map.dataset.close()
        event.accept()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--screenshot', default=os.environ.get('SONARSTUDIO_SCREENSHOT'))
    parser.add_argument('--no-autoload', action='store_true')
    args = parser.parse_args()
    if sys.stdout is None or sys.stderr is None:
        log_dir=pathlib.Path(__file__).parent/'logs'
        log_dir.mkdir(exist_ok=True)
        if sys.stdout is None:
            sys.stdout=open(log_dir/'session-output.log','a',encoding='utf-8',buffering=1)
        if sys.stderr is None:
            sys.stderr=open(log_dir/'session-errors.log','a',encoding='utf-8',buffering=1)
    app = QApplication(sys.argv[:1])
    font_path = pathlib.Path(__file__).parent / 'assets' / 'NotoSans.ttf'
    if font_path.exists():
        QFontDatabase.addApplicationFont(str(font_path))
    app.setFont(QFont('Noto Sans', 10))
    app.setApplicationName('SonarStudio')
    global STYLE
    STYLE = STYLE.replace('Segoe UI', 'Noto Sans').replace('Consolas', 'Noto Sans')
    window = Studio(autoload=not args.no_autoload)
    window.show()
    if args.screenshot:
        def capture():
            if window.worker and window.worker.isRunning():
                QTimer.singleShot(500, capture); return
            window.show_sonar(); window.map.render_raster()
            QTimer.singleShot(200, lambda: (window.grab().save(args.screenshot), app.quit()))
        QTimer.singleShot(1200, capture)
    return app.exec()


if __name__ == '__main__':
    raise SystemExit(main())
