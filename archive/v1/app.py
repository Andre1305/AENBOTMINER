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

ROOT = pathlib.Path(__file__).resolve().parents[2]
os.environ.setdefault('MPLBACKEND', 'Agg')
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'work' / 'mpl'))
os.environ.setdefault('NUMBA_CACHE_DIR', str(ROOT / 'work' / 'numba-cache'))

import numpy as np
import rasterio
from rasterio.windows import Window
from rasterio.enums import Resampling
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QRectF, QPointF
from PySide6.QtGui import QColor, QPainter, QPen, QImage, QPixmap, QPainterPath, QAction, QFontDatabase, QFont, QTransform
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLabel, QPushButton, QFileDialog, QSplitter, QComboBox, QCheckBox, QSlider,
    QDoubleSpinBox, QProgressBar, QPlainTextEdit, QMessageBox, QGroupBox,
    QGraphicsView, QGraphicsScene, QFormLayout, QFrame)

from recording import ROOT, Recording, import_recording, prepare_mapping, cache_project

STYLE = '''
QWidget { background: #0e1822; color: #dbe8f2; font: 10pt "Segoe UI"; }
QMainWindow { background: #0e1822; }
QLabel#brand { color: #59e2b1; font: 700 20pt "Segoe UI"; }
QLabel#muted { color: #8297ab; font-size: 9pt; }
QLabel#metric { font: 600 16pt "Segoe UI"; color: #f1f7fc; }
QGroupBox { border: 1px solid #283848; border-radius: 8px; margin-top: 14px; padding: 13px 8px 8px; }
QGroupBox::title { subcontrol-origin: margin; left: 10px; color: #8ba6bc; }
QPushButton { background: #1c2b39; border: 1px solid #354858; border-radius: 6px; padding: 8px 12px; }
QPushButton:hover { background: #294155; }
QPushButton#primary { background: #32c99a; color: #071b15; border: none; font-weight: 600; }
QPushButton:disabled { color: #586b79; background: #14212d; }
QComboBox, QDoubleSpinBox { background: #182835; border: 1px solid #354858; border-radius: 4px; padding: 5px; }
QPlainTextEdit { background: #09121b; border: none; font: 9pt "Consolas"; }
QProgressBar { border: none; border-radius: 3px; background: #20303d; text-align: center; height: 16px; }
QProgressBar::chunk { background: #32c99a; border-radius: 3px; }
QSplitter::handle { background: #283848; }
QSlider::groove:horizontal { height: 4px; background: #344a5c; border-radius: 2px; }
QSlider::handle:horizontal { background: #59e2b1; width: 13px; margin: -5px 0; border-radius: 6px; }
QStatusBar { background: #152330; color: #9fb7c9; }
'''


def rgba_image(gray, valid=None, gamma=1.0, palette='Cinza'):
    values = np.clip(np.asarray(gray, dtype=float) / 255.0, 0, 1) ** (1.0 / max(0.1, gamma))
    if palette == 'Âmbar':
        rgb = np.stack([values, values * .70, values * .31], axis=-1)
    else:
        rgb = np.repeat(values[..., None], 3, axis=-1)
    alpha = np.ones(values.shape) * 255 if valid is None else np.asarray(valid, dtype=np.uint8) * 255
    rgba = np.dstack([np.clip(rgb * 255, 0, 255).astype(np.uint8), alpha.astype(np.uint8)])
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
        except Exception:
            self.failed.emit(traceback.format_exc())


class MapView(QWidget):
    selected = Signal(int)
    coordinates = Signal(str)

    def __init__(self):
        super().__init__()
        self.setMinimumSize(400, 280)
        self.setMouseTracking(True)
        self.dataset = None
        self.recording = None
        self.center = np.array([0., 0.])
        self.span = 1000.
        self.gamma = 1.
        self.palette = 'Cinza'
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
        if new.crs is None or not new.crs.is_projected or new.transform.b >= 1e-8 or abs(new.transform.d) >= 1e-8:
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
            data = self.dataset.read(1, window=window, out_shape=(height, width), boundless=True, masked=True, resampling=Resampling.bilinear)
            self.image = rgba_image(data.filled(0), ~np.ma.getmaskarray(data), self.gamma, self.palette)
        except Exception as error:
            self.coordinates.emit(f'Falha ao ler o mosaico: {error}')
            self.image = None
        self.update()

    def resizeEvent(self, event):
        self.refresh()

    def paintEvent(self, event):
        p = QPainter(self)
        p.fillRect(self.rect(), QColor('#142331'))
        if self.image:
            p.drawImage(self.rect(), self.image)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        if self.recording and self.track_visible:
            xy = self.recording.xy
            stride = max(1, len(xy)//4500)
            path = QPainterPath()
            connected = False
            for point in xy[::stride]:
                if not np.isfinite(point).all():
                    connected = False
                    continue
                screen = self.world_to_screen(point)
                if connected:
                    path.lineTo(screen)
                else:
                    path.moveTo(screen)
                    connected = True
            p.setPen(QPen(QColor('#4ce0c0'), 1.0))
            p.drawPath(path)
        if self.recording and self.index < len(self.recording.xy):
            marker = self.world_to_screen(self.recording.xy[self.index])
            p.setBrush(QColor('#ffe18b'))
            p.setPen(QPen(QColor('#122332'), 2))
            p.drawEllipse(marker, 6, 6)
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
                nearest = self.recording.nearest(*xy)
                if nearest:
                    self.selected.emit(nearest[0])
        self.drag = None


class SonarView(QGraphicsView):
    def __init__(self):
        super().__init__()
        self.setScene(QGraphicsScene(self))
        self.setBackgroundBrush(QColor('#060d13'))
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.image_item = None
        self.image = None

    def display(self, array, gamma, palette, y_scale=1.0):
        self.image = rgba_image(array, gamma=gamma, palette=palette)
        self.scene().clear()
        self.image_item = self.scene().addPixmap(QPixmap.fromImage(self.image))
        self.image_item.setTransform(QTransform.fromScale(1, min(100, max(1, y_scale))))
        self.scene().setSceneRect(self.image_item.sceneBoundingRect())
        self.fit()

    def fit(self):
        if self.image_item:
            self.fitInView(self.image_item, Qt.AspectRatioMode.KeepAspectRatio)

    def wheelEvent(self, event):
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
        d = self.recording.data.inst_dep_m.to_numpy()
        stride = max(1, len(d)//max(1, self.width()))
        lo, hi = np.nanmin(d), np.nanmax(d)
        path = QPainterPath()
        for j, i in enumerate(range(0, len(d), stride)):
            point = QPointF(i/max(1, len(d)-1)*self.width(), 10+(d[i]-lo)/max(1, hi-lo)*(self.height()-24))
            path.moveTo(point) if j == 0 else path.lineTo(point)
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
        self.setWindowTitle('SonarStudio — Humminbird / PINGMapper')
        self.resize(1450, 950)
        self.setMinimumSize(1100, 700)
        self.recording = None
        self.worker = None
        self.last_mosaic = None
        self.raw_array = None
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
        self.palette = QComboBox(); self.palette.addItems(['Cinza', 'Âmbar']); self.palette.currentIndexChanged.connect(self.change_display); form.addRow('Paleta', self.palette)
        self.gamma = QSlider(Qt.Orientation.Horizontal); self.gamma.setRange(25, 300); self.gamma.setValue(100); self.gamma.valueChanged.connect(self.change_display); form.addRow('Contraste', self.gamma)
        self.water = QCheckBox('Remover coluna d’água'); self.water.toggled.connect(self.schedule_sonar); form.addRow(self.water)
        self.track = QCheckBox('Mostrar trajetória'); self.track.setChecked(True); self.track.toggled.connect(self.toggle_track); form.addRow(self.track)
        side.addWidget(group)
        group = QGroupBox('GERAR MOSAICO'); form = QFormLayout(group)
        self.resolution = QComboBox(); self.resolution.addItems(['Nativa — maior detalhe', '5 cm/pixel', '10 cm/pixel', '25 cm/pixel', '1 m/pixel']); form.addRow('Resolução', self.resolution)
        self.temperature = QDoubleSpinBox(); self.temperature.setRange(-2, 45); self.temperature.setSuffix(' °C'); self.temperature.setValue(10); form.addRow('Temperatura', self.temperature)
        self.temperature_note = QLabel('10 °C é o padrão inicial. Ajuste para\na temperatura da água no levantamento.'); self.temperature_note.setObjectName('muted'); form.addRow(self.temperature_note)
        self.generate_button = QPushButton('Gerar mosaico'); self.generate_button.clicked.connect(self.generate_mosaic); self.generate_button.setEnabled(False); form.addRow(self.generate_button)
        self.cancel_button = QPushButton('Cancelar operação'); self.cancel_button.clicked.connect(self.cancel_job); self.cancel_button.setEnabled(False); form.addRow(self.cancel_button)
        self.progress = QProgressBar(); self.progress.setRange(0, 100); form.addRow(self.progress)
        side.addWidget(group)
        self.info = QLabel(''); self.info.setWordWrap(True); side.addWidget(self.info)
        export = QPushButton('Exportar sondagens CSV'); export.clicked.connect(self.export_csv); side.addWidget(export)
        save = QPushButton('Salvar projeto'); save.clicked.connect(self.save_project); side.addWidget(save)
        load = QPushButton('Abrir projeto'); load.clicked.connect(self.open_project); side.addWidget(load)
        side.addStretch()
        self.log = QPlainTextEdit(); self.log.setReadOnly(True); self.log.setMaximumHeight(140); self.log.setPlaceholderText('Registro de operações'); side.addWidget(self.log)
        splitter.addWidget(sidebar)
        right = QWidget(); content = QVBoxLayout(right); content.setContentsMargins(0, 0, 0, 0)
        controls = QHBoxLayout(); controls.addWidget(QLabel('MAPA'))
        fit = QPushButton('Enquadrar'); fit.clicked.connect(lambda: self.map.fit()); controls.addWidget(fit)
        native = QPushButton('Zoom 1:1'); native.clicked.connect(lambda: self.map.native_zoom()); controls.addWidget(native)
        self.measure_button = QPushButton('Medir'); self.measure_button.setCheckable(True); self.measure_button.toggled.connect(self.toggle_measure); controls.addWidget(self.measure_button)
        crop = QPushButton('Exportar área PNG'); crop.clicked.connect(self.export_map_area); controls.addWidget(crop)
        controls.addStretch(); self.map_label = QLabel('Sem mosaico'); self.map_label.setObjectName('muted'); controls.addWidget(self.map_label); content.addLayout(controls)
        vertical = QSplitter(Qt.Orientation.Vertical)
        self.map = MapView(); self.map.selected.connect(self.set_index); self.map.coordinates.connect(self.statusBar().showMessage); vertical.addWidget(self.map)
        bottom = QWidget(); bl = QVBoxLayout(bottom); bl.setContentsMargins(0, 8, 0, 0)
        sonar_controls = QHBoxLayout(); sonar_controls.addWidget(QLabel('SONAR · ECOS BRUTOS'))
        sonar_fit = QPushButton('Enquadrar sonar'); sonar_fit.clicked.connect(lambda: self.sonar.fit()); sonar_controls.addWidget(sonar_fit)
        sonar_native = QPushButton('Pixels 1:1'); sonar_native.clicked.connect(lambda: self.sonar.resetTransform()); sonar_controls.addWidget(sonar_native)
        shot = QPushButton('Exportar trecho PNG'); shot.clicked.connect(self.export_sonar); sonar_controls.addWidget(shot)
        sonar_controls.addStretch(); bl.addLayout(sonar_controls)
        self.sonar = SonarView(); self.sonar.setMinimumHeight(170); bl.addWidget(self.sonar, 1)
        self.depth = DepthView(); self.depth.picked.connect(self.set_index); bl.addWidget(self.depth)
        self.slider = QSlider(Qt.Orientation.Horizontal); self.slider.setRange(0, 0); self.slider.valueChanged.connect(self.index_changed); bl.addWidget(self.slider)
        self.ping_label = QLabel('Clique na trajetória ou use o controle para escolher um trecho.'); self.ping_label.setObjectName('muted'); bl.addWidget(self.ping_label)
        vertical.addWidget(bottom); vertical.setSizes([550, 320]); content.addWidget(vertical, 1)
        splitter.addWidget(right); splitter.setSizes([280, 1130])
        self.sonar_timer = QTimer(self); self.sonar_timer.setSingleShot(True); self.sonar_timer.timeout.connect(self.show_sonar)
        self.statusBar().showMessage('Roda do mouse: zoom • Arraste: mover • Clique na trajetória: selecionar ping')
        menu = self.menuBar().addMenu('Arquivo')
        for title, callback in [('Abrir .DAT…', self.choose_dat), ('Abrir mosaico…', self.choose_mosaic), ('Salvar projeto…', self.save_project), ('Exportar CSV…', self.export_csv)]:
            action = QAction(title, self); action.triggered.connect(callback); menu.addAction(action)
        if autoload:
            dat = ROOT / 'work/input/Rec00001.DAT'; project = ROOT / 'outputs/Rec00001-processado'
            if dat.exists() and (project/'meta/DAT_meta.csv').exists():
                QTimer.singleShot(100, lambda: self.load_dat(dat, project))

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
        temperature = self.temperature.value()
        self.run_job(lambda note, progress, cancel: import_recording(dat, project, note, temperature), self.recording_loaded)

    def recording_loaded(self, recording):
        if self.recording:
            self.recording.close()
        self.recording = recording
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
        self.resolution.setItemText(0, f'Nativa · {d.pixM.median()*100:.2f} cm')
        self.name.setText(recording.dat.stem)
        duration = int(d.time_s.max()-d.time_s.min())
        self.summary.setText(f'{len(recording.channels)} canais · {len(d):,} pings na trajetória\n{duration//3600}h {(duration%3600)//60}min · {d.inst_dep_m.min():.1f}–{d.inst_dep_m.max():.1f} m\nAmostra nativa: {d.pixM.median()*100:.2f} cm')
        self.slider.setRange(0, len(d)-1); self.set_index(0)
        self.log.appendPlainText(f'Gravação aberta: {recording.dat}')
        candidates = [ROOT/'outputs/Rec00001-resolucao-nativa/mosaico-nativo.tif', ROOT/'outputs/mosaico-sonar.tif'] if recording.dat == (ROOT/'work/input/Rec00001.DAT').resolve() else []
        candidates = [recording.project/'mosaico-nativo.tif'] + candidates
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
        self.ping_label.setText(f'Ping {index+1:,}/{len(self.recording.data):,}   •   {row.get("time", "")}   •   Profundidade {row.inst_dep_m:.1f} m   •   {row.lat:.6f}, {row.lon:.6f}')
        self.sonar_timer.start(90)

    def schedule_sonar(self, *args):
        if hasattr(self, 'sonar_timer'):
            self.sonar_timer.start(90)

    def show_sonar(self):
        if self.recording:
            try:
                self.raw_array, info = self.recording.waterfall(self.slider.value(), self.channel.currentData(), self.water.isChecked())
                self.sonar_y_scale = info['along_track_m']/info['sampling_m']
                self.sonar.display(self.raw_array, self.gamma.value()/100, self.palette.currentText(), self.sonar_y_scale)
            except Exception as error:
                self.log.appendPlainText(f'Erro no trecho de sonar: {error}')

    def change_display(self, *args):
        self.map.gamma = self.gamma.value()/100; self.map.palette = self.palette.currentText(); self.map.refresh()
        if self.raw_array is not None:
            self.sonar.display(self.raw_array, self.map.gamma, self.map.palette, self.sonar_y_scale)

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
            self.map_label.setText(f'{d.width:,} × {d.height:,} px · {d.res[0]*100:.2f} cm/pixel')
            self.log.appendPlainText(f'Mosaico aberto: {path}')
        except Exception as error:
            self.notify_error(str(error))

    def generate_mosaic(self):
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
        def work(note, progress, cancel):
            from native_mosaic import rectify_project
            note('Preparando geração do mosaico. Os dados originais serão preservados.')
            rec = import_recording(dat, project, note, temperature)
            try:
                prepared = prepare_mapping(rec, temperature, note)
                return rectify_project(prepared, target, resolution, progress, cancel)
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
        if not self.sonar.image:
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Exportar trecho na resolução dos ecos', str(ROOT/'outputs/trecho-sonar.png'), 'PNG (*.png)')
        if path:
            self.sonar.image.save(path); self.log.appendPlainText(f'Trecho salvo: {path}')

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
            array = d.read(1, window=Window(c0, r0, c1-c0, r1-r0), masked=True)
            image = rgba_image(array.filled(0), ~np.ma.getmaskarray(array), self.map.gamma, self.map.palette)
            if not image.save(path):
                self.notify_error('Não foi possível salvar o PNG.'); return
            self.log.appendPlainText(f'Área salva em resolução nativa: {path} ({c1-c0} × {r1-r0} pixels)')

    def save_project(self):
        if not self.recording:
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Salvar projeto SonarStudio', str(ROOT/'outputs'/f'{self.recording.dat.stem}.sonar.json'), 'Projeto (*.json)')
        if path:
            data = {'format': 'SonarStudio-v1', 'dat': str(self.recording.dat), 'project': str(self.recording.project), 'mosaic': self.last_mosaic, 'temperature_c': self.temperature.value(), 'selected_ping': self.slider.value(), 'gamma': self.gamma.value(), 'remove_water': self.water.isChecked(), 'palette': self.palette.currentText(), 'track_visible': self.track.isChecked()}
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
