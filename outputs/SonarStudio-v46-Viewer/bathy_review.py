"""Reversible sounding review and bounded 3D relief preview."""
from pathlib import Path
import json,csv
import numpy as np
from PySide6.QtCore import Qt,QAbstractTableModel,QModelIndex
from PySide6.QtWidgets import QDialog,QVBoxLayout,QHBoxLayout,QTableView,QPushButton,QLabel,QDoubleSpinBox,QFileDialog,QInputDialog

def load_edits(recording):
    path=recording.project/'bathymetry-edits.json'
    if not path.exists():return {'pings':{},'water_levels':[]}
    data=json.loads(path.read_text(encoding='utf-8'))
    if data.get('source_dat')!=str(recording.dat):return {'pings':{},'water_levels':[]}
    return data

def save_edits(recording,data):
    data=dict(data,source_dat=str(recording.dat));target=recording.project/'bathymetry-edits.json';temporary=target.with_suffix('.tmp')
    temporary.write_text(json.dumps(data,indent=2,ensure_ascii=False),encoding='utf-8');temporary.replace(target)

class SoundingModel(QAbstractTableModel):
    def __init__(self,recording):
        super().__init__();self.rec=recording;self.edits=load_edits(recording)
    def rowCount(self,parent=QModelIndex()):return 0 if parent.isValid() else len(self.rec.data)
    def columnCount(self,parent=QModelIndex()):return 0 if parent.isValid() else 5
    def headerData(self,section,orientation,role=Qt.ItemDataRole.DisplayRole):
        if role==Qt.ItemDataRole.DisplayRole:return ['Ping','Tempo (s)','Original (m)','Revisada (m)','Usar'][section] if orientation==Qt.Orientation.Horizontal else section+1
    def data(self,index,role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():return None
        row=index.row();col=index.column();edit=self.edits.get('pings',{}).get(str(row),{})
        if col==4 and role==Qt.ItemDataRole.CheckStateRole:return Qt.CheckState.Unchecked if edit.get('excluded') else Qt.CheckState.Checked
        if role in [Qt.ItemDataRole.DisplayRole,Qt.ItemDataRole.EditRole]:
            d=self.rec.data.iloc[row];value=[row+1,round(float(d.time_s),3),round(float(d.inst_dep_m),3),edit.get('depth_m',round(float(d.inst_dep_m),3)),''][col];return str(value)
    def flags(self,index):
        flags=super().flags(index)
        if index.column()==3:flags|=Qt.ItemFlag.ItemIsEditable
        if index.column()==4:flags|=Qt.ItemFlag.ItemIsUserCheckable
        return flags
    def setData(self,index,value,role=Qt.ItemDataRole.EditRole):
        if not index.isValid():return False
        key=str(index.row());edit=dict(self.edits.get('pings',{}).get(key,{}))
        if index.column()==3 and role==Qt.ItemDataRole.EditRole:
            try:depth=float(str(value).replace(',','.'))
            except ValueError:return False
            if not np.isfinite(depth) or depth<=0 or depth>10000:return False
            edit['depth_m']=depth
        elif index.column()==4 and role==Qt.ItemDataRole.CheckStateRole:edit['excluded']=value not in [Qt.CheckState.Checked,2]
        else:return False
        self.edits.setdefault('pings',{})[key]=edit;save_edits(self.rec,self.edits);self.dataChanged.emit(index,index);return True

class SoundingReview(QDialog):
    def __init__(self,studio):
        super().__init__(studio);self.studio=studio;self.setWindowTitle('Revisão de sondagens e nível da água');self.resize(850,570)
        layout=QVBoxLayout(self);note=QLabel('Edite a profundidade revisada ou desmarque Usar. Ajustes são salvos separadamente.\nRegere grade/isóbatas para aplicar. CSV de água: time_s,water_level_m; tempos no mesmo referencial da gravação.\nA série soma ao nível constante do painel, com interpolação linear e extremos constantes.');note.setWordWrap(True);layout.addWidget(note)
        self.model=SoundingModel(studio.recording);self.table=QTableView();self.table.setModel(self.model);layout.addWidget(self.table)
        row=QHBoxLayout()
        for title,callback in [('Ir ao ping',self.goto),('Restaurar selecionadas',self.restore),('Importar série de nível',self.import_water),('Remover série de nível',self.remove_water)]:
            b=QPushButton(title);b.clicked.connect(callback);row.addWidget(b)
        layout.addLayout(row);self.note=QLabel();layout.addWidget(self.note)
        tides=QHBoxLayout()
        for title,callback in [('Importar maré CSV/JSON (timestamp)',self.import_tides),('Remover tabela de maré',self.remove_tides)]:
            button=QPushButton(title);button.clicked.connect(callback);tides.addWidget(button)
        layout.addLayout(tides)
    def goto(self):
        i=self.table.currentIndex()
        if i.isValid():self.studio.pause_playback();self.studio.set_index(i.row())
    def restore(self):
        rows={i.row() for i in self.table.selectionModel().selectedIndexes()}
        for row in rows:self.model.edits.get('pings',{}).pop(str(row),None)
        save_edits(self.model.rec,self.model.edits);self.model.beginResetModel();self.model.endResetModel()
    def import_water(self):
        path,_=QFileDialog.getOpenFileName(self,'Série temporal de nível da água','','CSV (*.csv)')
        if not path:return
        try:
            with open(path,encoding='utf-8-sig',newline='') as f:values=[[float(r['time_s']),float(r['water_level_m'])] for r in csv.DictReader(f)]
            values=sorted(values)
            if not values or not np.isfinite(values).all() or any(values[i][0]>=values[i+1][0] for i in range(len(values)-1)):raise ValueError('Tempos devem ser únicos, com números finitos.')
            if self.model.edits.get('tide_table'):raise ValueError('Remova a tabela de maré antes de importar outra série de nível.')
            self.model.edits['water_levels']=values;save_edits(self.model.rec,self.model.edits);self.note.setText(f'{len(values)} níveis importados. Regere a batimetria.')
        except Exception as error:self.note.setText(str(error))
    def remove_water(self):
        self.model.edits['water_levels']=[];save_edits(self.model.rec,self.model.edits);self.note.setText('Série removida; permanece o nível constante do painel.')

    def import_tides(self):
        path,_=QFileDialog.getOpenFileName(self,'Tabela de maré: timestamp,height_m','','CSV/JSON (*.csv *.json)')
        if not path:return
        first=self.model.rec.data.iloc[0]
        from tide_datum import suggested_anchor
        anchor,ok=QInputDialog.getText(self,'Relógio da gravação','Instante do primeiro ping, com fuso explícito (confira o relógio do sonar):',text=suggested_anchor(self.model.rec))
        if not ok:return
        datum,ok=QInputDialog.getText(self,'Datum das alturas','Datum/origem das alturas; informar nome não afere o datum:')
        if not ok:return
        try:
            from tide_datum import import_tides,tide_at
            table=import_tides(path,anchor,float(first.time_s),datum)
            tide_at(self.model.rec.data.time_s.to_numpy(),table)
            if self.model.edits.get('water_levels'):raise ValueError('Remova primeiro a série de nível antiga para evitar aplicar maré duas vezes.')
            self.model.edits['tide_table']=table;save_edits(self.model.rec,self.model.edits);self.note.setText('Maré vinculada ao relógio da gravação. Regere grade e isóbatas. Datum informado: '+datum)
        except Exception as error:self.note.setText(str(error))

    def remove_tides(self):
        self.model.edits.pop('tide_table',None);save_edits(self.model.rec,self.model.edits);self.note.setText('Tabela de maré removida; regere a batimetria.')

def relief_data(path,max_side=220):
    import rasterio
    with rasterio.open(path) as ds:
        scale=min(1.,max_side/max(ds.width,ds.height));w=max(2,round(ds.width*scale));h=max(2,round(ds.height*scale))
        a=ds.read(1,out_shape=(h,w),masked=True,resampling=rasterio.enums.Resampling.average).astype(float).filled(np.nan)
        xx,yy=np.meshgrid((np.arange(w)+.5)*(ds.bounds.right-ds.bounds.left)/w,(h-np.arange(h)-.5)*(ds.bounds.top-ds.bounds.bottom)/h)
        return xx,yy,-a,dict(crs=str(ds.crs),origin=[ds.bounds.left,ds.bounds.bottom],shape=[h,w],depth_convention='positive_down_metres')

class ReliefView(QDialog):
    def __init__(self,studio,data):
        super().__init__(studio);self.studio=studio;self.xx,self.yy,self.zz,self.metadata=data;self.setWindowTitle('Batimetria — relevo 3D');self.resize(1050,760)
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg,NavigationToolbar2QT
        self.figure=Figure(facecolor='#142331');self.canvas=FigureCanvasQTAgg(self.figure);self.axes=self.figure.add_subplot(111,projection='3d')
        layout=QVBoxLayout(self);toolbar=NavigationToolbar2QT(self.canvas,self);toolbar.setStyleSheet('background: #b8cad5; color: #101820;');layout.addWidget(toolbar);layout.addWidget(self.canvas,1)
        row=QHBoxLayout();row.addWidget(QLabel('Exagero vertical (apenas visual):'));self.exaggeration=QDoubleSpinBox();self.exaggeration.setRange(1,30);self.exaggeration.setValue(5);row.addWidget(self.exaggeration);b=QPushButton('Exportar PNG');b.clicked.connect(self.export);row.addWidget(b);layout.addLayout(row)
        layout.addWidget(QLabel('Arraste para girar. X/Y em metros a partir da origem local; Z = −profundidade.\nPrévia reduzida, sem preencher áreas sem sondagens. Não é exportação 3D para o GPS.'))
        self.exaggeration.valueChanged.connect(self.draw);self.draw()
    def draw(self):
        elevation,azimuth=self.axes.elev,self.axes.azim;self.axes.clear();self.axes.set_facecolor('#142331')
        from matplotlib import colormaps
        finite=np.isfinite(self.zz)
        if not finite.any():return
        lo,hi=np.nanpercentile(-self.zz,[1,99]);colors=colormaps[self.studio.professional.depth_palette.currentText()](np.clip((-self.zz-lo)/max(hi-lo,1e-6),0,1))
        self.axes.plot_surface(self.xx,self.yy,self.zz,facecolors=colors,rcount=120,ccount=120,linewidth=0,antialiased=False,shade=False)
        span=[max(np.ptp(self.xx),1),max(np.ptp(self.yy),1),max(np.nanmax(self.zz)-np.nanmin(self.zz),1)*self.exaggeration.value()];self.axes.set_box_aspect(span)
        self.axes.set_xlabel('Leste local (m)');self.axes.set_ylabel('Norte local (m)');self.axes.set_zlabel('−Profundidade (m)');self.axes.view_init(elevation,azimuth);self.canvas.draw_idle()
        from matplotlib.ticker import MaxNLocator
        for axis in [self.axes.xaxis,self.axes.yaxis,self.axes.zaxis]:
            axis.label.set_color('#dbe8f2');axis.set_major_locator(MaxNLocator(4));axis.set_pane_color((.1,.16,.21,1))
        self.axes.tick_params(colors='#dbe8f2',labelsize=9);self.axes.set_box_aspect(span,zoom=1.2);self.figure.subplots_adjust(left=.01,right=.93,bottom=.06,top=.99);self.canvas.draw_idle()
    def export(self):
        path,_=QFileDialog.getSaveFileName(self,'Salvar relevo 3D','','PNG (*.png)')
        if path:self.figure.savefig(path,dpi=200,facecolor=self.figure.get_facecolor())


def review_signature(recording):
    import hashlib
    return hashlib.sha256(json.dumps(load_edits(recording),sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
