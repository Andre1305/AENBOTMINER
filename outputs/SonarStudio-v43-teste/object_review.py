"""Human review and export of model suggestions, never silent confirmations."""
from pathlib import Path
import json,xml.etree.ElementTree as ET
from PySide6.QtWidgets import QDialog,QVBoxLayout,QHBoxLayout,QTableView,QPushButton,QFileDialog,QInputDialog,QLabel,QComboBox,QDoubleSpinBox,QLineEdit
from PySide6.QtCore import Qt,QAbstractTableModel,QModelIndex
from recording import ROOT

def export_objects(items,path):
    path=Path(path);items=[i for i in items if i.get('status')!='rejeitado' and not i.get('in_water_column') and i.get('longitude') is not None]
    features=[{'type':'Feature','geometry':{'type':'Point','coordinates':[i['longitude'],i['latitude']]},'properties':{k:v for k,v in i.items() if k not in ['box','longitude','latitude']}} for i in items]
    if path.suffix.lower() in ['.json','.geojson']:
        path.write_text(json.dumps({'type':'FeatureCollection','features':features},indent=2,ensure_ascii=False),encoding='utf-8')
    elif path.suffix.lower()=='.shp':
        from osgeo import ogr,osr
        ogr.UseExceptions();srs=osr.SpatialReference();srs.ImportFromEPSG(4326)
        dst=ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(str(path));layer=dst.CreateLayer(path.stem,srs,ogr.wkbPoint,options=['ENCODING=UTF-8'])
        for name,type_ in [('id',ogr.OFTString),('label',ogr.OFTString),('score',ogr.OFTReal),('status',ogr.OFTString),('ping',ogr.OFTInteger),('pos_q',ogr.OFTString)]:layer.CreateField(ogr.FieldDefn(name,type_))
        for i in items:
            f=ogr.Feature(layer.GetLayerDefn());geom=ogr.Geometry(ogr.wkbPoint);geom.AddPoint_2D(i['longitude'],i['latitude']);f.SetGeometry(geom)
            for key in ['id','label','score','status']:f.SetField(key,i[key])
            f.SetField('ping',i['ping']+1);f.SetField('pos_q','aproximada');layer.CreateFeature(f)
        dst=None
    elif path.suffix.lower()=='.gpx':
        ns='http://www.topografix.com/GPX/1/1';ET.register_namespace('',ns)
        root=ET.Element('{'+ns+'}gpx',version='1.1',creator='SonarStudio')
        for i in items:
            w=ET.SubElement(root,'{'+ns+'}wpt',lat=str(i['latitude']),lon=str(i['longitude']));ET.SubElement(w,'{'+ns+'}name').text=i['label'];ET.SubElement(w,'{'+ns+'}desc').text=f'{i["status"]}; score {i["score"]:.3f}; posição aproximada; ping {i["ping"]+1}'
        ET.ElementTree(root).write(path,encoding='utf-8',xml_declaration=True)
    elif path.suffix.lower()=='.kml':
        ns='http://www.opengis.net/kml/2.2';ET.register_namespace('',ns);root=ET.Element('{'+ns+'}kml');doc=ET.SubElement(root,'{'+ns+'}Document')
        for i in items:
            p=ET.SubElement(doc,'{'+ns+'}Placemark');ET.SubElement(p,'{'+ns+'}name').text=i['label'];ET.SubElement(p,'{'+ns+'}description').text=f'{i["model"]}; score {i["score"]:.3f}; {i["status"]}; posição aproximada'
            point=ET.SubElement(p,'{'+ns+'}Point');ET.SubElement(point,'{'+ns+'}coordinates').text=f'{i["longitude"]},{i["latitude"]},0'
        ET.ElementTree(root).write(path,encoding='utf-8',xml_declaration=True)
    else:raise ValueError('Escolha GeoJSON, Shapefile, GPX ou KML.')
    return len(items)

class ObjectModel(QAbstractTableModel):
    def __init__(self):super().__init__();self.items=[]
    def rowCount(self,parent=QModelIndex()):return 0 if parent.isValid() else len(self.items)
    def columnCount(self,parent=QModelIndex()):return 0 if parent.isValid() else 6
    def headerData(self,section,orientation,role=Qt.ItemDataRole.DisplayRole):
        if role==Qt.ItemDataRole.DisplayRole:return ['Objeto','Score','Ping','Canal','Revisão','Posição'][section] if orientation==Qt.Orientation.Horizontal else section+1
    def data(self,index,role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role!=Qt.ItemDataRole.DisplayRole:return None
        item=self.items[index.row()]
        return [item['label'],f'{item["score"]:.3f}',str(item['ping']+1),item['channel'],item['status'],'Coluna d’água' if item.get('in_water_column') else 'Aproximada'][index.column()]

class ObjectReview(QDialog):
    def __init__(self,studio):
        super().__init__(studio);self.studio=studio;self.setWindowTitle('Objetos — sugestões da IA e revisão');self.resize(900,540)
        layout=QVBoxLayout(self)
        note=QLabel('Covos e modelo multiclasse pré-treinados; classes experimentais exigem revisão. Somente modelos especializados em sonar geram novas identificações.\nPosições aproximadas. Os filtros abaixo controlam a lista, o mapa, o sonar e a exportação.');note.setWordWrap(True);layout.addWidget(note)
        filters=QHBoxLayout();self.search=QLineEdit();self.search.setPlaceholderText('Filtrar nome / modelo / canal');filters.addWidget(self.search)
        self.status=QComboBox();self.status.addItems(['Todas as revisões','pendente','confirmado','rejeitado']);filters.addWidget(self.status)
        self.score=QDoubleSpinBox();self.score.setRange(0,1);self.score.setSingleStep(.05);self.score.setPrefix('Score mín. ');filters.addWidget(self.score);layout.addLayout(filters)
        self.count=QLabel();layout.addWidget(self.count)
        for signal in [self.search.textChanged,self.status.currentIndexChanged,self.score.valueChanged]:signal.connect(self.filter_changed)
        self.table=QTableView();self.model=ObjectModel();self.table.setModel(self.model);self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows);self.table.setEditTriggers(QTableView.EditTrigger.NoEditTriggers);self.table.selectionModel().selectionChanged.connect(lambda *args:self.select());self.table.horizontalHeader().setResizeContentsPrecision(80);layout.addWidget(self.table)
        buttons=QHBoxLayout()
        for label,callback in [('Confirmar',lambda:self.mark('confirmado')),('Rejeitar',lambda:self.mark('rejeitado')),('Nomear objeto',self.name_object),('Exportar objetos',self.export)]:
            b=QPushButton(label);b.clicked.connect(callback);buttons.addWidget(b)
        layout.addLayout(buttons);self.ids=[]
        self.preview=QLabel('Selecione um candidato para inspecionar e tratar somente seu recorte.');self.preview.setMinimumHeight(100);self.preview.setAlignment(Qt.AlignmentFlag.AlignCenter);layout.addWidget(self.preview)
        row=QHBoxLayout();self.crop_filter=QComboBox();self.crop_filter.addItems(['Original','Mediana','Lee — speckle','Non-local means','Wavelet']);row.addWidget(self.crop_filter)
        for name,cb in [('Tratar recorte do objeto',self.filter_crop),('Limpar filtrados',self.clear_filtered),('Limpar lista',self.clear_all),('Desfazer limpeza',self.undo_clear)]:
            b=QPushButton(name);b.clicked.connect(cb);row.addWidget(b)
        layout.addLayout(row);self.removed={};self.crop_worker=None
    def matches(self,item):
        text=self.search.text().strip().casefold()
        return (not text or text in ' '.join(str(item.get(k,'')) for k in ['label','model','channel']).casefold()) and (self.status.currentIndex()==0 or item.get('status')==self.status.currentText()) and item.get('score',0)>=self.score.value()
    def filtered(self):return [i for i in self.studio.objects.values() if self.matches(i)]
    def filter_changed(self,*args):
        self.refresh();self.studio.map.objects={i['id']:i for i in self.filtered()};self.studio.map.update();self.studio.draw_objects()
    def refresh(self):
        selected=self.current();selection=self.table.selectionModel();selection.blockSignals(True);items=sorted(self.filtered(),key=lambda x:x['ping']);self.ids=[i['id'] for i in items];self.model.beginResetModel();self.model.items=items;self.model.endResetModel();self.count.setText(f'{len(items)} visíveis / {len(self.studio.objects)} candidatos')
        if selected and selected['id'] in self.ids:self.table.selectRow(self.ids.index(selected['id']))
        self.table.resizeColumnsToContents();selection.blockSignals(False)
    def current(self):
        row=self.table.currentIndex().row()
        return self.studio.objects.get(self.ids[row]) if 0<=row<len(self.ids) else None
    def select(self):
        item=self.current()
        if item:
            self.studio.inspect_object(item['id']);self.preview.setText(f'{item["label"]} · ping {item["ping"]+1} · selecione o tratamento do recorte' + (f'\nContraste eco/sombra: {item["shadow_evidence"].get("contrast", "indisponível")} — evidência para revisão' if item.get('shadow_evidence') else ''))
    def mark(self,status):
        item=self.current()
        if item:item['status']=status;self.studio.save_objects();self.filter_changed()
    def name_object(self):
        item=self.current()
        if not item:return
        text,ok=QInputDialog.getText(self,'Revisão manual','Nome atribuído por você:',text=item['label'])
        if ok and text.strip():item['label']=text.strip();item['manual_label']=True;self.studio.save_objects();self.refresh()
    def export(self):
        path,_=QFileDialog.getSaveFileName(self,'Exportar objetos filtrados não rejeitados',str(ROOT/'outputs/objetos.geojson'),'GeoJSON (*.geojson);;Shapefile (*.shp);;GPX (*.gpx);;KML (*.kml)')
        if path:
            try:count=export_objects(self.filtered(),path);self.studio.log.appendPlainText(f'{count} objetos exportados: {path}')
            except Exception as error:self.studio.notify_error(str(error))
    def clear_items(self,ids):
        self.studio.ai_epoch+=1;self.studio.ai_continuous.setChecked(False)
        self.studio.batch_dialog.cancel();self.studio.batch_dialog.pending={}
        if self.studio.ai_worker and self.studio.ai_worker.isRunning():self.studio.ai_worker.stop.set()
        self.removed={key:self.studio.objects.pop(key) for key in ids if key in self.studio.objects}
        self.studio.save_objects();self.filter_changed();self.preview.clear()
    def clear_filtered(self):self.clear_items(list(self.ids))
    def clear_all(self):self.clear_items(list(self.studio.objects))
    def undo_clear(self):
        self.studio.objects.update(self.removed);self.removed={};self.studio.save_objects();self.filter_changed()
    def filter_crop(self):
        item=self.current();rec=self.studio.recording
        if not item or not rec or (self.crop_worker and self.crop_worker.isRunning()):return
        from app import Worker,rgba_image
        from image_processing import ImageSettings,enhance
        from PySide6.QtGui import QPixmap
        from dataclasses import replace
        import numpy as np
        obj=dict(item);method=self.crop_filter.currentText();settings=replace(self.studio.processing,method=method)
        self.preview.setText('Lendo e tratando somente o recorte…')
        def work(note,progress,cancel):
            first=int(obj.get('frame_start',obj['ping']-32));a,info=rec.waterfall(first+32,obj.get('frame_channel',obj['channel']),obj.get('frame_water',False),64)
            x0,y0,x1,y1=obj['box'];shift=first-info['start'];y0+=shift;y1+=shift
            x0=max(0,int(x0)-16);x1=min(a.shape[1],int(np.ceil(x1))+16);y0=max(0,int(y0)-8);y1=min(a.shape[0],int(np.ceil(y1))+8)
            if x1<=x0 or y1<=y0:raise ValueError('Recorte fora do trecho gravado.')
            crop=a[y0:y1,x0:x1].copy()
            from PIL import Image
            ratio=max(.1,min(40.,info['along_track_m']/info['sampling_m']))
            crop=np.asarray(Image.fromarray(crop).resize((crop.shape[1],max(1,round(len(crop)*ratio))),Image.Resampling.BILINEAR))
            return enhance(crop,settings),settings.palette
        def ready(result):
            a,palette=result;pix=QPixmap.fromImage(rgba_image(a,gamma=1.,palette=palette));self.preview.setPixmap(pix.scaled(self.preview.size(),Qt.AspectRatioMode.KeepAspectRatio,Qt.TransformationMode.SmoothTransformation))
        self.crop_worker=Worker(work);self.crop_worker.ready.connect(ready);self.crop_worker.failed.connect(self.preview.setText);self.crop_worker.start()
