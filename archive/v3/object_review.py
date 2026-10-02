"""Human review and export of model suggestions, never silent confirmations."""
from pathlib import Path
import json,xml.etree.ElementTree as ET
from PySide6.QtWidgets import QDialog,QVBoxLayout,QHBoxLayout,QTableWidget,QTableWidgetItem,QPushButton,QFileDialog,QInputDialog,QLabel
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

class ObjectReview(QDialog):
    def __init__(self,studio):
        super().__init__(studio);self.studio=studio;self.setWindowTitle('Objetos — sugestões da IA e revisão');self.resize(900,540)
        layout=QVBoxLayout(self)
        note=QLabel('YOLO detecta candidatos a covos. Isolation Forest destaca anomalias, sem classificar o objeto.\nScores de anomalia são relativos. Posições de alvos são aproximadas; confirme visualmente.');note.setWordWrap(True);layout.addWidget(note)
        self.table=QTableWidget(0,6);self.table.setHorizontalHeaderLabels(['Objeto','Score','Ping','Canal','Revisão','Posição']);self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows);self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers);self.table.itemSelectionChanged.connect(self.select);layout.addWidget(self.table)
        buttons=QHBoxLayout()
        for label,callback in [('Confirmar',lambda:self.mark('confirmado')),('Rejeitar',lambda:self.mark('rejeitado')),('Nomear objeto',self.name_object),('Exportar objetos',self.export)]:
            b=QPushButton(label);b.clicked.connect(callback);buttons.addWidget(b)
        layout.addLayout(buttons);self.ids=[]
    def refresh(self):
        self.table.blockSignals(True);items=sorted(self.studio.objects.values(),key=lambda x:x['ping']);self.ids=[i['id'] for i in items];self.table.setRowCount(len(items))
        for row,item in enumerate(items):
            values=[item['label'],f'{item["score"]:.3f}',str(item['ping']+1),item['channel'],item['status'],'Coluna d’água' if item.get('in_water_column') else 'Aproximada']
            for col,value in enumerate(values):self.table.setItem(row,col,QTableWidgetItem(value))
        self.table.resizeColumnsToContents();self.table.blockSignals(False)
    def current(self):
        row=self.table.currentRow()
        return self.studio.objects.get(self.ids[row]) if 0<=row<len(self.ids) else None
    def select(self):
        item=self.current()
        if item:self.studio.pause_playback();self.studio.set_index(item['ping'])
    def mark(self,status):
        item=self.current()
        if item:item['status']=status;self.studio.save_objects();self.refresh();self.studio.map.update()
    def name_object(self):
        item=self.current()
        if not item:return
        text,ok=QInputDialog.getText(self,'Revisão manual','Nome atribuído por você:',text=item['label'])
        if ok and text.strip():item['label']=text.strip();item['manual_label']=True;self.studio.save_objects();self.refresh()
    def export(self):
        path,_=QFileDialog.getSaveFileName(self,'Exportar objetos não rejeitados',str(ROOT/'outputs/objetos.geojson'),'GeoJSON (*.geojson);;Shapefile (*.shp);;GPX (*.gpx);;KML (*.kml)')
        if path:
            try:count=export_objects(list(self.studio.objects.values()),path);self.studio.log.appendPlainText(f'{count} objetos exportados: {path}')
            except Exception as error:self.studio.notify_error(str(error))
