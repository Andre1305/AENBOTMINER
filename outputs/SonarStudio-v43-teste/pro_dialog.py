"""Advanced processing, bathymetry and geospatial export controls."""
from pathlib import Path
from dataclasses import replace
import hashlib,json,time
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (QDialog,QVBoxLayout,QFormLayout,QTabWidget,QWidget,QComboBox,
 QDoubleSpinBox,QSpinBox,QPushButton,QCheckBox,QLabel,QFileDialog,QScrollArea)
from image_processing import ImageSettings
from bathymetry import BathySettings
from recording import ROOT,Recording

class Professional(QDialog):
    def __init__(self,studio):
        super().__init__(studio);self.studio=studio
        self.setWindowTitle('SonarStudio — Imagem, batimetria e exportação');self.resize(650,760)
        layout=QVBoxLayout(self);tabs=QTabWidget();layout.addWidget(tabs)
        def page(title):
            body=QWidget();form=QFormLayout(body);scroll=QScrollArea();scroll.setWidgetResizable(True);scroll.setWidget(body);tabs.addTab(scroll,title);return form
        def choice(form,label,values):
            widget=QComboBox();widget.addItems(values);form.addRow(label,widget);return widget
        def number(form,label,low,high,value,step=1,decimals=2):
            widget=QDoubleSpinBox();widget.setRange(low,high);widget.setDecimals(decimals);widget.setSingleStep(step);widget.setValue(value);form.addRow(label,widget);return widget
        def button(form,label,callback):
            widget=QPushButton(label);widget.clicked.connect(callback);form.addRow(widget);return widget
        f=page('Sonar / imagem')
        note=QLabel('Filtros não generativos. Compare com o original; ajustes fortes podem apagar ecos reais.\nA exportação registra os parâmetros usados.');note.setWordWrap(True);f.addRow(note)
        self.method=choice(f,'Redução de ruído',['Original','Mediana','Lee — speckle','Non-local means','Wavelet'])
        self.strength=number(f,'Intensidade do filtro',0,1,.35,.05)
        self.destripe=number(f,'Faixas entre pings — sonar',0,1,0,.05)
        self.clahe=number(f,'Contraste local (CLAHE)',0,1,0,.05)
        self.sharpen=number(f,'Nitidez',0,2,0,.1)
        self.black=number(f,'Nível preto',0,254,0,1,0);self.white=number(f,'Nível branco',1,255,255,1,0)
        self.pings=QSpinBox();self.pings.setRange(64,3000);self.pings.setValue(160);f.addRow('Pings no Sonar Viewer',self.pings)
        self.aspect=choice(f,'Proporção do sonar',['Distância real','Pings × amostras','Preencher painel (escala livre)']);self.aspect.setCurrentIndex(2)
        self.compare=QCheckBox('Comparar o mesmo trecho: original / tratado');f.addRow(self.compare)
        self.on_map=QCheckBox('Aplicar filtros na prévia do mosaico');f.addRow(self.on_map)
        button(f,'Aplicar ao Sonar Viewer',self.apply_image)
        button(f,'Ajustar níveis pelo trecho atual (2–99%)',self.auto_levels)
        button(f,'Restaurar imagem original',self.reset_image)
        self.mosaic_depth_enabled=QCheckBox('Filtrar mosaico pela profundidade dos pings');f.addRow(self.mosaic_depth_enabled)
        self.mosaic_depth_min=number(f,'Mosaico: profundidade mínima (m)',0,2000,0,1)
        self.mosaic_depth_max=number(f,'Mosaico: profundidade máxima (m)',.1,2000,50,1)
        button(f,'Gerar novo mosaico com tratamento dos ecos',self.filtered_mosaic)
        self.overlap=choice(f,'Sobreposição de passagens',['Média das intensidades','Priorizar primeira passagem','Priorizar última passagem','IA — preservar candidatos a covos','IA — preservar objetos (experimental)'])
        self.blend_roi=QCheckBox('Mesclar somente a área visível');self.blend_roi.setChecked(True);f.addRow(self.blend_roi)
        button(f,'Recompor passagens do mosaico preparado',self.recombine)
        explanation=QLabel('O tratamento antes da retificação atua nos pings SON. A paleta copper é aplicada na visualização e exportação RGB. O GeoTIFF original permanece preservado.');explanation.setWordWrap(True);f.addRow(explanation)
        f=page('Batimetria')
        self.depth_source=choice(f,'Fonte da profundidade',['Original do equipamento','Processada pelo PINGMapper'])
        self.cell=number(f,'Grade (m)',.1,100,2,.5)
        self.radius=number(f,'Distância máxima de apoio (m)',1,1000,25,5)
        self.neighbors=QSpinBox();self.neighbors.setRange(1,64);self.neighbors.setValue(12);f.addRow('Vizinhos IDW',self.neighbors)
        self.power=number(f,'Expoente IDW',.5,6,2,.5)
        self.interpolation=choice(f,'Interpolação',['IDW','TIN'])
        self.smoothing=number(f,'Suavização do relevo (m; 0 desliga)',0,50,0,.5)
        self.min_depth=number(f,'Profundidade mínima (m)',0,10000,.5)
        self.max_depth=number(f,'Profundidade máxima (m)',.1,10000,100)
        self.median=QSpinBox();self.median.setRange(1,101);self.median.setSingleStep(2);self.median.setValue(5);f.addRow('Janela de mediana (pings)',self.median)
        self.spikes=number(f,'Excluir desvios da mediana (m; 0 desliga)',0,1000,0,.5)
        self.draft=number(f,'Transdutor abaixo da superfície (m)',-100,100,0,.1)
        self.level=number(f,'Água acima da referência (m)',-100,100,0,.1)
        self.reference=choice(f,'Referência vertical',['Relativa — sem datum aferido','Referência informada pelo usuário'])
        from PySide6.QtWidgets import QLineEdit
        self.datum=QLineEdit();self.datum.setPlaceholderText('Nome e origem da referência vertical');f.addRow('Descrição da referência',self.datum)
        self.boundary=QLineEdit();self.boundary.setPlaceholderText('Opcional: polígono da área de água');self.boundary.setClearButtonEnabled(True);f.addRow('Limite de água',self.boundary)
        button(f,'Selecionar limite / margens',self.choose_boundary)
        self.interval=number(f,'Intervalo das isóbatas (m)',.1,100,1,.5)
        self.depth_palette=choice(f,'Cores da batimetria',['turbo_r','viridis_r','Blues','terrain'])
        self.auto_range=QCheckBox('Escala de cores automática (percentis 1–99)');self.auto_range.setChecked(True);f.addRow(self.auto_range)
        self.color_min=number(f,'Início da escala de cores (m)',-10000,10000,0)
        self.color_max=number(f,'Fim da escala de cores (m)',-10000,10000,50)
        button(f,'Aplicar escala de cores',self.apply_depth_colors)
        self.start=QSpinBox();self.start.setRange(1,100000000);self.start.setValue(1);f.addRow('Primeiro ping',self.start)
        self.end=QSpinBox();self.end.setRange(0,100000000);self.end.setValue(0);self.end.setSpecialValueText('Até o último');f.addRow('Último ping',self.end)
        self.bathy_roi=QCheckBox('Limitar à área visível do mapa');f.addRow(self.bathy_roi)
        note=QLabel('Profundidade positiva para baixo: mediana(fonte) + imersão do transdutor − nível da água.\nSem apoio dentro da distância máxima, a célula fica sem dados. Não há correção automática de maré, atitude ou erro de GPS.');note.setWordWrap(True);f.addRow(note)
        button(f,'Gerar grade e isóbatas',self.generate_bathy)
        button(f,'Revisar sondagens / série de nível da água',self.review_depths)
        button(f,'Visualizar relevo 3D',self.show_relief)
        f=page('Exportar GIS')
        self.format=choice(f,'Produto',['Mosaico GeoTIFF copper','Mosaico KMZ','Mosaico KML','Sondagens Shapefile','Sondagens + trajetória GPX','Sondagens KML','Sondagens KMZ','Sondagens GeoPackage','Grade batimétrica GeoTIFF','Isóbatas Shapefile','Isóbatas KML','Isóbatas GPX'])
        self.scope=choice(f,'Área do mosaico',['Área visível','Mosaico completo'])
        self.export_res=choice(f,'Resolução do mosaico',['Nativa','5 cm','10 cm','25 cm','1 m','5 m'])
        note=QLabel('KML/KMZ usam imagens PNG em níveis de zoom e coordenadas WGS84. Shapefile contém vetores: sondagens, trajetória e isóbatas disponíveis. GeoTIFF mantém o raster e seu CRS.');note.setWordWrap(True);f.addRow(note)
        button(f,'Exportar com os parâmetros atuais',self.export)
        self.export_note=QLabel('');self.export_note.setWordWrap(True);f.addRow(self.export_note)
        layout.addWidget(QLabel('As operações usam a barra de progresso e o botão Cancelar da janela principal.'))

    def image_settings(self):
        return ImageSettings(self.method.currentText(),self.strength.value(),self.destripe.value(),self.clahe.value(),self.sharpen.value(),self.black.value(),self.white.value(),self.studio.gamma.value()/100,self.studio.palette.currentText())

    def bathy_settings(self):
        reference='Profundidade relativa; sem datum vertical aferido' if self.reference.currentIndex()==0 else self.datum.text().strip()
        if not reference:raise ValueError('Informe a referência vertical usada.')
        return BathySettings(self.cell.value(),self.radius.value(),self.neighbors.value(),self.power.value(),self.min_depth.value(),self.max_depth.value(),self.median.value(),self.spikes.value(),self.draft.value(),self.level.value(),'inst_dep_m' if self.depth_source.currentIndex()==0 else 'dep_m',self.interval.value(),self.start.value()-1,self.end.value()-1,reference,self.interpolation.currentText(),self.smoothing.value(),self.boundary.text().strip())

    def choose_boundary(self):
        path,_=QFileDialog.getOpenFileName(self,'Polígono da área de água','','Polígonos (*.geojson *.gpkg *.shp)')
        if path:self.boundary.setText(path)

    def review_depths(self):
        if not self.studio.recording:return
        from bathy_review import SoundingReview
        self.studio.pause_playback();self.depth_review=SoundingReview(self.studio);self.depth_review.show()

    def show_relief(self):
        s=self.studio
        if not s.bathy_result:s.notify_error('Gere uma grade batimétrica primeiro.');return
        from bathy_review import relief_data,ReliefView
        path=s.bathy_result['raster'];s.pause_playback()
        def ready(data):self.relief=ReliefView(s,data);self.relief.show()
        s.run_job(lambda note,progress,cancel:relief_data(path),ready);self.hide()

    def bounds(self):
        s=self.studio
        from PySide6.QtCore import QPointF
        a=s.map.screen_to_world(QPointF(0,0));b=s.map.screen_to_world(QPointF(s.map.width(),s.map.height()))
        return (float(a[0]),float(b[1]),float(b[0]),float(a[1]))

    def apply_image(self):
        s=self.studio
        try:
            settings=self.image_settings()
            if settings.white<=settings.black:raise ValueError('Branco deve ser maior que preto.')
            s.processing=settings
            s.map.processing=replace(settings,destripe=0.) if self.on_map.isChecked() else None
            s.show_sonar();s.map.refresh()
        except Exception as e:s.notify_error(str(e))

    def reset_image(self):
        self.method.setCurrentIndex(0)
        for w in [self.destripe,self.clahe,self.sharpen]:w.setValue(0)
        self.black.setValue(0);self.white.setValue(255);self.compare.setChecked(False);self.on_map.setChecked(False)
        self.apply_image()

    def auto_levels(self):
        import numpy as np
        a=self.studio.raw_array
        if a is None:return
        values=a[a>0]
        if not len(values):return
        lo,hi=np.percentile(values,[2,99])
        self.black.setValue(min(lo,254));self.white.setValue(max(lo+1,hi));self.apply_image()

    def mosaic_depth_limits(self):
        from mosaic_depth import validate_limits
        return validate_limits(self.mosaic_depth_min.value(),self.mosaic_depth_max.value()) if self.mosaic_depth_enabled.isChecked() else None

    def filtered_mosaic(self):
        self.apply_image();self.studio.generate_mosaic(filtered=True);self.hide()

    def recombine(self):
        s=self.studio
        if not s.sidescan_path:return
        prepared=Path(s.sidescan_path).parent
        if not (prepared/'resultado.json').exists() or not list(prepared.glob('ss_*/rect_wcr/*.tif')):
            s.notify_error('Abra o mosaico original preparado, na pasta que contém os blocos retificados, para recompor as passagens.');return
        target=prepared.with_name(prepared.name+'-sobreposicao-'+time.strftime('%Y%m%d-%H%M%S'))
        method=['mean','first','last','ai_yolo','ai_multiclass'][self.overlap.currentIndex()]
        bounds=self.bounds() if self.blend_roi.isChecked() else None
        def work(note,progress,cancel):
            from mosaic_blend import recombine
            note('Recompondo blocos retificados. Média pode suavizar emendas e produzir duplicação em passagens desalinhadas.')
            return recombine(prepared,target,method,progress,cancel,bounds)
        s.run_job(work,lambda result:s.open_mosaic(result['mosaic']));self.hide()

    def generate_bathy(self):
        s=self.studio
        if s.recording is None:return
        try:settings=self.bathy_settings()
        except Exception as e:s.notify_error(str(e));return
        folder=QFileDialog.getExistingDirectory(self,'Pasta para o novo projeto batimétrico',str(ROOT/'outputs'))
        if not folder:return
        target=Path(folder)/(s.recording.dat.stem+'-batimetria-'+time.strftime('%Y%m%d-%H%M%S'))
        bounds=self.bounds() if self.bathy_roi.isChecked() else None
        dat=s.recording.dat;project=s.recording.project
        def work(note,progress,cancel):
            from bathymetry import generate
            rec=Recording(dat,project)
            try:return generate(rec,target,settings,progress,cancel,bounds)
            finally:rec.close()
        def ready(result):
            s.bathy_result=result;s.load_contours(result['contours']);s.open_mosaic(result['raster']);s.log.appendPlainText('Batimetria e isóbatas geradas: '+str(target))
        s.run_job(work,ready);self.hide()

    def apply_depth_colors(self):
        m=self.studio.map
        if not self.auto_range.isChecked() and self.color_max.value()<=self.color_min.value():
            self.studio.notify_error('O fim da escala deve ser maior que o início.');return
        m.depth_palette=self.depth_palette.currentText()
        m.depth_fixed_range=None if self.auto_range.isChecked() else (self.color_min.value(),self.color_max.value())
        if m.depth_fixed_range:m.depth_range=m.depth_fixed_range
        elif m.dataset and m.dataset.dtypes[0].startswith('float'):
            import numpy as np
            data=m.dataset.read(1,out_shape=(min(512,m.dataset.height),min(512,m.dataset.width)),masked=True).compressed()
            if len(data):m.depth_range=tuple(np.percentile(data,[1,99]))
        m.refresh()

    def export(self):
        s=self.studio;fmt=self.format.currentText()
        extensions=['tif','kmz','kml','shp','gpx','kml','kmz','gpkg','tif','shp','kml','gpx'];extension=extensions[self.format.currentIndex()]
        path,_=QFileDialog.getSaveFileName(self,'Exportar '+fmt,str(ROOT/'outputs'/('produto-'+time.strftime('%Y%m%d-%H%M%S')+'.'+extension)),f'{extension.upper()} (*.{extension})')
        if not path:return
        image=self.image_settings()
        try:bathy=self.bathy_settings()
        except Exception as e:s.notify_error(str(e));return
        source=s.sidescan_path;bounds=self.bounds() if self.scope.currentIndex()==0 else None
        resolution=[None,.05,.1,.25,1.,5.][self.export_res.currentIndex()]
        dat=s.recording.dat if s.recording else None;project=s.recording.project if s.recording else None
        contours=s.bathy_result.get('contours') if s.bathy_result else None
        bathy_raster=s.bathy_result.get('raster') if s.bathy_result else None
        def work(note,progress,cancel):
            from pro_exports import export_raster,raster_google,vectors
            if fmt.startswith('Mosaico'):
                if source is None:raise ValueError('Abra um mosaico primeiro.')
                return export_raster(source,path,image,resolution,bounds,progress,cancel) if extension=='tif' else raster_google(source,path,image,resolution,bounds,progress,cancel)
            if fmt.startswith('Grade'):
                if not bathy_raster:raise ValueError('Gere a batimetria primeiro.')
                import shutil
                shutil.copyfile(bathy_raster,path);return path
            if fmt.startswith('Isóbatas'):
                if not contours:raise ValueError('Gere as isóbatas primeiro.')
                from osgeo import gdal
                gdal.UseExceptions()
                driver={'shp':'ESRI Shapefile','kml':'KML','gpx':'GPX'}[extension]
                options=gdal.VectorTranslateOptions(format=driver,dstSRS='EPSG:4326' if extension in ['kml','gpx'] else None,datasetCreationOptions=['GPX_USE_EXTENSIONS=YES'] if extension=='gpx' else [])
                result=gdal.VectorTranslate(path,contours,options=options);result=None
                from pro_exports import recipe
                recipe(path,contours,image,bathymetry_settings=bathy.json());return path
            if not dat:raise ValueError('Abra a gravação primeiro.')
            rec=Recording(dat,project)
            try:return vectors(rec,path,bathy,contours,progress,cancel)
            finally:rec.close()
        s.run_job(work,lambda result:s.log.appendPlainText('Exportação salva: '+str(result)));self.hide()

    def state(self):
        return {'mosaic_depth':self.mosaic_depth_limits(),'image':self.image_settings().json(),'bathymetry':self.bathy_settings().json(),
                'pings':self.pings.value(),'aspect':self.aspect.currentIndex(),'compare':self.compare.isChecked(),'map_filters':self.on_map.isChecked(),
                'export_scope':self.scope.currentIndex(),'export_resolution':self.export_res.currentIndex(),'overlap_method':['mean','first','last','ai_yolo','ai_multiclass'][self.overlap.currentIndex()],
                'depth_colors':{'palette':self.depth_palette.currentText(),'automatic':self.auto_range.isChecked(),'min':self.color_min.value(),'max':self.color_max.value()}}

    def restore(self,state):
        limits=state.get('mosaic_depth');self.mosaic_depth_enabled.setChecked(bool(limits))
        if limits:self.mosaic_depth_min.setValue(limits[0]);self.mosaic_depth_max.setValue(limits[1])
        image=state.get('image',{})
        self.method.setCurrentText(image.get('method','Original'))
        for name in ['strength','destripe','clahe','sharpen','black','white']:
            if name in image:getattr(self,name).setValue(image[name])
        bathy=state.get('bathymetry',{})
        mapping={'resolution':'cell','max_distance':'radius','neighbors':'neighbors','power':'power','min_depth':'min_depth','max_depth':'max_depth','median_window':'median','spike_threshold':'spikes','draft':'draft','water_level':'level','contour_interval':'interval'}
        for key,widget in mapping.items():
            if key in bathy:getattr(self,widget).setValue(bathy[key])
        self.interpolation.setCurrentText(bathy.get('interpolation','IDW'));self.smoothing.setValue(bathy.get('smoothing_m',0))
        self.boundary.setText(bathy.get('boundary_path',''))
        self.start.setValue(bathy.get('start_ping',0)+1);self.end.setValue(bathy.get('end_ping',-1)+1)
        self.depth_source.setCurrentIndex(0 if bathy.get('depth_source','inst_dep_m')=='inst_dep_m' else 1)
        reference=bathy.get('reference','')
        if reference and 'sem datum' not in reference:self.reference.setCurrentIndex(1);self.datum.setText(reference)
        self.pings.setValue(state.get('pings',160));self.aspect.setCurrentIndex(state.get('aspect',2));self.compare.setChecked(state.get('compare',False));self.on_map.setChecked(state.get('map_filters',False))
        self.scope.setCurrentIndex(state.get('export_scope',0));self.export_res.setCurrentIndex(state.get('export_resolution',0));self.apply_image()
        self.overlap.setCurrentIndex(['mean','first','last','ai_yolo','ai_multiclass'].index(state['overlap_method']) if state.get('overlap_method') in ['mean','first','last','ai_yolo','ai_multiclass'] else {0:0,1:1,2:2,3:3,4:0,5:4}.get(state.get('overlap',0),0))
        colors=state.get('depth_colors',{});self.depth_palette.setCurrentText(colors.get('palette','turbo_r'));self.auto_range.setChecked(colors.get('automatic',True));self.color_min.setValue(colors.get('min',0));self.color_max.setValue(colors.get('max',50));self.apply_depth_colors()
