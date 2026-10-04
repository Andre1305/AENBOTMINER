"""Georeferenced raster/vector exports, preserving source and recording provenance."""
from pathlib import Path
import json,zipfile,xml.etree.ElementTree as ET
from dataclasses import replace
import numpy as np
import rasterio
from rasterio.windows import Window,from_bounds,transform as window_transform
from rasterio.enums import Resampling
from image_processing import ImageSettings,colorize

def recipe(path,source,settings,**extra):
    Path(str(path)+'.recipe.json').write_text(json.dumps({'source':str(source),'image_settings':settings.json(),**extra},indent=2,ensure_ascii=False),encoding='utf-8')

def export_raster(source,target,settings=ImageSettings(),resolution=None,bounds=None,progress=lambda x:None,cancel=lambda:False):
    """Tiled RGBA GeoTIFF. Process in overlapping windows, bounded memory."""
    from rasterio.transform import from_origin
    source=Path(source);target=Path(target)
    with rasterio.open(source) as src:
        if src.count!=1 or src.dtypes[0]!='uint8':raise ValueError('Use exportação de profundidade para rasters batimétricos; esta função trata intensidade sonar de 8 bits.')
        r=max(src.res[0],resolution or src.res[0])
        b=src.bounds if bounds is None else rasterio.coords.BoundingBox(*bounds)
        left=max(b.left,src.bounds.left);bottom=max(b.bottom,src.bounds.bottom)
        right=min(b.right,src.bounds.right);top=min(b.top,src.bounds.top)
        if right<=left or top<=bottom:raise ValueError('Área sem interseção com o raster.')
        # Snap ROI to native raster grid; preserve exact transform at native sampling.
        c0,r0=(~src.transform)*(left,top);c1,r1=(~src.transform)*(right,bottom)
        # Inverse projected coordinates can be a few ulps off an integer pixel.
        # Avoid growing native exports by one row/column because of that error.
        c0=max(0,int(np.floor(c0+1e-6)));r0=max(0,int(np.floor(r0+1e-6)))
        c1=min(src.width,int(np.ceil(c1-1e-6)));r1=min(src.height,int(np.ceil(r1-1e-6)))
        left,top=src.transform*(c0,r0);right,bottom=src.transform*(c1,r1)
        width=max(1,int(np.ceil((c1-c0)*src.res[0]/r-1e-9)))
        height=max(1,int(np.ceil((r1-r0)*src.res[1]/r-1e-9)))
        trans=from_origin(left,top,r,r)
        tmp=target.with_name(target.stem+'.incomplete.tif')
        halo=64;block=512;total=((width+block-1)//block)*((height+block-1)//block);done=0
        options=replace(settings,destripe=0.) # Destriping must precede georectification.
        with rasterio.open(tmp,'w',driver='GTiff',width=width,height=height,count=4,dtype='uint8',
                           crs=src.crs,transform=trans,tiled=True,compress='deflate',predictor=2,
                           photometric='RGB',BIGTIFF='IF_SAFER') as out:
            out.colorinterp=(rasterio.enums.ColorInterp.red,rasterio.enums.ColorInterp.green,rasterio.enums.ColorInterp.blue,rasterio.enums.ColorInterp.alpha)
            for row in range(0,height,block):
                for col in range(0,width,block):
                    if cancel():raise InterruptedError('Exportação cancelada.')
                    h=min(block,height-row);w=min(block,width-col)
                    x0=max(0,col-halo);y0=max(0,row-halo);x1=min(width,col+w+halo);y1=min(height,row+h+halo)
                    ul=trans*(x0,y0);lr=trans*(x1,y1)
                    window=from_bounds(ul[0],lr[1],lr[0],ul[1],src.transform)
                    data=src.read(1,window=window,out_shape=(y1-y0,x1-x0),masked=True,resampling=Resampling.nearest if r==src.res[0] else Resampling.average)
                    rgba=colorize(data.filled(0),options,~np.ma.getmaskarray(data))
                    rgba=rgba[row-y0:row-y0+h,col-x0:col-x0+w]
                    out.write(np.moveaxis(rgba,-1,0),window=Window(col,row,w,h))
                    done+=1;progress({'stage':'export','percent':100*done/total})
            out.update_tags(source=str(source.resolve()),processing=json.dumps(options.json()))
            factors=[f for f in [2,4,8,16,32,64,128] if min(width,height)//f>0]
            if factors:out.build_overviews(factors,Resampling.average)
        tmp.replace(target)
    recipe(target,source,options,resolution_m=r,crs=str(src.crs),width=width,height=height)
    return str(target)

def raster_google(source,target,settings,resolution=None,bounds=None,progress=lambda x:None,cancel=lambda:False):
    from osgeo import gdal
    gdal.UseExceptions()
    target=Path(target);folder=target.parent/(target.stem+'-assets');folder.mkdir(parents=True,exist_ok=True)
    rgb=folder/'mosaico-copper.tif'
    export_raster(source,rgb,settings,resolution,bounds,progress,cancel)
    warp=gdal.Warp('',str(rgb),format='VRT',dstSRS='EPSG:4326',resampleAlg='bilinear',dstAlpha=False)
    def callback(value,msg,data):
        progress({'stage':'google-earth','percent':value*100});return 0 if cancel() else 1
    output=gdal.Translate(str(target),warp,format='KMLSUPEROVERLAY',creationOptions=['FORMAT=PNG'],callback=callback)
    if output is None:raise RuntimeError('Falha ao exportar Google Earth.')
    output=None;warp=None
    recipe(target,source,settings,resolution_m=resolution,bounds=bounds,geographic_crs='EPSG:4326')
    return str(target)

def vectors(recording,target,settings,contours=None,progress=lambda x:None,cancel=lambda:False):
    from bathymetry import soundings
    from bathy_review import review_signature
    from osgeo import ogr,osr,gdal
    ogr.UseExceptions();gdal.UseExceptions()
    xy,z,indices,raw=soundings(recording,settings)
    if len(indices)<2:raise ValueError('Selecione pelo menos duas sondagens válidas para exportar pontos e trajetória.')
    target=Path(target);kind=target.suffix.lower()
    if kind=='.gpx':
        ns='http://www.topografix.com/GPX/1/1';son='https://sonarstudio.local/schema/1'
        ET.register_namespace('',ns);ET.register_namespace('sonar',son)
        root=ET.Element('{'+ns+'}gpx',version='1.1',creator='SonarStudio')
        track=ET.SubElement(root,'{'+ns+'}trk');ET.SubElement(track,'{'+ns+'}name').text=recording.dat.stem
        segment=ET.SubElement(track,'{'+ns+'}trkseg')
        for j,(index,depth) in enumerate(zip(indices,z)):
            if j%1000==0:
                if cancel():raise InterruptedError('Exportação cancelada.')
                progress({'stage':'vector','percent':j/max(1,len(indices))*100})
            row=recording.data.iloc[index]
            if not np.isfinite([row.lat,row.lon]).all():continue
            point=ET.SubElement(segment,'{'+ns+'}trkpt',lat=f'{row.lat:.9f}',lon=f'{row.lon:.9f}')
            ext=ET.SubElement(point,'{'+ns+'}extensions')
            ET.SubElement(ext,'{'+son+'}depth_m').text=f'{depth:.4f}'
            ET.SubElement(ext,'{'+son+'}raw_depth_m').text=f'{raw[j]:.4f}'
            ET.SubElement(ext,'{'+son+'}ping').text=str(index+1)
        ET.ElementTree(root).write(target,encoding='utf-8',xml_declaration=True)
    else:
        driver='ESRI Shapefile' if kind=='.shp' else ('GPKG' if kind=='.gpkg' else 'KML')
        if kind=='.kmz':
            import time
            stage=target.parent/(target.stem+'-kmz-assets-'+str(time.time_ns()));stage.mkdir()
            kml=stage/'doc.kml';vectors(recording,kml,settings,contours,progress,cancel)
            contour_file=kml.with_name(kml.stem+'-isobatas.kml')
            if contour_file.exists():
                ns='http://www.opengis.net/kml/2.2';tree=ET.parse(kml)
                document=tree.find('{'+ns+'}Document')
                if document is not None:
                    network=ET.SubElement(document,'{'+ns+'}NetworkLink')
                    ET.SubElement(network,'{'+ns+'}name').text='Isóbatas'
                    link=ET.SubElement(network,'{'+ns+'}Link');ET.SubElement(link,'{'+ns+'}href').text=contour_file.name
                    tree.write(kml,encoding='utf-8',xml_declaration=True)
            with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as archive:
                archive.write(kml,'doc.kml')
                if contour_file.exists():archive.write(contour_file,contour_file.name)
            Path(str(target)+'.recipe.json').write_text(json.dumps({'source_dat':str(recording.dat),'sounding_review_sha256':review_signature(recording),'bathymetry_settings':settings.json(),'depth_convention':'positive down, metres','contours':str(contours) if contours else None},indent=2,ensure_ascii=False),encoding='utf-8')
            return str(target)
        if target.exists():
            raise ValueError('O destino já existe. Escolha outro nome para preservar o arquivo anterior.')
        dst=ogr.GetDriverByName(driver).CreateDataSource(str(target))
        srs=osr.SpatialReference();srs.ImportFromWkt(recording.crs.to_wkt());srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        geographic=kind in ('.kml','.kmz')
        out_srs=osr.SpatialReference();out_srs.ImportFromEPSG(4326);out_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        conversion=osr.CoordinateTransformation(srs,out_srs) if geographic else None
        layer=dst.CreateLayer(target.stem,srs=out_srs if geographic else srs,geom_type=ogr.wkbPoint,options=['ENCODING=UTF-8'] if driver=='ESRI Shapefile' else [])
        for name,type_ in [('ping',ogr.OFTInteger),('depth_m',ogr.OFTReal),('raw_dep_m',ogr.OFTReal)]:layer.CreateField(ogr.FieldDefn(name,type_))
        if driver=='GPKG':layer.StartTransaction()
        for j,(point,depth,index) in enumerate(zip(xy,z,indices)):
            if j%1000==0:
                if cancel():raise InterruptedError('Exportação cancelada.')
                progress({'stage':'vector','percent':j/max(1,len(indices))*100})
            geometry=ogr.Geometry(ogr.wkbPoint);geometry.AddPoint_2D(*point)
            if conversion:geometry.Transform(conversion)
            feature=ogr.Feature(layer.GetLayerDefn());feature.SetGeometry(geometry)
            feature.SetField('ping',int(index)+1);feature.SetField('depth_m',float(depth));feature.SetField('raw_dep_m',float(raw[j]));layer.CreateFeature(feature)
        if driver=='GPKG':layer.CommitTransaction()
        # A shapefile contains one geometry type. Trajectory and contours are companions.
        track_dst=dst
        if driver=='ESRI Shapefile':track_dst=ogr.GetDriverByName(driver).CreateDataSource(str(target.with_name(target.stem+'-trajetoria.shp')))
        track=track_dst.CreateLayer('trajetoria',srs=out_srs if geographic else srs,geom_type=ogr.wkbLineString)
        line=ogr.Geometry(ogr.wkbLineString)
        for point in xy:line.AddPoint_2D(*point)
        if conversion:line.Transform(conversion)
        feature=ogr.Feature(track.GetLayerDefn());feature.SetGeometry(line);track.CreateFeature(feature)
        if track_dst is not dst:track_dst=None
        dst=None
        if contours:
            contour_target=target.with_name(target.stem+'-isobatas'+('.shp' if driver=='ESRI Shapefile' else '.kml' if geographic else '.gpkg'))
            options=gdal.VectorTranslateOptions(format=driver,dstSRS='EPSG:4326' if geographic else None)
            output=gdal.VectorTranslate(str(contour_target),str(contours),options=options);output=None
    Path(str(target)+'.recipe.json').write_text(json.dumps({'source_dat':str(recording.dat),'sounding_review_sha256':review_signature(recording),'bathymetry_settings':settings.json(),'depth_convention':'positive down, metres','contours':str(contours) if contours else None},indent=2,ensure_ascii=False),encoding='utf-8')
    return str(target)
