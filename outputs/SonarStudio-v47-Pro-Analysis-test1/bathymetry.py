"""IDW depth model with explicit distance mask and positive-down depth convention."""
from pathlib import Path
from dataclasses import dataclass, asdict
import json
import numpy as np
from scipy.spatial import cKDTree
from scipy.ndimage import median_filter
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

@dataclass(frozen=True)
class BathySettings:
    resolution: float = 2.
    max_distance: float = 25.
    neighbors: int = 12
    power: float = 2.
    min_depth: float = .5
    max_depth: float = 100.
    median_window: int = 5
    spike_threshold: float = 0.
    draft: float = 0.
    water_level: float = 0.
    depth_source: str = 'inst_dep_m'
    contour_interval: float = 1.
    start_ping: int = 0
    end_ping: int = -1
    reference: str = 'Profundidade relativa; sem datum vertical aferido'
    interpolation: str = 'IDW'
    smoothing_m: float = 0.
    boundary_path: str = ''

    def json(self): return asdict(self)

def soundings(recording,settings,edits=None):
    if settings.min_depth>=settings.max_depth:raise ValueError('Profundidade mínima deve ser menor que a máxima.')
    if settings.neighbors<1 or settings.power<=0:raise ValueError('Parâmetros IDW inválidos.')
    if settings.depth_source not in recording.data:raise ValueError('Fonte de profundidade indisponível nesta gravação.')
    d=recording.data
    end=len(d) if settings.end_ping<0 else min(len(d),settings.end_ping+1)
    indices=np.arange(max(0,settings.start_ping),end)
    if not len(indices): raise ValueError('Intervalo de pings vazio.')
    raw=d.iloc[indices][settings.depth_source].to_numpy(dtype=float)
    original=raw.copy();excluded=np.zeros(len(indices),bool)
    from bathy_review import load_edits
    if edits is None:edits=load_edits(recording)
    for key,edit in edits.get('pings',{}).items():
        j=int(key)-indices[0]
        if j<0 or j>=len(indices):continue
        excluded[j]=bool(edit.get('excluded',False))
        if edit.get('depth_m') is not None:raw[j]=float(edit['depth_m'])
    xy=recording.xy[indices]
    usable=np.isfinite(raw)&~excluded
    if not usable.any():raise ValueError('Nenhuma sondagem válida após a revisão.')
    filled=np.interp(np.arange(len(raw)),np.where(usable)[0],raw[usable])
    med=median_filter(filled,size=max(1,int(settings.median_window)|1),mode='nearest')
    valid=usable&np.isfinite(xy).all(axis=1)&(raw>=settings.min_depth)&(raw<=settings.max_depth)
    if settings.spike_threshold>0: valid &= np.abs(raw-med)<=settings.spike_threshold
    level=np.full(len(indices),settings.water_level)
    series=edits.get('water_levels',[])
    if series:
        values=np.asarray(series,float);level+=np.interp(d.iloc[indices].time_s.to_numpy(),values[:,0],values[:,1])
    if edits.get('tide_table'):
        from tide_datum import tide_at
        if series:raise ValueError('Remova a série antiga de água antes de usar maré por timestamp; impede correção duplicada.')
        level+=tide_at(d.iloc[indices].time_s.to_numpy(),edits['tide_table'])
    z=med+settings.draft-level
    return xy[valid],z[valid],indices[valid],original[valid]

def generate(recording,target,settings,progress=lambda x:None,cancel=lambda:False,bounds=None):
    if settings.resolution<=0 or settings.max_distance<=0 or settings.contour_interval<=0: raise ValueError('Grade, distância e intervalo das isóbatas precisam ser positivos.')
    from bathy_review import load_edits
    review=load_edits(recording)
    xy,z,indices,raw=soundings(recording,settings,review)
    boundaries=[]
    if settings.boundary_path:
        from osgeo import ogr,osr
        source=ogr.Open(settings.boundary_path)
        if source is None:raise ValueError('Não foi possível abrir o limite da área de água.')
        dst= osr.SpatialReference();dst.ImportFromWkt(recording.crs.to_wkt());dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        for layer in source:
            srs=layer.GetSpatialRef()
            if srs is None:raise ValueError('O limite precisa ter um sistema de coordenadas definido.')
            srs=srs.Clone();srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER);transform_geometry=osr.CoordinateTransformation(srs,dst)
            for feature in layer:
                original=feature.GetGeometryRef()
                if original is None:continue
                geometry=original.Clone()
                if ogr.GT_Flatten(geometry.GetGeometryType()) not in [ogr.wkbPolygon,ogr.wkbMultiPolygon]:continue
                if not geometry.IsValid():raise ValueError('O limite contém um polígono inválido.')
                geometry.Transform(transform_geometry);boundaries.append(json.loads(geometry.ExportToJson()))
        source=None
        if not boundaries:raise ValueError('Use polígonos de área de água (GeoJSON, GPKG ou Shapefile).')
    if len(xy)<3: raise ValueError('Poucas sondagens válidas para interpolar.')
    # Repeated GPS positions must not bias inverse distance weighting.
    rounded=np.round(xy,2)
    unique,inverse=np.unique(rounded,axis=0,return_inverse=True)
    import pandas as pd
    grouped=pd.DataFrame({'group':inverse,'z':z}).groupby('group').z.median()
    z=grouped.to_numpy(); xy=unique; tree=cKDTree(xy)
    if settings.interpolation not in ['IDW','TIN']:raise ValueError('Escolha IDW ou TIN.')
    interpolator=None
    if settings.interpolation=='TIN':
        from scipy.interpolate import LinearNDInterpolator
        try:interpolator=LinearNDInterpolator(xy,z,fill_value=np.nan)
        except Exception as error:raise ValueError('TIN exige sondagens em posições não colineares; use IDW para uma linha isolada.') from error
    r=settings.resolution
    if bounds is None:
        lo=xy.min(axis=0)-settings.max_distance;hi=xy.max(axis=0)+settings.max_distance
        left,bottom=lo;right,top=hi
    else: left,bottom,right,top=bounds
    left=np.floor(left/r)*r;top=np.ceil(top/r)*r
    width=int(np.ceil((right-left)/r));height=int(np.ceil((top-bottom)/r))
    if width<=0 or height<=0 or width*height>100_000_000: raise ValueError('Grade acima de 100 milhões de células ou extensão inválida. Aumente o tamanho da célula ou reduza a área.')
    target=Path(target);target.mkdir(parents=True,exist_ok=True)
    path=target/'batimetria-profundidade.tif';tmp=target/'batimetria-profundidade.incomplete.tif'
    transform=from_origin(left,top,r,r);nodata=-9999.
    profile=dict(driver='GTiff',width=width,height=height,count=1,dtype='float32',crs=recording.crs,
                 transform=transform,nodata=nodata,tiled=True,compress='deflate',predictor=3,BIGTIFF='IF_SAFER')
    total=((width+255)//256)*((height+255)//256);done=0;valid_cells=0
    with rasterio.open(tmp,'w',**profile) as out:
        for row in range(0,height,256):
            for col in range(0,width,256):
                if cancel():raise InterruptedError('Batimetria cancelada; originais preservados.')
                h=min(256,height-row);w=min(256,width-col)
                xx,yy=np.meshgrid(left+(col+np.arange(w)+.5)*r,top-(row+np.arange(h)+.5)*r)
                dist,ids=tree.query(np.c_[xx.ravel(),yy.ravel()],k=min(settings.neighbors,len(xy)),workers=1)
                if dist.ndim==1:dist=dist[:,None];ids=ids[:,None]
                supported=dist[:,0]<=settings.max_distance
                allowed=dist<=settings.max_distance
                weights=np.where(allowed,1/np.maximum(dist,1e-6)**settings.power,0)
                values=(weights*z[ids]).sum(axis=1)/np.maximum(weights.sum(axis=1),1e-30)
                if interpolator is not None:
                    values=interpolator(np.c_[xx.ravel(),yy.ravel()]);supported &= np.isfinite(values)
                exact=dist[:,0]<1e-6;values[exact]=z[ids[exact,0]]
                if boundaries:
                    from rasterio.features import geometry_mask
                    inside=geometry_mask(boundaries,out_shape=(h,w),transform=from_origin(left+col*r,top-row*r,r,r),invert=True)
                    supported &= inside.ravel()
                values[~supported]=nodata;valid_cells+=int(supported.sum())
                out.write(values.astype('float32').reshape(h,w),1,window=Window(col,row,w,h))
                done+=1;progress({'stage':'bathymetry','percent':100*done/total})
        out.update_tags(depth_convention='positive_down_metres',vertical_reference=settings.reference,tide_datum=review.get('tide_table',{}).get('datum','none'),tide_source_sha256=review.get('tide_table',{}).get('source_sha256','none'),datum_independently_verified='false',
                        correction='depth = median(reviewed source) + draft - constant water_level - interpolated water_level_series or timestamp_tide (exclusive)',settings=json.dumps(settings.json()))
        factors=[f for f in [2,4,8,16,32] if width//f>0 and height//f>0]
        if factors:out.build_overviews(factors,rasterio.enums.Resampling.average)
    if settings.smoothing_m>0:
        from scipy.ndimage import gaussian_filter
        sigma=settings.smoothing_m/r;halo=max(1,int(np.ceil(4*sigma)))
        if halo>1024:raise ValueError('Suavização muito ampla para esta grade; aumente a célula ou reduza a suavização.')
        smooth=target/'batimetria-suavizada.incomplete.tif'
        with rasterio.open(tmp) as src,rasterio.open(smooth,'w',**profile) as out:
            for row in range(0,height,256):
                for col in range(0,width,256):
                    if cancel():raise InterruptedError('Suavização cancelada.')
                    h=min(256,height-row);w=min(256,width-col);y=max(0,row-halo);x=max(0,col-halo)
                    a=src.read(1,window=Window(x,y,min(width,col+w+halo)-x,min(height,row+h+halo)-y));valid=a!=nodata
                    numer=gaussian_filter(np.where(valid,a,0),sigma);denom=gaussian_filter(valid.astype(float),sigma)
                    data=np.where(valid,numer/np.maximum(denom,1e-12),nodata).astype('float32')
                    out.write(data[row-y:row-y+h,col-x:col-x+w],1,window=Window(col,row,w,h))
            out.update_tags(**src.tags())
            if factors:out.build_overviews(factors,rasterio.enums.Resampling.average)
        smooth.replace(tmp)
    tmp.replace(path)
    from osgeo import gdal,ogr,osr
    gdal.UseExceptions();ogr.UseExceptions()
    contours=target/'isobatas.gpkg'
    if contours.exists(): contours.unlink()
    dst=ogr.GetDriverByName('GPKG').CreateDataSource(str(contours))
    srs=osr.SpatialReference();srs.ImportFromWkt(recording.crs.to_wkt())
    layer=dst.CreateLayer('isobatas',srs,ogr.wkbLineString)
    layer.CreateField(ogr.FieldDefn('id',ogr.OFTInteger));layer.CreateField(ogr.FieldDefn('depth_m',ogr.OFTReal))
    ds=gdal.Open(str(path));gdal.ContourGenerate(ds.GetRasterBand(1),settings.contour_interval,0,[],1,nodata,layer,0,1)
    ds=None;dst=None
    result={'success':True,'raster':str(path),'contours':str(contours),'soundings':len(indices),'unique_positions':len(xy),
            'width':width,'height':height,'valid_cells':valid_cells,'settings':settings.json(),'sounding_review':review,'source_dat':str(recording.dat),'crs':str(recording.crs)}
    (target/'resultado-batimetria.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    return result
