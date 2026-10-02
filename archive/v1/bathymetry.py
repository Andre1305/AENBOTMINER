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

    def json(self): return asdict(self)

def soundings(recording,settings):
    d=recording.data
    end=len(d) if settings.end_ping<0 else min(len(d),settings.end_ping+1)
    indices=np.arange(max(0,settings.start_ping),end)
    if not len(indices): raise ValueError('Intervalo de pings vazio.')
    raw=d.iloc[indices][settings.depth_source].to_numpy(dtype=float)
    xy=recording.xy[indices]
    med=median_filter(raw,size=max(1,int(settings.median_window)|1),mode='nearest')
    valid=np.isfinite(raw)&np.isfinite(xy).all(axis=1)&(raw>=settings.min_depth)&(raw<=settings.max_depth)
    if settings.spike_threshold>0: valid &= np.abs(raw-med)<=settings.spike_threshold
    z=med+settings.draft-settings.water_level
    return xy[valid],z[valid],indices[valid],raw[valid]

def generate(recording,target,settings,progress=lambda x:None,cancel=lambda:False,bounds=None):
    if settings.resolution<=0 or settings.max_distance<=0: raise ValueError('Grade e distância precisam ser positivas.')
    xy,z,indices,raw=soundings(recording,settings)
    if len(xy)<3: raise ValueError('Poucas sondagens válidas para interpolar.')
    # Repeated GPS positions must not bias inverse distance weighting.
    rounded=np.round(xy,2)
    unique,inverse=np.unique(rounded,axis=0,return_inverse=True)
    import pandas as pd
    grouped=pd.DataFrame({'group':inverse,'z':z}).groupby('group').z.median()
    z=grouped.to_numpy(); xy=unique; tree=cKDTree(xy)
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
                exact=dist[:,0]<1e-6;values[exact]=z[ids[exact,0]]
                values[~supported]=nodata;valid_cells+=int(supported.sum())
                out.write(values.astype('float32').reshape(h,w),1,window=Window(col,row,w,h))
                done+=1;progress({'stage':'bathymetry','percent':100*done/total})
        out.update_tags(depth_convention='positive_down_metres',vertical_reference=settings.reference,
                        correction='depth = median(source) + draft - water_level',settings=json.dumps(settings.json()))
        factors=[f for f in [2,4,8,16,32] if width//f>0 and height//f>0]
        if factors:out.build_overviews(factors,rasterio.enums.Resampling.average)
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
            'width':width,'height':height,'valid_cells':valid_cells,'settings':settings.json(),'source_dat':str(recording.dat),'crs':str(recording.crs)}
    (target/'resultado-batimetria.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    return result
