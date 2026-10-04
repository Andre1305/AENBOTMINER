"""Uncalibrated candidate-score visualization; never a probability of presence."""
from pathlib import Path
import json
import numpy as np

def points(items):
    valid=[]
    for obj in items:
        if obj.get('status')=='rejeitado' or obj.get('in_water_column'):continue
        try:value=np.array([obj['east'],obj['north'],obj['score']],float)
        except (KeyError,TypeError,ValueError):continue
        if np.isfinite(value).all() and 0<=value[2]<=1:valid.append(value)
    return np.asarray(valid,dtype=float).reshape(-1,3)

def score_grid(items,bounds,width,height,radius_m=10.,cancel=lambda:False):
    left,bottom,right,top=map(float,bounds)
    if not np.isfinite([left,bottom,right,top,radius_m]).all() or right<=left or top<=bottom or radius_m<=0 or width<1 or height<1:
        raise ValueError('Geometria do mapa de scores inválida.')
    if width*height>10_000_000:raise ValueError('Limite de 10 milhões de células; reduza a área ou aumente a célula.')
    result=np.zeros((height,width),np.float32);dx=(right-left)/width;dy=(top-bottom)/height
    for ordinal,(east,north,score) in enumerate(points(items)):
        if ordinal%32==0 and cancel():raise InterruptedError('Mapa de scores cancelado.')
        x=(east-left)/dx-.5;y=(top-north)/dy-.5
        sx=radius_m/dx;sy=radius_m/dy
        c0=max(0,int(np.floor(x-3*sx)));c1=min(width,int(np.ceil(x+3*sx))+1)
        r0=max(0,int(np.floor(y-3*sy)));r1=min(height,int(np.ceil(y+3*sy))+1)
        if c1<=c0 or r1<=r0:continue
        kernel=score*np.exp(-.5*(((np.arange(r0,r1)-y)/sy)[:,None]**2+((np.arange(c0,c1)-x)/sx)[None,:]**2))
        np.maximum(result[r0:r1,c0:c1],kernel,out=result[r0:r1,c0:c1])
    return result

def export_heatmap(items,target,crs,cell_m=2.,radius_m=10.,cancel=lambda:False):
    import rasterio
    from rasterio.transform import from_origin
    p=points(items)
    if not len(p):raise ValueError('Nenhum candidato georreferenciado e não rejeitado para exportar.')
    if not np.isfinite([cell_m,radius_m]).all() or cell_m<=0 or radius_m<=0:raise ValueError('Célula e raio devem ser positivos.')
    left=np.floor((p[:,0].min()-3*radius_m)/cell_m)*cell_m;right=np.ceil((p[:,0].max()+3*radius_m)/cell_m)*cell_m
    bottom=np.floor((p[:,1].min()-3*radius_m)/cell_m)*cell_m;top=np.ceil((p[:,1].max()+3*radius_m)/cell_m)*cell_m
    width=max(1,round((right-left)/cell_m));height=max(1,round((top-bottom)/cell_m))
    if cancel():raise InterruptedError('Exportação cancelada.')
    grid=score_grid(items,(left,bottom,right,top),width,height,radius_m,cancel)
    if cancel():raise InterruptedError('Exportação cancelada.')
    path=Path(target);tmp=path.with_name(path.stem+'.incomplete.tif')
    with rasterio.open(tmp,'w',driver='GTiff',height=height,width=width,count=1,dtype='float32',crs=crs,transform=from_origin(left,top,cell_m,cell_m),nodata=-9999,compress='deflate',tiled=True) as out:
        out.write(np.where(grid>0,grid,-9999).astype('float32'),1)
        out.update_tags(product='uncalibrated_candidate_score_heatmap',score_semantics='max(candidate_score * Gaussian(distance));not_probability',radius_m=str(radius_m),absence_semantics='uncolored/nodata does not establish absence',position_quality='approximate')
    tmp.replace(path)
    recipe={'product':'candidate-score-heatmap','calibrated_probability':False,'candidate_count':len(p),'cell_m':cell_m,'radius_m':radius_m,'crs':str(crs),'absence_is_unknown':True}
    Path(str(path)+'.recipe.json').write_text(json.dumps(recipe,indent=2),encoding='utf-8')
    return str(path)
