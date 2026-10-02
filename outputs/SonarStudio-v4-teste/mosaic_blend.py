"""Recombine rectified passes without re-reading or modifying original recordings."""
from pathlib import Path
import json
from contextlib import ExitStack
import numpy as np
import rasterio
from rasterio.windows import Window,from_bounds
from scipy.spatial import cKDTree
from osgeo import gdal

def recombine(prepared,target,method='mean',progress=lambda x:None,cancel=lambda:False,bounds=None):
    prepared=Path(prepared);target=Path(target);target.mkdir(parents=True,exist_ok=True)
    result=json.loads((prepared/'resultado.json').read_text())
    files=sorted(prepared.glob('ss_*/rect_wcr/*.tif'))
    if not files:raise ValueError('O mosaico selecionado não possui os blocos retificados ao lado.')
    order=files if method!='first' else list(reversed(files))
    r=result['resolution_m'];gdal.UseExceptions()
    vrt=target/'fontes.vrt';gdal.BuildVRT(str(vrt),[str(f) for f in order],options=gdal.BuildVRTOptions(resolution='user',xRes=r,yRes=r,srcNodata=0,VRTNodata=0,outputBounds=bounds))
    path=target/'mosaico-recomposto.tif';temporary=target/'mosaico-recomposto.incomplete.tif'
    if method in ('first','last'):
        ds=gdal.Translate(str(temporary),str(vrt),creationOptions=['BIGTIFF=IF_SAFER','TILED=YES','COMPRESS=DEFLATE','PREDICTOR=2'],callback=lambda v,m,d:(progress({'stage':'blend','percent':100*v}) or (0 if cancel() else 1)))
        ds=None
    elif method in ('mean','ai_yolo','ai_anomaly','ai_multiclass'):
        with rasterio.open(vrt) as base,ExitStack() as stack:
            sources=[stack.enter_context(rasterio.open(f)) for f in files]
            boxes=np.array([[s.bounds.left,s.bounds.bottom,s.bounds.right,s.bounds.top] for s in sources])
            centers=(boxes[:,:2]+boxes[:,2:])/2;radius=np.linalg.norm(boxes[:,2:]-boxes[:,:2],axis=1).max()/2
            tree=cKDTree(centers);profile=base.profile.copy();profile.update(driver='GTiff',tiled=True,blockxsize=512,blockysize=512,compress='deflate',predictor=2,BIGTIFF='IF_SAFER')
            total=((base.width+511)//512)*((base.height+511)//512);done=0
            with rasterio.open(temporary,'w',**profile) as out:
                for row in range(0,base.height,512):
                    for col in range(0,base.width,512):
                        if cancel():raise InterruptedError('Recomposição cancelada; original preservado.')
                        h=min(512,base.height-row);w=min(512,base.width-col)
                        ul=base.transform*(col,row);lr=base.transform*(col+w,row+h)
                        mid=((ul[0]+lr[0])/2,(ul[1]+lr[1])/2)
                        candidates=tree.query_ball_point(mid,radius+np.hypot(w*r,h*r)/2)
                        summed=np.zeros((h,w),np.float32);counts=np.zeros((h,w),np.uint16)
                        chosen=np.zeros((h,w),np.uint8);best_score=np.zeros((h,w),np.float32)
                        for i in candidates:
                            left,bottom,right,top=boxes[i]
                            if right<=ul[0] or left>=lr[0] or top<=lr[1] or bottom>=ul[1]:continue
                            window=from_bounds(ul[0],lr[1],lr[0],ul[1],sources[i].transform)
                            a=sources[i].read(1,window=window,out_shape=(h,w),boundless=True,fill_value=0,resampling=rasterio.enums.Resampling.nearest)
                            valid=a>0;summed+=a;counts+=valid
                            if method.startswith('ai_') and len(candidates)>1 and valid.any():
                                from sonar_ai import detect_yolo,detect_multiclass,anomaly_map
                                if method in ['ai_yolo','ai_multiclass']:
                                    detector=detect_yolo if method=='ai_yolo' else detect_multiclass
                                    detections,_=detector(a,cancel=cancel);confidence=np.zeros(a.shape,np.float32)
                                    for item in detections:
                                        x0,y0,x1,y1=item['box'];x0=max(0,int(x0));y0=max(0,int(y0));x1=min(w,int(np.ceil(x1)));y1=min(h,int(np.ceil(y1)))
                                        confidence[y0:y1,x0:x1]=np.maximum(confidence[y0:y1,x0:x1],item['score'])
                                else:confidence=anomaly_map(a);confidence=np.where(confidence>=.8,confidence,0)
                                better=(confidence>best_score)&valid
                                chosen[better]=a[better];best_score[better]=confidence[better]
                        data=np.round(summed/np.maximum(counts,1)).astype('uint8')
                        if method.startswith('ai_'):data[best_score>0]=chosen[best_score>0]
                        out.write(data,1,window=Window(col,row,w,h));done+=1
                        if done%16==0 or done==total:progress({'stage':'blend','percent':100*done/total})
                out.update_tags(overlap=method,source_project=str(prepared),object_policy='choose_recorded_pass_inside_AI_candidates;mean_elsewhere' if method.startswith('ai_') else 'mean_nonzero_intensity')
    else:raise ValueError('Método de sobreposição desconhecido.')
    if cancel():raise InterruptedError('Recomposição cancelada.')
    with rasterio.open(temporary,'r+') as ds:
        factors=[f for f in [2,4,8,16,32,64,128] if min(ds.width,ds.height)//f>0]
        if factors:ds.build_overviews(factors,rasterio.enums.Resampling.average)
    temporary.replace(path)
    summary={'success':True,'mosaic':str(path),'overlap':method,'tile_source':str(prepared),'resolution_m':r,'tile_count':len(files)}
    (target/'resultado.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    return summary
