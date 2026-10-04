"""Optional per-window Isolation Forest on acoustic texture; no semantic labels."""
import numpy as np
from PIL import Image
from scipy.ndimage import label,find_objects

def triage_regions(image,contamination=.1,max_regions=12,cancel=lambda:False):
    if not .01<=contamination<=.3 or not 1<=max_regions<=64:
        raise ValueError('Parâmetros da triagem inválidos.')
    if cancel():raise InterruptedError('Triagem cancelada.')
    a=np.asarray(image,dtype=np.uint8)
    if a.ndim!=2:raise ValueError('Triagem exige intensidade sonar 2D.')
    if not a.any():return [],{'patches':0,'anomalous_patches':0,'fallback':False}
    ratio=min(1.,1024/max(a.shape));small=np.asarray(Image.fromarray(a).resize((max(1,round(a.shape[1]*ratio)),max(1,round(a.shape[0]*ratio))),Image.Resampling.BOX))
    block=32;features=[];cells=[];h,w=small.shape;rows=(h+block-1)//block;cols=(w+block-1)//block
    for y in range(0,h,block):
        for x in range(0,w,block):
            tile=small[y:y+block,x:x+block];values=tile[tile>0].astype(float)
            if len(values)<max(16,tile.size//4):continue
            lo,hi=np.percentile(values,[10,90]);valid=tile.astype(float)
            gx=np.abs(np.diff(valid,axis=1));gy=np.abs(np.diff(valid,axis=0))
            features.append([values.mean(),values.std(),hi-lo,np.mean(gx) if gx.size else 0,np.mean(gy) if gy.size else 0])
            cells.append((y//block,x//block))
    if len(features)<16 or np.max(np.std(features,axis=0))<1e-6:
        # Insufficient context or homogeneous bottom: do not veto the classifiers.
        return [[0,0,a.shape[1],a.shape[0]]],{'patches':len(features),'anomalous_patches':0,'fallback':True}
    from sklearn.ensemble import IsolationForest
    model=IsolationForest(n_estimators=48,max_samples=min(256,len(features)),contamination=contamination,random_state=45,n_jobs=1)
    flags=model.fit_predict(np.asarray(features))==-1
    if cancel():raise InterruptedError('Triagem cancelada.')
    mask=np.zeros((rows,cols),bool)
    for (y,x),flag in zip(cells,flags):mask[y,x]=flag
    components,n=label(mask,np.ones((3,3),bool));regions=[]
    for s in find_objects(components):
        if s is None:continue
        sy,sx=s
        # Include the acoustic shadow and context around the anomalous patch.
        regions.append([max(0,int((sx.start-2)*block/ratio)),max(0,int((sy.start-1)*block/ratio)),min(a.shape[1],int(np.ceil((sx.stop+2)*block/ratio))),min(a.shape[0],int(np.ceil((sy.stop+1)*block/ratio)))])
    if len(regions)>max_regions:
        return [[0,0,a.shape[1],a.shape[0]]],{'patches':len(features),'anomalous_patches':int(flags.sum()),'fallback':True}
    return regions,{'patches':len(features),'anomalous_patches':int(flags.sum()),'fallback':False}
