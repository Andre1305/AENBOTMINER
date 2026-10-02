"""CPU sonar detection: licensed GhostVision ONNX and local anomaly candidates."""
from pathlib import Path
import json,hashlib,threading
import numpy as np
from PIL import Image
from scipy.ndimage import uniform_filter,maximum_filter,minimum_filter,label,find_objects

MODEL=Path(__file__).parent/'models/gv-yolo12/weights.onnx'
_session=None
_lock=threading.Lock()

def session():
    global _session
    with _lock:
        if _session is None:
            import onnxruntime as ort
            options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1
            _session=ort.InferenceSession(str(MODEL),sess_options=options,providers=['CPUExecutionProvider'])
    return _session

def nms(boxes,iou=.35):
    boxes=sorted(boxes,key=lambda b:b['score'],reverse=True);keep=[]
    while boxes:
        best=boxes.pop(0);keep.append(best);a=np.array(best['box'])
        rest=[]
        for b in boxes:
            c=np.array(b['box']);size=np.maximum(0,np.minimum(a[2:],c[2:])-np.maximum(a[:2],c[:2]))
            intersect=size.prod();area=(a[2]-a[0])*(a[3]-a[1])+(c[2]-c[0])*(c[3]-c[1])-intersect
            if intersect/max(area,1e-9)<iou:rest.append(b)
        boxes=rest
    return keep

def decode_yolo(output,threshold=.35):
    a=np.asarray(output)
    if a.ndim!=3 or a.shape[0]!=1:raise ValueError('Saída ONNX incompatível com YOLO.')
    a=a[0]
    if a.shape[0]==5:a=a.T
    if a.shape[1]!=5:raise ValueError('O modelo integrado deve ter uma classe e saída [x,y,w,h,score].')
    chosen=a[a[:,4]>=threshold];boxes=[]
    for x,y,w,h,score in chosen:
        boxes.append({'box':[float(x-w/2),float(y-h/2),float(x+w/2),float(y+h/2)],'score':float(score),'label':'Candidato a covo','model':'GhostVision YOLO12','status':'pendente'})
    return nms(boxes)

def detect_yolo(array,threshold=.35,max_tiles=32,cancel=lambda:False,max_side=2048):
    """Bounded physical frame: tile the visible array, retaining coordinates."""
    a=np.asarray(array,dtype=np.uint8);height,width=a.shape
    # Keep enough range detail while bounding interactive inference cost.
    scale=min(1.,max_side/max(height,width))
    resized=np.asarray(Image.fromarray(a).resize((max(1,round(width*scale)),max(1,round(height*scale))),Image.Resampling.BILINEAR))
    h,w=resized.shape;stride=480
    xs=list(range(0,max(1,w-640+1),stride));ys=list(range(0,max(1,h-640+1),stride))
    if not xs or xs[-1]!=max(0,w-640):xs.append(max(0,w-640))
    if not ys or ys[-1]!=max(0,h-640):ys.append(max(0,h-640))
    tiles=[(x,y) for y in sorted(set(ys)) for x in sorted(set(xs))]
    if len(tiles)>max_tiles:raise ValueError('Trecho muito grande para IA interativa. Reduza a quantidade de pings.')
    detector=session();input_name=detector.get_inputs()[0].name;detections=[]
    for x,y in tiles:
        if cancel():raise InterruptedError('Análise cancelada.')
        crop=resized[y:y+640,x:x+640];ch,cw=crop.shape
        pad=np.full((640,640),114,np.uint8);pad[:ch,:cw]=crop
        rgb=np.repeat(pad[None,None,:,:],3,axis=1).astype('float32')/255
        output=detector.run(None,{input_name:rgb})[0]
        for item in decode_yolo(output,threshold):
            bx=np.array(item['box']);bx[[0,2]]=np.clip(bx[[0,2]],0,cw);bx[[1,3]]=np.clip(bx[[1,3]],0,ch)
            if bx[2]<=bx[0] or bx[3]<=bx[1]:continue
            bx+=np.array([x,y,x,y]);item['box']=(bx/scale).tolist();detections.append(item)
    return nms(detections),{'model':'PINGEcosystem/gv-yolo12','threshold':threshold,'tiles':len(tiles),'analysis_scale':scale,'original_shape':[height,width],'class':'Crab-Pot'}

def anomaly_map(array):
    """Isolation Forest on patch texture; scores are relative, not probabilities."""
    from sklearn.ensemble import IsolationForest
    a=np.asarray(array,dtype=np.float32);stride=16
    mean=uniform_filter(a,9);variance=np.maximum(uniform_filter(a*a,9)-mean*mean,0)
    contrast=maximum_filter(a,7)-minimum_filter(a,7)
    gradient=np.hypot(*np.gradient(mean))
    features=np.stack([mean,variance**.5,contrast,gradient],axis=-1)[::stride,::stride]
    flat=features.reshape(-1,4)
    valid=flat[:,0]>1
    scores=np.zeros(len(flat),np.float32)
    if valid.sum()>=8:
        model=IsolationForest(n_estimators=48,max_samples=min(256,int(valid.sum())),random_state=17,n_jobs=1,contamination='auto')
        model.fit(flat[valid]);s=-model.score_samples(flat[valid]);lo,hi=np.percentile(s,[10,99])
        scores[valid]=np.clip((s-lo)/max(1e-6,hi-lo),0,1)
    grid=scores.reshape(features.shape[:2])
    return np.asarray(Image.fromarray(grid).resize((a.shape[1],a.shape[0]),Image.Resampling.BILINEAR))

def detect_anomalies(array,threshold=.8):
    a=np.asarray(array);scale=min(1.,1024/max(a.shape))
    small=np.asarray(Image.fromarray(a).resize((max(2,round(a.shape[1]*scale)),max(2,round(a.shape[0]*scale)))))
    scores=anomaly_map(small);connected,n=label((scores>=threshold)&(small>0));items=[]
    for i,region in enumerate(find_objects(connected),1):
        if region is None:continue
        y,x=region;area=int((connected[region]==i).sum())
        if area<6:continue
        items.append({'box':[x.start/scale,y.start/scale,x.stop/scale,y.stop/scale],'score':float(scores[region].max()),'label':'Anomalia — revisar','model':'Isolation Forest','status':'pendente'})
    return sorted(items,key=lambda x:x['score'],reverse=True)[:40],{'model':'Isolation Forest','relative_score':True,'analysis_scale':scale,'threshold':threshold}

def locate(items,recording,info):
    """Approximate target projection; never equate to surveyed object coordinates."""
    from pyproj import Transformer
    transform=Transformer.from_crs(recording.crs,4326,always_xy=True)
    located=[];part=info['part_width'];channel=info['channel'];sampling=info['sampling_m']
    for item in items:
        item=dict(item);x0,y0,x1,y1=item['box'];x=(x0+x1)/2;y=(y0+y1)/2
        ping=min(info['end']-1,max(info['start'],info['start']+int(y)));row=recording.data.iloc[ping]
        if channel=='both':port=x<part;distance=(part-1-x if port else x-part)*sampling
        else:port=channel.startswith('ss_port');distance=(part-1-x if port else x)*sampling
        depth=float(row.get('dep_m',row.inst_dep_m));ground=distance if info['remove_water'] else np.sqrt(max(0,distance*distance-depth*depth))
        valid_range=info['remove_water'] or distance>=depth
        heading=float(row.instr_heading)
        if not np.isfinite(heading):
            lo=max(0,ping-20);hi=min(len(recording.xy)-1,ping+20);delta=recording.xy[hi]-recording.xy[lo]
            heading=np.degrees(np.arctan2(delta[0],delta[1]))
        theta=np.radians(heading);side=-1 if port else 1
        east=float(row.e+side*np.cos(theta)*ground);north=float(row.n-side*np.sin(theta)*ground)
        lon,lat=transform.transform(east,north)
        item.update(ping=ping,channel='ss_port' if port else 'ss_star',ground_range_m=float(ground),depth_m=depth,
                    east=east,north=north,longitude=lon,latitude=lat,georeference='aproximada: navegação + orientação + alcance',in_water_column=not valid_range)
        payload=f'{recording.dat}|{item["channel"]}|{ping//5}|{round(ground)}|{item["model"]}'
        item['id']=hashlib.sha256(payload.encode()).hexdigest()[:16];located.append(item)
    return located
