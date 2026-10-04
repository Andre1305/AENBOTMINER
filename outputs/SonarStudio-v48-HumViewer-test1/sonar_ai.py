"""CPU detection with GhostVision and SonarVision models trained for side-scan sonar."""
from pathlib import Path
import json,hashlib,threading
import numpy as np
from PIL import Image

MODEL=Path(__file__).parent/'models/gv-yolo12/weights.onnx'
_session=None
_lock=threading.Lock()
MULTI_MODEL=Path(__file__).parent/'models/sonarvision-v6/weights.onnx'
MULTI_LABELS=['Detrito / obstáculo','Aeronave submersa','Objeto semelhante a mina','Naufrágio']
_multi_session=None
_session_path=None
_multi_session_path=None

def multi_session():
    global _multi_session,_multi_session_path
    with _lock:
        from model_registry import model_path
        path=model_path('sonarvision')
        key=(str(path),path.stat().st_size,path.stat().st_mtime_ns)
        if _multi_session is None or _multi_session_path!=key:
            import onnxruntime as ort
            options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1
            _multi_session=ort.InferenceSession(str(path),sess_options=options,providers=['CPUExecutionProvider']);_multi_session_path=key
    return _multi_session

def session():
    global _session,_session_path
    with _lock:
        from model_registry import model_path
        path=model_path('ghostvision')
        key=(str(path),path.stat().st_size,path.stat().st_mtime_ns)
        if _session is None or _session_path!=key:
            import onnxruntime as ort
            options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1
            _session=ort.InferenceSession(str(path),sess_options=options,providers=['CPUExecutionProvider']);_session_path=key
    return _session

def nms(boxes,iou=.35):
    boxes=sorted(boxes,key=lambda b:b['score'],reverse=True);keep=[]
    while boxes:
        best=boxes.pop(0);keep.append(best);a=np.array(best['box'])
        rest=[]
        for b in boxes:
            c=np.array(b['box']);size=np.maximum(0,np.minimum(a[2:],c[2:])-np.maximum(a[:2],c[:2]))
            intersect=size.prod();area=(a[2]-a[0])*(a[3]-a[1])+(c[2]-c[0])*(c[3]-c[1])-intersect
            if best.get('class_id')!=b.get('class_id') or intersect/max(area,1e-9)<iou:rest.append(b)
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

def decode_multi(output,threshold=.35):
    a=np.asarray(output)
    if a.ndim!=3 or a.shape[0]!=1 or a.shape[1]!=8:raise ValueError('Saída incompatível com SonarVision v6.')
    a=a[0].T;classes=a[:,4:].argmax(axis=1);scores=a[np.arange(len(a)),4+classes];items=[]
    for row,cls,score in zip(a[scores>=threshold],classes[scores>=threshold],scores[scores>=threshold]):
        x,y,w,h=row[:4]
        if w<=0 or h<=0 or not np.isfinite(row).all():continue
        items.append({'box':[float(x-w/2),float(y-h/2),float(x+w/2),float(y+h/2)],'class_id':int(cls),'score':float(score),'label':MULTI_LABELS[cls],'model':'SonarVision v6 — experimental','status':'pendente'})
    return nms(items)

def detect_multiclass(array,threshold=.35,max_tiles=192,cancel=lambda:False,max_side=2048):
    a=np.asarray(array,dtype=np.uint8);height,width=a.shape;scale=min(1.,max_side/max(a.shape))
    small=np.asarray(Image.fromarray(a).resize((max(1,round(width*scale)),max(1,round(height*scale))),Image.Resampling.BILINEAR))
    h,w=small.shape;tiles=[(x,y) for y in sorted(set(list(range(0,max(1,h-256+1),192))+[max(0,h-256)])) for x in sorted(set(list(range(0,max(1,w-256+1),192))+[max(0,w-256)]))]
    if len(tiles)>max_tiles:raise ValueError('Trecho grande demais para o modelo multiclasse; use qualidade Rápida ou menos pings.')
    detector=multi_session();name=detector.get_inputs()[0].name;items=[]
    for x,y in tiles:
        if cancel():raise InterruptedError('Análise cancelada.')
        crop=small[y:y+256,x:x+256];ch,cw=crop.shape;pad=np.full((256,256),114,np.uint8);pad[:ch,:cw]=crop
        tensor=np.repeat(pad[None,None],3,axis=1).astype('float32')/255.
        for item in decode_multi(detector.run(None,{name:tensor})[0],threshold):
            box=np.asarray(item['box']);box[[0,2]]=np.clip(box[[0,2]],0,cw);box[[1,3]]=np.clip(box[[1,3]],0,ch)
            if box[2]<=box[0] or box[3]<=box[1]:continue
            item['box']=((box+np.array([x,y,x,y]))/scale).tolist();items.append(item)
    return nms(items),{'model':'Dinoman1221/sonarvision-yolov8-esi-v6','classes':MULTI_LABELS,'threshold':threshold,'tiles':len(tiles),'analysis_scale':scale,'experimental':True}

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
        depth=float(row.inst_dep_m);ground=distance if info['remove_water'] else np.sqrt(max(0,distance*distance-depth*depth))
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
        payload=f'{recording.dat}|{item["channel"]}|{ping//5}|{round(ground)}|{item["model"]}|{item.get("class_id", "")}'
        item['id']=hashlib.sha256(payload.encode()).hexdigest()[:16];located.append(item)
    return located
