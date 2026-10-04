"""Local human-feedback archive. Rejection is never silently a background label."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, math, os, uuid
import numpy as np
from PIL import Image

def training_class(item):
    from sonar_ai import MULTI_LABELS
    if item.get('manual_label'):return None
    model=str(item.get('model','')).lower()
    class_id=item.get('class_id')
    if 'ghost' in model or 'yolo12' in model:
        return ('ghostvision',0,'Crab-Pot') if class_id in (None,0) else None
    if 'sonarvision' in model and isinstance(class_id,int) and 0<=class_id<len(MULTI_LABELS):
        return ('sonarvision',class_id,MULTI_LABELS[class_id])
    return None

def candidate_crop(recording,item,padding=16):
    if str(recording.dat)!=item.get('source_dat',str(recording.dat)):
        raise ValueError('O candidato pertence a outra gravação.')
    box=np.asarray(item['box'],dtype=float)
    if box.shape!=(4,) or not np.isfinite(box).all() or np.any(box[2:]<=box[:2]):
        raise ValueError('Caixa do candidato inválida.')
    first=int(item.get('frame_start',int(item['ping'])-32))
    last=first+max(64,int(math.ceil(box[3])))
    count=min(len(recording.data),last-first)
    wanted=int(item.get('frame_part_width',1))
    sides=2 if item.get('frame_channel')=='both' else 1
    if count<1 or count>4096 or wanted<1 or wanted>200000 or count*wanted*sides>16_000_000:
        raise ValueError('Recorte excede os limites de leitura; revise a geometria do candidato.')
    array,info=recording.waterfall(first+count//2,item.get('frame_channel',item['channel']),item.get('frame_water',False),count)
    # Interactive candidates can come from 64 rows cut out of a wider Viewer
    # frame. Re-anchor each side at the nadir before reusing stored box columns.
    actual=int(info['part_width']);wanted=int(item.get('frame_part_width',actual))
    if wanted<1 or wanted>200000:raise ValueError('Largura original inválida.')
    both=info['channel']=='both';port=info['channel'].startswith('ss_port')
    parts=[]
    for number in range(2 if both else 1):
        a=array[:,number*actual:(number+1)*actual];b=np.zeros((len(a),wanted),np.uint8)
        n=min(wanted,actual)
        if (both and number==0) or port:b[:,-n:]=a[:,-n:]
        else:b[:,:n]=a[:,:n]
        parts.append(b)
    array=np.concatenate(parts,axis=1)
    box[[1,3]]+=first-info['start']
    clipped=np.array([max(0,box[0]),max(0,box[1]),min(array.shape[1],box[2]),min(len(array),box[3])])
    if np.any(clipped[2:]<=clipped[:2]):raise ValueError('Candidato fora dos dados disponíveis.')
    if not np.allclose(clipped,box):raise ValueError('Caixa parcialmente fora do trecho; revise a anotação.')
    x0=max(0,int(math.floor(box[0]))-padding);y0=max(0,int(math.floor(box[1]))-8)
    x1=min(array.shape[1],int(math.ceil(box[2]))+padding);y1=min(len(array),int(math.ceil(box[3]))+8)
    crop=array[y0:y1,x0:x1].copy()
    local=box-np.array([x0,y0,x0,y0])
    rows=recording.data.iloc[info['start']:info['end']];delta=np.diff(rows.time_s.to_numpy());positive=delta[delta>0]
    travel=float(rows.speed_ms.median())*(float(np.median(positive)) if len(positive) else .1)
    ratio=float(item.get('inference_row_ratio',max(.1,min(40.,travel/info['sampling_m'])) if np.isfinite(travel) and travel>0 else 1.))
    metadata={'inference_row_ratio':ratio,'frame':info,'source_part_width':wanted,'crop_origin':[x0,y0],
              'box_pixels':local.tolist(),'sampling_m':info['sampling_m'],
              'orientation':'port_left_starboard_right;chronological_rows;no_display_flip',
              'image_domain':'original_uint8_waterfall;no_palette_or_cosmetic_filters'}
    return crop,metadata

def save_feedback(recording,item,root,cancel=lambda:False):
    if item.get('status') not in ('confirmado','rejeitado'):
        raise ValueError('Apenas decisões humanas explícitas entram no dataset.')
    if cancel():raise InterruptedError('Registro de revisão cancelado.')
    crop,geometry=candidate_crop(recording,item)
    identity=hashlib.sha256((str(recording.dat)+'|'+str(item['id'])).encode()).hexdigest()[:24]
    folder=Path(root)/'reviewed'/identity;folder.mkdir(parents=True,exist_ok=True)
    event=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'-'+uuid.uuid4().hex[:8]
    image_file=folder/(event+'.png');record_file=folder/(event+'.json')
    temp=image_file.with_suffix('.png.tmp');Image.fromarray(crop).save(temp,format='PNG')
    if cancel():temp.unlink(missing_ok=True);raise InterruptedError('Registro de revisão cancelado.')
    temp.replace(image_file)
    klass=training_class(item)
    entry={'format':'SonarStudio-feedback-v1','created_utc':datetime.now(timezone.utc).isoformat(),
           'candidate':dict(item),'image':image_file.name,'geometry':geometry,
           'image_sha256':hashlib.sha256(image_file.read_bytes()).hexdigest(),
           'source_dat_sha256':hashlib.sha256(recording.dat.read_bytes()).hexdigest(),
           'decision':item['status'],'training_ready':False,'needs_complete_annotation':True,
           'rejection_is_background':False,'model_namespace':klass[0] if klass else None,
           'proposed_class':klass[1] if klass else None,
           'reason':'Review candidate box/classes and every object in the crop before fine-tuning.'}
    if klass and item['status']=='confirmado':
        x0,y0,x1,y1=geometry['box_pixels'];height,width=crop.shape
        label=f'{klass[1]} {(x0+x1)/2/width:.8f} {(y0+y1)/2/height:.8f} {(x1-x0)/width:.8f} {(y1-y0)/height:.8f}\n'
        proposal=folder/(event+'.label-proposto.txt');proposal.write_text(label,encoding='utf-8')
        entry['proposed_label']=proposal.name
    temporary=record_file.with_suffix('.json.tmp');temporary.write_text(json.dumps(entry,indent=2,ensure_ascii=False),encoding='utf-8');temporary.replace(record_file)
    # Latest points to an immutable decision event; changing a decision keeps history.
    latest=folder/'latest.json';temporary=folder/'latest.tmp'
    temporary.write_text(json.dumps({'event':record_file.name,'decision':item['status']},indent=2),encoding='utf-8');temporary.replace(latest)
    return {'folder':str(folder),'record':str(record_file),'decision':item['status'],'training_ready':False}
