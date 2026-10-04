"""YOLO image/label pairs with explicit partial negatives and immutable provenance."""
from pathlib import Path
import json,hashlib
import numpy as np
from PIL import Image

def export_dataset(source,target,namespace,progress=lambda value:None,cancel=lambda:False):
    from sonar_ai import MULTI_LABELS
    if namespace not in ('ghostvision','sonarvision'):raise ValueError('Selecione namespace GhostVision ou SonarVision.')
    source=Path(source);target=Path(target);target.mkdir(parents=True,exist_ok=True)
    if (target/'manifest.json').exists():raise ValueError('Destino já contém dataset; use uma nova pasta.')
    names=['Crab-Pot'] if namespace=='ghostvision' else MULTI_LABELS
    latest=sorted(source.glob('reviewed/*/latest.json'));samples=[];skipped=[]
    for number,index in enumerate(latest):
        if cancel():raise InterruptedError('Exportação de dataset cancelada.')
        current=json.loads(index.read_text(encoding='utf-8'));record_path=index.parent/current['event'];record=json.loads(record_path.read_text(encoding='utf-8'))
        if record.get('model_namespace')!=namespace or record.get('proposed_class') is None:
            skipped.append(str(record_path));continue
        raw=(index.parent/record['image']).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=record['image_sha256']:raise ValueError('Recorte alterado desde a revisão: '+str(index))
        identity=index.parent.name;decision=record['decision'];positive=decision=='confirmado'
        group='train' if positive else 'partial-negatives'
        images=target/'images'/group;labels=target/'labels'/group;images.mkdir(parents=True,exist_ok=True);labels.mkdir(parents=True,exist_ok=True)
        image=Image.open(index.parent/record['image']).convert('L')
        raw_width,raw_height=image.size
        ratio=float(record['geometry'].get('inference_row_ratio',1.))
        height=max(1,round(raw_height*ratio))
        if not np.isfinite(ratio) or ratio<=0 or raw_width*height>32_000_000:raise ValueError('Escala física do recorte inválida ou excessiva.')
        image=image.resize((raw_width,height),Image.Resampling.BILINEAR);image.save(images/(identity+'.png'))
        box=np.asarray(record['geometry']['box_pixels'],float);width,height=raw_width,raw_height
        normalized=np.array([(box[0]+box[2])/2/width,(box[1]+box[3])/2/height,(box[2]-box[0])/width,(box[3]-box[1])/height])
        if not np.isfinite(normalized).all() or np.any(normalized<0) or np.any(normalized>1):raise ValueError('Caixa normalizada inválida.')
        klass=int(record['proposed_class'])
        if not 0<=klass<len(names):raise ValueError('Classe fora do namespace especializado.')
        label=f'{klass} '+ ' '.join(f'{v:.8f}' for v in normalized)+'\n' if positive else ''
        (labels/(identity+'.txt')).write_text(label,encoding='utf-8')
        samples.append({'id':identity,'image':(images/(identity+'.png')).relative_to(target).as_posix(),'label':(labels/(identity+'.txt')).relative_to(target).as_posix(),
                        'class_id':klass,'box_xywh':normalized.tolist(),'decision':decision,'supervision':'positive_box' if positive else 'negative_class_in_box_only',
                        'whole_image_background_verified':False,'source_dat_sha256':record['source_dat_sha256'],'feedback_record':str(record_path),
                        'exported_image_sha256':hashlib.sha256((images/(identity+'.png')).read_bytes()).hexdigest(),'inference_row_ratio':ratio,'image_domain':'grayscale physical-aspect crop; no cosmetic palette','annotation_complete':record.get('training_ready',False)})
        progress({'stage':'dataset','percent':100*(number+1)/max(1,len(latest))})
    if not samples:raise ValueError('Nenhum recorte humano revisado para o namespace selecionado.')
    manifest={'format':'SonarStudio-yolo-dataset-v1','namespace':namespace,'classes':names,'samples':samples,'skipped_records':skipped,
              'independent_validation_available':False,'validation_split':'not assigned; requires independent reviewed recordings','negative_policy':'partial negatives excluded from generic YOLO train folder; CPU head trainer masks supervision to rejected class/box'}
    temporary=target/'manifest.incomplete.json';temporary.write_text(json.dumps(manifest,indent=2,ensure_ascii=False),encoding='utf-8');temporary.replace(target/'manifest.json')
    (target/'data.yaml').write_text('path: '+json.dumps(str(target.resolve()))+'\ntrain: images/train\nval: images/val\nnames:\n'+''.join(f'  {i}: {json.dumps(name,ensure_ascii=False)}\n' for i,name in enumerate(names)),encoding='utf-8')
    (target/'images/val').mkdir(exist_ok=True)
    return {'dataset':str(target),'samples':len(samples),'positive':sum(s['decision']=='confirmado' for s in samples),'partial_negative':sum(s['decision']=='rejeitado' for s in samples),'manifest':str(target/'manifest.json')}
