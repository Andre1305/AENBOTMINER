"""CPU fine-tuning of YOLO ONNX classification convolutions; backbone/boxes frozen."""
from pathlib import Path
import argparse,json,hashlib,tempfile,os,sys,subprocess,threading,queue,time
import numpy as np
from PIL import Image

def prepare_input(path,size):
    image=Image.open(path).convert('L');w,h=image.size;ratio=min(1.,size/max(w,h))
    width=max(1,round(w*ratio));height=max(1,round(h*ratio));a=np.asarray(image.resize((width,height),Image.Resampling.BILINEAR))
    pad=np.full((size,size),114,np.uint8);pad[:height,:width]=a
    return np.repeat(pad[None,None],3,axis=1).astype('float32')/255.,width,height

def discover_heads(model,classes):
    from onnx import numpy_helper
    initial={v.name:numpy_helper.to_array(v) for v in model.graph.initializer};heads=[]
    for node in model.graph.node:
        if node.op_type!='Conv' or len(node.input)<3:continue
        if node.input[1] not in initial or node.input[2] not in initial:continue
        weight=initial[node.input[1]]
        # YOLO detect classification branch final 1x1 conv; exclude DFL/box conv.
        if len(weight.shape)==4 and weight.shape[0]==classes and weight.shape[2:]==(1,1) and ('cv3' in node.input[1] or 'one2one_cv3' in node.input[1]):
            heads.append({'feature':node.input[0],'weight':node.input[1],'bias':node.input[2],'W':weight.copy(),'B':initial[node.input[2]].copy()})
    if not heads:raise ValueError('ONNX sem head YOLO cv3 1x1 compatível. Não é seguro re-treinar pesos arbitrários.')
    return heads

def supervision(sample,shape,width,height,size,classes):
    _,_,h,w=shape;cx,cy,bw,bh=sample['box_xywh'];class_id=int(sample['class_id'])
    if not 0<=class_id<classes:raise ValueError('Classe incompatível com pesos de origem.')
    x0=(cx-bw/2)*width/size*w;x1=(cx+bw/2)*width/size*w
    y0=(cy-bh/2)*height/size*h;y1=(cy+bh/2)*height/size*h
    xx=np.arange(w)+.5;yy=np.arange(h)+.5;region=(xx[None,:]>=x0)&(xx[None,:]<=x1)&(yy[:,None]>=y0)&(yy[:,None]<=y1)
    if not region.any():region[min(h-1,max(0,int(cy*height/size*h))),min(w-1,max(0,int(cx*width/size*w)))]=True
    target=np.zeros((1,classes,h,w),np.float32);mask=np.zeros_like(target)
    mask[0,class_id,region]=1
    if sample['decision']=='confirmado':target[0,class_id,region]=1
    elif sample['decision']!='rejeitado':raise ValueError('Decisão humana necessária para treinamento.')
    return target,mask

def finetune_isolated(dataset,source,target,epochs,learning_rate,progress,cancel):
    # Windows GIS and PyTorch load conflicting DLL names. Training belongs in a
    # separate CPU process; it uses this same installed Python environment.
    executable=Path(sys.executable)
    if executable.name.lower()=='pythonw.exe':executable=executable.with_name('python.exe')
    command=[str(executable),str(Path(__file__).resolve()),'--dataset',str(dataset),'--source',str(source),'--output',str(target),'--epochs',str(epochs),'--lr',str(learning_rate)]
    env=dict(os.environ);env['SONAR_TRAIN_CHILD']='1';env['PYTHONUNBUFFERED']='1'
    process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace',env=env,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    lines=queue.Queue();thread=threading.Thread(target=lambda:[lines.put(line) for line in process.stdout],daemon=True);thread.start();result=None;errors=[]
    try:
        while process.poll() is None or thread.is_alive() or not lines.empty():
            if cancel():process.terminate();process.wait(timeout=15);raise InterruptedError('Treinamento cancelado; modelo original preservado.')
            try:line=lines.get(timeout=.1)
            except queue.Empty:continue
            try:
                value=json.loads(line)
                if value.get('event')=='progress':progress(value['value'])
                elif value.get('event')=='result':result=value['value']
                else:errors.append(line)
            except ValueError:errors.append(line)
        if process.returncode or result is None:raise RuntimeError('Fine-tuning CPU falhou: '+''.join(errors)[-6000:])
        return result
    finally:
        if process.poll() is None:process.terminate();process.wait(timeout=15)

def finetune(dataset,source,target,epochs=8,learning_rate=.0005,progress=lambda value:None,cancel=lambda:False):
    if os.environ.get('SONAR_TRAIN_CHILD')!='1':return finetune_isolated(dataset,source,target,epochs,learning_rate,progress,cancel)
    import torch,onnx,onnxruntime as ort
    from onnx import numpy_helper,helper,TensorProto
    dataset=Path(dataset);source=Path(source);target=Path(target)
    if target.resolve()==source.resolve():raise ValueError('Pesos originais não podem ser sobrescritos.')
    if target.resolve().is_relative_to((Path(__file__).parent/'models').resolve()):raise ValueError('Salve pesos treinados fora da pasta dos modelos originais.')
    if target.exists():raise ValueError('Destino ONNX já existe; escolha outro nome para preservar os pesos anteriores.')
    if not 1<=epochs<=500 or not 0<learning_rate<=.1:raise ValueError('Parâmetros de treinamento inválidos.')
    manifest=json.loads((dataset/'manifest.json').read_text(encoding='utf-8'));samples=manifest['samples']
    if not samples:raise ValueError('Dataset vazio.')
    torch.set_num_threads(2);torch.manual_seed(45)
    model=onnx.load(str(source));size=int(model.graph.input[0].type.tensor_type.shape.dim[2].dim_value);classes=len(manifest['classes'])
    if size not in (256,640):raise ValueError('Entrada ONNX não compatível com modelos integrados.')
    heads=discover_heads(model,classes)
    # Extract frozen feature tensors by exposing inputs to the final convs.
    feature_model=onnx.ModelProto();feature_model.CopyFrom(model)
    del feature_model.graph.output[:]
    for head in heads:feature_model.graph.output.append(helper.make_tensor_value_info(head['feature'],TensorProto.FLOAT,None))
    options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1
    features=ort.InferenceSession(feature_model.SerializeToString(),sess_options=options,providers=['CPUExecutionProvider'])
    layers=torch.nn.ModuleList()
    for head in heads:
        conv=torch.nn.Conv2d(head['W'].shape[1],classes,1)
        with torch.no_grad():conv.weight.copy_(torch.from_numpy(head['W']));conv.bias.copy_(torch.from_numpy(head['B']))
        layers.append(conv)
    optimizer=torch.optim.Adam(layers.parameters(),lr=learning_rate);original=[(layer.weight.detach().clone(),layer.bias.detach().clone()) for layer in layers]
    cache=[];cached_bytes=0
    for sample in samples:
        if cancel():raise InterruptedError('Treinamento cancelado; modelo original preservado.')
        path=(dataset/sample['image']).resolve()
        if not path.is_relative_to(dataset.resolve()):raise ValueError('Imagem fora do dataset.')
        if sample.get('exported_image_sha256') and hashlib.sha256(path.read_bytes()).hexdigest()!=sample['exported_image_sha256']:raise ValueError('Imagem do dataset alterada desde a exportação.')
        tensor,width,height=prepare_input(path,size);arrays=features.run(None,{features.get_inputs()[0].name:tensor})
        cached_bytes+=sum(a.nbytes for a in arrays)
        if cached_bytes>512*1024**2:raise ValueError('Cache de features excede 512 MiB. Use um subconjunto para treinar em CPU.')
        targets=[supervision(sample,a.shape,width,height,size,classes) for a in arrays]
        cache.append((sample,[torch.from_numpy(a) for a in arrays],[(torch.from_numpy(t),torch.from_numpy(m)) for t,m in targets]))
    def evaluate():
        loss=0.;scores=[]
        with torch.no_grad():
            for sample,arrays,targets in cache:
                sample_scores=[]
                for layer,a,(t,mask) in zip(layers,arrays,targets):
                    logits=layer(a);loss+=float((torch.nn.functional.binary_cross_entropy_with_logits(logits,t,reduction='none')*mask).sum()/mask.sum())
                    sample_scores.append(float((torch.sigmoid(logits)*mask).sum()/mask.sum()))
                scores.append({'sample':sample['id'],'decision':sample['decision'],'class_id':sample['class_id'],'score':float(np.mean(sample_scores))})
        return loss/(len(cache)*len(layers)),scores
    before_loss,before=evaluate();history=[]
    for epoch in range(epochs):
        for sample,arrays,targets in cache:
            if cancel():raise InterruptedError('Treinamento cancelado; modelo original preservado.')
            optimizer.zero_grad();loss=0
            for layer,a,(t,mask),(ow,ob) in zip(layers,arrays,targets,original):
                logits=layer(a);loss=loss+(torch.nn.functional.binary_cross_entropy_with_logits(logits,t,reduction='none')*mask).sum()/mask.sum()+.01*((layer.weight-ow).square().mean()+(layer.bias-ob).square().mean())
            loss.backward();torch.nn.utils.clip_grad_norm_(layers.parameters(),1.);optimizer.step()
        measured,_=evaluate();history.append(measured);progress({'stage':'finetune','percent':100*(epoch+1)/epochs,'loss':measured})
    after_loss,after=evaluate();updates={}
    for head,layer in zip(heads,layers):updates[head['weight']]=layer.weight.detach().numpy();updates[head['bias']]=layer.bias.detach().numpy()
    changed=[]
    for initializer in model.graph.initializer:
        if initializer.name in updates:changed.append(initializer.name);initializer.CopyFrom(numpy_helper.from_array(updates[initializer.name],initializer.name))
    onnx.checker.check_model(model);target.parent.mkdir(parents=True,exist_ok=True);temporary=target.with_suffix('.incomplete.onnx');onnx.save(model,str(temporary))
    # Verify exported weights run through the same ONNX inference backend.
    trained=ort.InferenceSession(str(temporary),sess_options=options,providers=['CPUExecutionProvider'])
    original_session=ort.InferenceSession(str(source),sess_options=options,providers=['CPUExecutionProvider']);comparisons=[]
    for sample,_,_ in cache:
        tensor,_,_=prepare_input(dataset/sample['image'],size)
        old=original_session.run(None,{original_session.get_inputs()[0].name:tensor})[0]
        new=trained.run(None,{trained.get_inputs()[0].name:tensor})[0]
        if old.shape!=new.shape or not np.isfinite(new).all():raise ValueError('Modelo treinado gerou saída incompatível.')
        if not np.allclose(old[:,:4],new[:,:4],atol=1e-5,rtol=1e-6):raise ValueError('Regressão de caixas alterada inesperadamente.')
        comparisons.append({'sample':sample['id'],'decision':sample['decision'],'original_max_score':float(old[0,4+sample['class_id']].max()),'trained_max_score':float(new[0,4+sample['class_id']].max())})
    if cancel():temporary.unlink(missing_ok=True);raise InterruptedError('Treinamento cancelado.')
    temporary.replace(target)
    report={'format':'SonarStudio-head-finetune-v1','namespace':manifest['namespace'],'device':'cpu','source_model':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'trained_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'changed_initializers':changed,'frozen_backbone_and_box_regression':True,'epochs':epochs,'samples':len(samples),'loss_before':before_loss,'loss_after':after_loss,'history':history,'supervised_region_before':before,'supervised_region_after':after,'inference_comparison':comparisons,'evaluation_scope':'training samples; not independent field accuracy','accuracy_improvement_claimed':False}
    Path(str(target)+'.training.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    return {'weights':str(target),'evidence':str(target)+'.training.json','loss_before':before_loss,'loss_after':after_loss}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--dataset',required=True);parser.add_argument('--source',required=True);parser.add_argument('--output',required=True);parser.add_argument('--epochs',type=int,default=8);parser.add_argument('--lr',type=float,default=.0005);args=parser.parse_args()
    os.environ['SONAR_TRAIN_CHILD']='1'
    result=finetune(args.dataset,args.source,args.output,args.epochs,args.lr,progress=lambda value:print(json.dumps({'event':'progress','value':value}),flush=True))
    print(json.dumps({'event':'result','value':result}),flush=True)
