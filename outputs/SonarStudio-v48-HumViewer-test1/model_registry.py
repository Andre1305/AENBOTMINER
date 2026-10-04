"""Explicit local selection of specialized fine-tuned weights, originals preserved."""
from pathlib import Path
import json,hashlib
from functools import lru_cache

CONFIG=Path(__file__).parent/'model-selection.json'

@lru_cache(maxsize=16)
def checked_hash(path,size,mtime):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def model_path(namespace):
    original=Path(__file__).parent/'models'/('gv-yolo12' if namespace=='ghostvision' else 'sonarvision-v6')/'weights.onnx'
    if not CONFIG.exists():return original
    data=json.loads(CONFIG.read_text(encoding='utf-8'));entry=data.get(namespace)
    if not entry:return original
    path=Path(entry['path'])
    if not path.exists():raise ValueError('Modelo treinado selecionado não encontrado; restaure o original.')
    stat=path.stat()
    if checked_hash(str(path),stat.st_size,stat.st_mtime_ns)!=entry['sha256']:raise ValueError('Modelo selecionado foi alterado; selecione novamente pesos com proveniência válida.')
    return path

def activate(namespace,path=None):
    import onnxruntime as ort
    if namespace not in ('ghostvision','sonarvision'):raise ValueError('Namespace inválido.')
    data=json.loads(CONFIG.read_text(encoding='utf-8')) if CONFIG.exists() else {}
    if path:
        path=Path(path).resolve();report=json.loads(Path(str(path)+'.training.json').read_text(encoding='utf-8'))
        if report.get('namespace')!=namespace or report.get('trained_sha256')!=hashlib.sha256(path.read_bytes()).hexdigest():raise ValueError('Proveniência ou hash do modelo treinado incompatível.')
        session=ort.InferenceSession(str(path),providers=['CPUExecutionProvider']);size=640 if namespace=='ghostvision' else 256
        if session.get_inputs()[0].shape!=[1,3,size,size]:raise ValueError('Forma da entrada incompatível.')
        data[namespace]={'path':str(path),'sha256':report['trained_sha256']}
    else:data.pop(namespace,None)
    temporary=CONFIG.with_suffix('.tmp');temporary.write_text(json.dumps(data,indent=2),encoding='utf-8');temporary.replace(CONFIG)
    return str(model_path(namespace))
