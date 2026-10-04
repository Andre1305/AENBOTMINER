"""Resumable real sonar triage; no fabricated detections or coordinates."""
from dataclasses import dataclass,asdict
from pathlib import Path
import hashlib,json,sqlite3,time
import numpy as np
from PIL import Image
from sonar_ai import detect_yolo,detect_multiclass,locate,MODEL,MULTI_MODEL

@dataclass(frozen=True)
class BatchSettings:
    model: str = 'sonar_ensemble'
    threshold: float = .4
    first_ping: int = 0
    last_ping: int = -1
    chunk_pings: int = 64
    overlap_pings: int = 16
    sample_factor: int = 1
    detailed: bool = False
    channel: str = 'both'
    remove_water: bool = False
    gain_normalization: bool = False
    fft_destripe: bool = False
    anomaly_triage: bool = False
    triage_contamination: float = .1
    triage_audit_every: int = 10

def windows(total,settings):
    if settings.triage_audit_every<1 or not .01<=settings.triage_contamination<=.3:raise ValueError('Configuração de triagem inválida.')
    first=max(0,settings.first_ping);last=total if settings.last_ping<0 else min(total,settings.last_ping+1)
    if first>=last:raise ValueError('Intervalo de pings vazio.')
    if settings.chunk_pings<16 or not 0<=settings.overlap_pings<settings.chunk_pings or settings.sample_factor<1:raise ValueError('Blocos, sobreposição ou amostragem inválidos.')
    final=max(first,last-settings.chunk_pings);step=(settings.chunk_pings-settings.overlap_pings)*settings.sample_factor
    starts=list(range(first,final+1,step))
    if not starts or starts[-1]!=final:starts.append(final)
    return [(start,min(start+settings.chunk_pings,last)) for start in starts]

def fingerprint(recording,settings):
    sources=[(str(p),p.stat().st_size,p.stat().st_mtime_ns) for p in recording.paths.values()]
    models=[]
    from model_registry import model_path
    for path in [model_path('ghostvision'),model_path('sonarvision')]:
        if path.exists():models.append((path.name,str(path.parent.name),hashlib.sha256(path.read_bytes()).hexdigest()))
    metadata=[(p.name,p.stat().st_size,p.stat().st_mtime_ns) for p in sorted((recording.project/'meta').glob('*.csv'))]
    value={'format':'batch-v1','algorithm_sha256':hashlib.sha256(Path(__file__).read_bytes()+(Path(__file__).parent/'sonar_radiometry.py').read_bytes()+(Path(__file__).parent/'sonar_ai.py').read_bytes()+(Path(__file__).parent/'recording.py').read_bytes()+(Path(__file__).parent/'sonar_triage.py').read_bytes()).hexdigest(),'dat':str(recording.dat),'dat_sha256':hashlib.sha256(recording.dat.read_bytes()).hexdigest(),'son':sources,'metadata':metadata,'settings':asdict(settings),'models':models,'temperature_c':recording.temperature}
    return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest(),value

def ground_mask(array,recording,info):
    a=np.array(array,copy=True)
    if info['remove_water']:return a
    columns=np.arange(a.shape[1]);part=info['part_width'];channel=info['channel']
    distance=np.where(columns<part,part-1-columns,columns-part) if channel=='both' else part-1-columns if channel.startswith('ss_port') else columns
    rows=recording.data.iloc[info['start']:info['end']];depth=rows['dep_m' if 'dep_m' in rows else 'inst_dep_m'].to_numpy(dtype=float)
    valid=np.isfinite(depth)&(depth>0);a[(distance[None,:]*info['sampling_m']<depth[:,None])|~valid[:,None]]=0
    return a

def physical_image(array,recording,info):
    rows=recording.data.iloc[info['start']:info['end']];intervals=np.diff(rows.time_s.to_numpy());positive=intervals[intervals>0]
    speed=float(rows.speed_ms.median());travel=speed*(float(np.median(positive)) if len(positive) else .1)
    ratio=max(.1,min(40.,travel/info['sampling_m'])) if np.isfinite(travel) and travel>0 else 1.
    height=max(1,round(len(array)*ratio));ratio=height/len(array)
    return np.asarray(Image.fromarray(array).resize((array.shape[1],height),Image.Resampling.BILINEAR)),ratio

def tile_count(shape,max_side,patch,stride):
    scale=min(1.,max_side/max(shape));h,w=[max(1,round(n*scale)) for n in shape]
    def count(n):return len(set(range(0,max(1,n-patch+1),stride))|{max(0,n-patch)})
    return count(h)*count(w)

def analyze_window(recording,array,info,settings,cancel=lambda:False):
    if settings.model not in ['sonar_ensemble','cascade','covos','objects']:raise ValueError('Modelo de varredura desconhecido.')
    if not 0<settings.threshold<1:raise ValueError('Limiar de confiança inválido.')
    from sonar_radiometry import radiometry,shadow_evidence
    masked=ground_mask(array,recording,info)
    if settings.gain_normalization or settings.fft_destripe:masked=radiometry(masked,info['part_width'],settings.gain_normalization,settings.fft_destripe)
    if settings.triage_audit_every<1 or not .01<=settings.triage_contamination<=.3:raise ValueError('Configuração de triagem inválida.')
    stats={'anomaly_calls':0,'yolo_calls':0,'multiclass_calls':0,'gated_windows':0,'unclassified':0,'discarded_water':0,'triage_full_audits':0,'triage_regions':0,'yolo_tiles':0,'multiclass_tiles':0,'triage_cost_fallbacks':0}
    if not masked.any():return [],stats
    if cancel():raise InterruptedError('Varredura cancelada.')
    physical,ratio=physical_image(masked,recording,info)
    detectors=[]
    if settings.model in ('covos','sonar_ensemble','cascade'):detectors.append((detect_yolo,'yolo_calls',4096 if settings.detailed else 2048,640,480,'yolo_tiles'))
    if settings.model in ('objects','sonar_ensemble','cascade'):detectors.append((detect_multiclass,'multiclass_calls',2048 if settings.detailed else 1024,256,192,'multiclass_tiles'))
    regions=[[0,0,physical.shape[1],physical.shape[0]]]
    if settings.anomaly_triage:
        audit=bool(info.get('triage_audit',False))
        if audit:stats['triage_full_audits']=1
        else:
            from sonar_triage import triage_regions
            regions,_=triage_regions(physical,settings.triage_contamination,cancel=cancel)
            stats['anomaly_calls']=1
            area=sum((x1-x0)*(y1-y0) for x0,y0,x1,y1 in regions)
            if area>physical.size*.65:regions=[[0,0,physical.shape[1],physical.shape[0]]]
            if not regions:stats['gated_windows']=1;return [],stats
            full_cost=sum(tile_count(physical.shape,size,patch,stride) for detector,key,size,patch,stride,tiles_key in detectors)
            roi_cost=0
            for x0,y0,x1,y1 in regions:
                shape=(y1-y0,x1-x0)
                for detector,key,size,patch,stride,tiles_key in detectors:
                    global_scale=min(1.,size/max(physical.shape));roi_size=max(1,round(max(shape)*global_scale))
                    roi_cost+=tile_count(shape,roi_size,patch,stride)
            if roi_cost>=full_cost:
                regions=[[0,0,physical.shape[1],physical.shape[0]]];stats['triage_cost_fallbacks']=1
        stats['triage_regions']=len(regions)
    items=[]
    for x0,y0,x1,y1 in regions:
        roi=physical[y0:y1,x0:x1]
        for detector,key,size,patch,stride,tiles_key in detectors:
            if cancel():raise InterruptedError('Varredura cancelada.')
            global_scale=min(1.,size/max(physical.shape));roi_size=max(1,round(max(roi.shape)*global_scale))
            detected,metadata=detector(roi,settings.threshold,max_tiles=192,cancel=cancel,max_side=roi_size)
            for item in detected:
                item=dict(item);item['box']=[float(v)+shift for v,shift in zip(item['box'],[x0,y0,x0,y0])]
                items.append(item)
            stats[key]+=1
            stats[tiles_key]+=metadata.get('tiles',0)
    from sonar_ai import nms
    grouped={}
    for item in items:grouped.setdefault(item.get('model'),[]).append(item)
    items=[item for group in grouped.values() for item in nms(group)]
    for item in items:item['box'][1]/=ratio;item['box'][3]/=ratio
    located=locate(items,recording,info);stats['discarded_water']=sum(bool(item['in_water_column']) for item in located)
    located=[item for item in located if not item['in_water_column']]
    for item in located:item.update(inference_row_ratio=ratio,frame_start=info['start'],frame_channel=info['channel'],frame_part_width=info['part_width'],frame_water=info['remove_water'],source_dat=str(recording.dat),batch_model=settings.model,status='pendente')
    for item in located:item['shadow_evidence']=shadow_evidence(array,item,info)
    return located,stats

def duplicate(a,b):
    if a['model']!=b['model'] or a.get('class_id')!=b.get('class_id') or a['channel']!=b['channel']:return False
    if a.get('frame_part_width')!=b.get('frame_part_width') or a.get('frame_water')!=b.get('frame_water'):return False
    if abs(a['ping']-b['ping'])>128:return False
    aa=np.array(a['box'],float);bb=np.array(b['box'],float);aa[[1,3]]+=a['frame_start'];bb[[1,3]]+=b['frame_start']
    size=np.maximum(0,np.minimum(aa[2:],bb[2:])-np.maximum(aa[:2],bb[:2]));intersection=size.prod()
    union=np.maximum(0,aa[2:]-aa[:2]).prod()+np.maximum(0,bb[2:]-bb[:2]).prod()-intersection
    return intersection/max(union,1e-9)>.3

def run_batch(recording,target,settings,progress=lambda value:None,emit=lambda items:None,cancel=lambda:False):
    target=Path(target);target.mkdir(parents=True,exist_ok=True);identity,source=fingerprint(recording,settings);db=target/'batch.sqlite'
    plan=windows(len(recording.data),settings);started=time.monotonic();initial_done=0;calls={};changed=[]
    connection=sqlite3.connect(db)
    try:
        connection.execute('PRAGMA journal_mode=WAL');connection.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT)');connection.execute('CREATE TABLE IF NOT EXISTS chunks (first_ping INTEGER PRIMARY KEY,last_ping INTEGER,stats TEXT)');connection.execute('CREATE TABLE IF NOT EXISTS candidates (id TEXT PRIMARY KEY,ping INTEGER,data TEXT)')
        connection.execute('CREATE INDEX IF NOT EXISTS candidate_ping ON candidates(ping)')
        old=connection.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
        if old and old[0]!=identity:raise ValueError('O checkpoint pertence a outra gravação, modelo ou configuração; escolha nova pasta.')
        connection.execute("INSERT OR REPLACE INTO meta VALUES ('identity',?)",(identity,));connection.execute("INSERT OR REPLACE INTO meta VALUES ('source',?)",(json.dumps(source),));connection.commit()
        finished={row[0] for row in connection.execute('SELECT first_ping FROM chunks')};initial_done=len(finished)
        objects={row[0]:json.loads(row[1]) for row in connection.execute('SELECT id,data FROM candidates')};emit(list(objects.values()))
        for row in connection.execute('SELECT stats FROM chunks'):
            for key,value in json.loads(row[0]).items():calls[key]=calls.get(key,0)+value
        progress({'stage':'batch','percent':100*len(finished)/len(plan),'done':len(finished),'total':len(plan),'candidates':len(objects),'status':'running','resumed':bool(finished),'elapsed_s':0})
        status='complete'
        for ordinal,(first,last) in enumerate(plan):
            if first in finished:continue
            if cancel():status='cancelled';break
            array,info=recording.waterfall(first+(last-first)//2,settings.channel,settings.remove_water,last-first)
            info['triage_audit']=ordinal%settings.triage_audit_every==0
            try:items,stats=analyze_window(recording,array,info,settings,cancel)
            except InterruptedError:status='cancelled';break
            if cancel():status='cancelled';break
            changed=[]
            recent=[objects[row[0]] for row in connection.execute('SELECT id FROM candidates WHERE ping BETWEEN ? AND ?',(first-128,last+128))]
            with connection:
                for item in items:
                    old=objects.get(item['id'])
                    if old is None:old=next((other for other in recent if duplicate(item,other)),None)
                    if old:
                        item['id']=old['id']
                        if old['score']>=item['score']:continue
                    objects[item['id']]=item;changed.append(item);recent.append(item)
                    connection.execute('INSERT OR REPLACE INTO candidates VALUES (?,?,?)',(item['id'],item['ping'],json.dumps(item)))
                connection.execute('INSERT INTO chunks VALUES (?,?,?)',(first,last,json.dumps(stats)))
            finished.add(first)
            for key,value in stats.items():calls[key]=calls.get(key,0)+value
            if changed:emit(changed)
            elapsed=time.monotonic()-started;processed=len(finished)-initial_done
            progress({'stage':'batch','percent':100*len(finished)/len(plan),'done':len(finished),'total':len(plan),'candidates':len(objects),'status':'running','resumed':initial_done>0,'elapsed_s':elapsed,'eta_s':elapsed/max(1,processed)*(len(plan)-len(finished))})
        if len(finished)==len(plan):status='complete'
        covered=0;end=-1
        for first,last in plan:
            if first not in finished:continue
            covered+=max(0,last-max(first,end));end=max(end,last)
        scope_last=len(recording.data) if settings.last_ping<0 else min(len(recording.data),settings.last_ping+1)
        result={'status':status,'success':status=='complete','identity':identity,'database':str(db),'settings':asdict(settings),'source_dat':str(recording.dat),'total_recording_pings':len(recording.data),'done':len(finished),'total':len(plan),'covered_pings':covered,'requested_pings':scope_last-max(0,settings.first_ping),'elapsed_this_run_s':time.monotonic()-started,'model_calls':calls,'candidates':len(objects),'triage_may_miss_objects':settings.anomaly_triage,'coverage_semantics':'read_ping_windows; optional triage restricts classifier spatial coverage','georeference':'approximate; no independently surveyed accuracy','objects':list(objects.values())}
        summary=dict(result);summary.pop('objects');temporary=target/'resultado.tmp';temporary.write_text(json.dumps(summary,indent=2),encoding='utf-8');temporary.replace(target/'resultado-batch.json')
        connection.execute("INSERT OR REPLACE INTO meta VALUES ('status',?)",(status,));connection.commit()
        return result
    finally:connection.close()
