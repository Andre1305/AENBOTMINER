"""Complete, explicit per-recording beta workflow; source files remain read-only."""
from dataclasses import dataclass,asdict
from pathlib import Path
import json,hashlib,time
from recording import import_recording,prepare_mapping,cache_project
from bathymetry import BathySettings,generate
from batch_ai import BatchSettings,run_batch
from image_processing import ImageSettings,colorize

@dataclass(frozen=True)
class WorkflowSettings:
 temperature: float=10.
 mosaic_resolution: float|None=None
 bathy_resolution: float=2.
 google_resolution: float=1.
 mosaic: bool=True
 bathymetry: bool=True
 ai: bool=True
 exports: bool=True
 ai_model: str='sonar_ensemble'
 mosaic_depth_min: float|None=None
 mosaic_depth_max: float|None=None
 ai_first_ping: int=0
 ai_last_ping: int=-1
 align_overlap: bool=False

 def json(self):return asdict(self)

def workflow_target(dat,settings):
 base=cache_project(dat,settings.temperature)
 digest=hashlib.sha256(json.dumps(settings.json(),sort_keys=True).encode()).hexdigest()[:12]
 return base.with_name(base.name+'-beta-'+digest)

def reusable_exports(folder,rec,bathy,contours,mosaic,resolution):
 """Accept only readable, complete outputs with a matching processing recipe."""
 import zipfile,xml.etree.ElementTree as ET
 from bathy_review import review_signature
 from osgeo import ogr
 ogr.UseExceptions()
 folder=Path(folder)
 try:
  for ext in ['gpx','kml','shp','gpkg','kmz']:
   path=folder/('levantamento.'+ext)
   recipe=json.loads(Path(str(path)+'.recipe.json').read_text(encoding='utf-8'))
   if recipe.get('sounding_review_sha256')!=review_signature(rec) or recipe.get('source_dat')!=str(rec.dat) or recipe.get('bathymetry_settings')!=bathy.json() or recipe.get('contours')!=(str(contours) if contours else None):return False
   if ext in ['shp','gpkg','kml']:
    ds=ogr.Open(str(path))
    if ds is None or ds.GetLayer(0).GetFeatureCount()<2:return False
    ds=None
    companion=path.with_name(path.stem+'-isobatas'+('.shp' if ext=='shp' else '.kml' if ext=='kml' else '.gpkg'))
    if contours and not companion.is_file():return False
    if ext=='shp':
     for stem in ['levantamento','levantamento-trajetoria']+(['levantamento-isobatas'] if contours else []):
      for suffix in ['.shp','.shx','.dbf','.prj']:
       if not (folder/(stem+suffix)).is_file():return False
   elif ext=='gpx':
    if len(ET.parse(path).findall('.//{http://www.topografix.com/GPX/1/1}trkpt'))<2:return False
   else:
    with zipfile.ZipFile(path) as z:
     if z.testzip() is not None or 'doc.kml' not in z.namelist():return False
  if not (folder/'sondagens.csv').is_file():return False
  if mosaic:
   for ext in ['kmz','kml']:
    path=folder/('mosaico-copper.'+ext);recipe=json.loads(Path(str(path)+'.recipe.json').read_text(encoding='utf-8'))
    if recipe.get('source')!=str(mosaic) or recipe.get('resolution_m')!=resolution:return False
    if ext=='kmz':
     with zipfile.ZipFile(path) as z:
      if z.testzip() is not None:return False
    else:
     pending=[path];seen=set()
     while pending:
      current=pending.pop()
      if current in seen:continue
      seen.add(current)
      for node in ET.parse(current).iter():
       if node.tag.endswith('}href') and node.text and not node.text.startswith('http'):
        child=(current.parent/node.text).resolve()
        if not child.is_file():return False
        if child.suffix=='.kml':pending.append(child)
  return True
 except (OSError,ValueError,KeyError,RuntimeError,ET.ParseError,zipfile.BadZipFile):return False


def run_workflow(dat,target,settings=WorkflowSettings(),note=lambda s:None,progress=lambda p:None,cancel=lambda:False,project=None):
 from mosaic_depth import validate_limits
 validate_limits(settings.mosaic_depth_min,settings.mosaic_depth_max)
 from native_mosaic import rectify_project
 from pro_exports import raster_google,vectors
 from PIL import Image
 target=Path(target);target.mkdir(parents=True,exist_ok=True)
 identity={'source_cache':str(cache_project(dat,settings.temperature)),'settings':settings.json()}
 manifest_path=target/'projeto-beta.json';previous_products={}
 if manifest_path.exists():
  old=json.loads(manifest_path.read_text(encoding='utf-8'))
  previous_products=old.get('products',{})
  if old.get('identity')!=identity:raise ValueError('Esta pasta pertence a outra gravação ou configuração. Use uma nova pasta.')
  if project is None and old.get('metadata_project'):
   previous=Path(old['metadata_project'])
   if (previous/'meta/DAT_meta.csv').is_file():project=previous
 state={'version':'4.5-MAX','source_dat':str(Path(dat).resolve()),'identity':identity,'status':'running','completed_stages':[],'products':{}}
 def save():
  temporary=manifest_path.with_suffix('.json.tmp');temporary.write_text(json.dumps(state,indent=2,ensure_ascii=False),encoding='utf-8');temporary.replace(manifest_path)
 stages=['import','previews']+(['mosaic'] if settings.mosaic else [])+(['bathymetry'] if settings.bathymetry else [])+(['exports'] if settings.exports else [])+(['ai'] if settings.ai else [])
 def step(name):
  if cancel():raise InterruptedError('Processamento cancelado; produtos concluídos preservados.')
  note('Etapa: '+name);state['current_stage']=name;save()
 def done(name):state['completed_stages'].append(name);save();progress({'stage':name,'percent':100*len(state['completed_stages'])/len(stages)})
 maximum_percent=[0.]
 def update(value):
  part=float(value.get('percent',100*value.get('done',0)/max(1,value.get('total',1))))
  maximum_percent[0]=max(maximum_percent[0],100*(len(state['completed_stages'])+part/100)/len(stages))
  progress({**value,'percent':maximum_percent[0],'stage_percent':part})
 rec=None;started=time.monotonic()
 try:
  step('import');rec=import_recording(dat,project,note,settings.temperature);state['metadata_project']=str(rec.project);state['pings']=len(rec.data);state['native_sampling_m']=rec.sampling_m;state['crs']=str(rec.crs);done('import')
  step('previews');folder=target/'sonar';folder.mkdir(exist_ok=True)
  channels=(['both'] if any(k.startswith('ss_port') for k in rec.channels) and any(k.startswith('ss_star') for k in rec.channels) else [])+[k for k in rec.channels if k.startswith('ds')]
  for channel in channels:
   if cancel():raise InterruptedError('Processamento cancelado.')
   a,info=rec.waterfall(len(rec.data)//2,channel,channel=='both',160)
   Image.fromarray(colorize(a,ImageSettings(palette='Âmbar'))).save(folder/(channel+'.png'))
   (folder/(channel+'.json')).write_text(json.dumps({**info,'source_dat':str(rec.dat),'georeferenced':False},indent=2),encoding='utf-8')
  state['products']['sonar_previews']=str(folder);done('previews')
  if settings.mosaic:
   step('mosaic');mapped=prepare_mapping(rec,settings.temperature,note)
   resolution=None if settings.mosaic_resolution is None else max(rec.sampling_m,settings.mosaic_resolution)
   result=rectify_project(mapped,target/'mosaico',resolution,update,cancel,depth_min=settings.mosaic_depth_min,depth_max=settings.mosaic_depth_max)
   if settings.align_overlap:
    from mosaic_blend import recombine
    result=recombine(target/'mosaico',target/'mosaico-alinhado',alignment=True,progress=update,cancel=cancel)
   state['products']['mosaic']=result['mosaic'];state['mosaic_result']=result;done('mosaic')
  bathy=BathySettings(resolution=settings.bathy_resolution,interpolation='TIN',smoothing_m=settings.bathy_resolution)
  if settings.bathymetry:
   step('bathymetry');result=generate(rec,target/'batimetria',bathy,update,cancel);state['products']['bathymetry']=result['raster'];state['products']['contours']=result['contours'];state['bathy_result']=result;done('bathymetry')
  if settings.exports:
   step('exports');folder=Path(previous_products.get('exports',target/'exportacoes'))
   reusable=reusable_exports(folder,rec,bathy,state['products'].get('contours'),state['products'].get('mosaic'),settings.google_resolution)
   if not reusable and folder.exists():folder=target/('exportacoes-'+str(time.time_ns()))
   folder.mkdir(exist_ok=True)
   contours=state['products'].get('contours')
   for extension in ['gpx','kml','shp','gpkg','kmz']:
    if cancel():raise InterruptedError('Processamento cancelado.')
    path=folder/('levantamento.'+extension)
    if not reusable:vectors(rec,path,bathy,contours,update,cancel)
   if state['products'].get('mosaic'):
    for extension in ['kmz','kml']:
     if not reusable:raster_google(state['products']['mosaic'],folder/('mosaico-copper.'+extension),ImageSettings(palette='Âmbar'),settings.google_resolution,progress=update,cancel=cancel)
   if not reusable:rec.export_csv(folder/'sondagens.csv')
   else:note('Exportações completas e compatíveis encontradas; retomando sem sobrescrever.')
   state['products']['exports']=str(folder);done('exports')
  if settings.ai:
   step('ai');result=run_batch(rec,target/'ia',BatchSettings(model=settings.ai_model,first_ping=settings.ai_first_ping,last_ping=settings.ai_last_ping),update,cancel=cancel)
   path=target/'ia/candidatos.json';path.write_text(json.dumps({'dat':str(rec.dat),'objects':result['objects']},indent=2,ensure_ascii=False),encoding='utf-8');state['products']['candidates']=str(path)
   state['ai_result']={key:value for key,value in result.items() if key!='objects'}
   if not result['success']:raise InterruptedError('Varredura cancelada com checkpoint.')
   if settings.exports:
    from object_review import export_objects
    folder=Path(state['products']['exports'])/('objetos-ia-'+str(time.time_ns()));folder.mkdir(exist_ok=True)
    for suffix in ['geojson','shp','kml','gpx']:export_objects(result['objects'],folder/('candidatos.'+suffix))
    state['products']['candidate_exports']=str(folder)
   done('ai')
  state['status']='complete';state['success']=True;progress({'stage':'complete','percent':100})
 except Exception as error:
  state['status']='cancelled' if isinstance(error,InterruptedError) else 'failed';state['success']=False;state['error']=str(error);raise
 finally:
  state['elapsed_s']=time.monotonic()-started;save()
  if rec:rec.close()
 return state
