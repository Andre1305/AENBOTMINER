"""Native-grid PINGMapper rectification with bounded warp buffers."""
import pathlib,sys,os,json,time,inspect,textwrap,threading
import numpy as np
from scipy.ndimage import map_coordinates
ROOT=pathlib.Path(__file__).resolve().parents[2]
os.environ.setdefault('MPLBACKEND','Agg')
os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'work/mpl'))

def stripe_warp(image,inverse_map,output_shape,**kwargs):
    h,w=map(int,output_shape)
    if h*w>500_000_000:raise ValueError('Bloco de retificação excede 500 milhões de pixels; reduza nchunk.')
    result=np.zeros((h,w),dtype=np.uint8)
    cols=np.arange(w,dtype=np.float64)
    for top in range(0,h,128):
        bottom=min(h,top+128)
        x,y=np.meshgrid(cols,np.arange(top,bottom,dtype=np.float64))
        points=np.column_stack((x.ravel(),y.ravel()))
        mapped=np.asarray(inverse_map(points))[:,:2]
        values=map_coordinates(image,[mapped[:,1],mapped[:,0]],order=1,mode='constant',cval=0.0,output=np.float64,prefilter=False)
        result[top:bottom]=np.clip(values,0,255).astype(np.uint8).reshape(bottom-top,w)
    return result

def install_stripe_renderer():
    import pingmapper.class_rectObj as mod
    from triangle_renderer import triangle_warp
    if getattr(mod.rectObj,'_native_renderer_installed',False):
        return mod.rectObj
    src=textwrap.dedent(inspect.getsource(mod.rectObj._rectSonRubber))
    src=src.replace('_MEM_CAP_BYTES = 1 * 1024**3','_MEM_CAP_BYTES = 2**60')
    namespace=dict(vars(mod));namespace['warp']=triangle_warp
    exec(compile(src,'native_rectification_adapter','exec'),namespace)
    mod.rectObj._rectSonRubber=namespace['_rectSonRubber']
    writer=textwrap.dedent(inspect.getsource(mod.rectObj._write_rect_geotiff))
    writer=writer.replace('zlevel=9,','zlevel=1,\n            num_threads=2,')
    exec(compile(writer,'native_lossless_writer','exec'),namespace)
    mod.rectObj._write_rect_geotiff=namespace['_write_rect_geotiff']
    mod.rectObj._native_renderer_installed=True
    return mod.rectObj

def rectify_project(project,target,resolution=None,progress=print,cancel=None):
    Rect=install_stripe_renderer()
    project=pathlib.Path(project);target=pathlib.Path(target);target.mkdir(parents=True,exist_ok=True)
    entries=[]
    for f in sorted((project/'meta').glob('*.meta')):
        obj=Rect(str(f))
        if str(obj.beamName).startswith(('ss_port','ss_star')):entries.append((f,obj))
    if not entries:raise ValueError('Projeto sem canais laterais preparados para retificação.')
    import pandas as pd
    native=float(pd.read_csv(entries[0][1].sonMetaFile,usecols=['pixM']).pixM.median())
    resolution=native if resolution is None else float(resolution)
    if resolution<native-1e-10:raise ValueError('Resolução abaixo da amostra nativa apenas aumenta o arquivo.')
    summary=target/'resultado.json'
    if summary.exists():
        cached=json.loads(summary.read_text(encoding='utf-8'))
        if cached.get('success') and abs(cached.get('resolution_m',-1)-resolution)<1e-10 and pathlib.Path(cached.get('mosaic','')).is_file():
            return cached
    total=sum(len(obj._getChunkID()) for _,obj in entries);done=0;start=time.time()
    for f,obj in entries:
        obj.projDir=str(target);obj.outDir=str(target/obj.beamName)
        pathlib.Path(obj.outDir).mkdir(exist_ok=True)
        obj.rect_wcp=False;obj.rect_wcr=True;obj.pix_res_son=resolution;obj.pix_res_map=resolution
        obj.egn=False;obj.remShadow=0;obj.export_16bit=False;obj._getSonColorMap('None')
        obj.smthTrkFile=str(project/'meta'/('Trackline_Smth_'+obj.beamName+'.csv'))
        for chunk in obj._getChunkID():
            if cancel and cancel():raise InterruptedError('Operação cancelada; blocos prontos preservados.')
            path=target/obj.beamName/'rect_wcr'/f'{target.name}_rect_wcr_{obj.beamName}_{int(chunk):05d}.tif'
            marker=path.with_suffix('.complete')
            if not path.exists() or not marker.exists():
                obj._rectSonRubber(int(chunk),50,True,False,True)
                marker.write_text('complete',encoding='utf-8')
            done+=1
            msg={'stage':'rectification','done':done,'total':total,'channel':obj.beamName,'elapsed_s':round(time.time()-start,1),'resolution_m':resolution}
            progress(json.dumps(msg),flush=True) if progress is print else progress(msg)
    from osgeo import gdal
    gdal.UseExceptions();gdal.SetCacheMax(256*1024**2)
    gdal.SetConfigOption('COMPRESS_OVERVIEW','DEFLATE')
    gdal.SetConfigOption('PREDICTOR_OVERVIEW','2')
    gdal.SetConfigOption('ZLEVEL_OVERVIEW','1')
    gdal.SetConfigOption('GDAL_NUM_THREADS','2')
    files=[str(f) for f in sorted(target.glob('ss_*/rect_wcr/*.tif')) if not f.stem.endswith('temp')]
    vrt=target/'mosaico-nativo.vrt'
    gdal.BuildVRT(str(vrt),files,options=gdal.BuildVRTOptions(resolution='user',xRes=resolution,yRes=resolution,srcNodata=0,VRTNodata=0))
    tif=target/'mosaico-nativo.tif'
    last_percent=[-1]
    def callback(value,message,data):
        if cancel and cancel():return 0
        msg={'stage':'mosaic','percent':round(value*100,1)}
        if progress is not print:
            progress(msg)
        elif int(msg['percent']) != last_percent[0]:
            print(json.dumps(msg),flush=True)
            last_percent[0]=int(msg['percent'])
        return 1
    ds=gdal.Translate(str(tif),str(vrt),options=gdal.TranslateOptions(format='GTiff',creationOptions=['BIGTIFF=YES','TILED=YES','BLOCKXSIZE=512','BLOCKYSIZE=512','COMPRESS=DEFLATE','PREDICTOR=2','ZLEVEL=1','NUM_THREADS=2'],callback=callback))
    last_percent[0]=-1
    ds.BuildOverviews('AVERAGE',[2,4,8,16,32,64,128],callback=callback);ds=None
    result={'success':True,'mosaic':str(tif),'source_project':str(project.resolve()),'tile_count':len(files),'resolution_m':resolution,'native_sampling_m':native,'elapsed_s':round(time.time()-start,1)}
    (target/'resultado.json').write_text(json.dumps(result,indent=2),encoding='utf-8');return result

if __name__=='__main__':
    project=pathlib.Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/'outputs/Rec00001-processado'
    target=pathlib.Path(sys.argv[2]) if len(sys.argv)>2 else ROOT/'outputs/Rec00001-resolucao-nativa'
    print(json.dumps(rectify_project(project,target)),flush=True)
