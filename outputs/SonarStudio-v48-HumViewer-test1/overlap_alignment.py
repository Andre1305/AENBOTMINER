"""Bounded rigid image registration, with measurable acceptance and provenance."""
from pathlib import Path
import json
import numpy as np
import cv2
import rasterio
from rasterio.transform import from_origin,Affine
from rasterio.windows import from_bounds
from scipy.optimize import minimize
from skimage.registration import phase_cross_correlation

def transformed(a,m):
    return cv2.warpAffine(a,np.asarray(m,dtype=np.float32),(a.shape[1],a.shape[0]),flags=cv2.INTER_LINEAR,borderMode=cv2.BORDER_CONSTANT,borderValue=0)

def metrics(reference,moving):
    mask=(reference>0)&(moving>0)
    if mask.sum()<256:return {'ncc':-1.,'rmse':None,'valid_pixels':int(mask.sum())}
    a=reference[mask].astype(float);b=moving[mask].astype(float);a-=a.mean();b-=b.mean()
    den=np.linalg.norm(a)*np.linalg.norm(b)
    return {'ncc':float(a@b/den) if den>1e-6 else -1.,'rmse':float(np.sqrt(np.mean((reference[mask].astype(float)-moving[mask])**2))),'valid_pixels':int(mask.sum())}

def estimate_rigid(reference,moving,max_angle=3.,max_shift_px=40.,cancel=lambda:False):
    ref=np.asarray(reference,np.float32);mov=np.asarray(moving,np.float32)
    if ref.shape!=mov.shape or ref.ndim!=2:raise ValueError('Sobreposição exige duas imagens 2D da mesma grade.')
    before=metrics(ref,mov);h,w=ref.shape;center=((w-1)/2,(h-1)/2)
    def matrix(parameters):
        angle,dx,dy=parameters;m=cv2.getRotationMatrix2D(center,float(angle),1);m[:,2]+=[dx,dy];return m
    def objective(parameters):
        if cancel():raise InterruptedError('Alinhamento cancelado.')
        after=metrics(ref,transformed(mov,matrix(parameters)))
        if after['valid_pixels']<max(256,before['valid_pixels']*.65):return 2.
        return 1-after['ncc']
    initial=np.array([0.,0.,0.]);best=objective(initial)
    for angle in np.linspace(-max_angle,max_angle,13):
        if cancel():raise InterruptedError('Alinhamento cancelado.')
        rotated=transformed(mov,matrix([angle,0,0]))
        # High-pass removes broad gain ramps; cross-correlation estimates translation.
        a=ref-cv2.GaussianBlur(ref,(0,0),3);b=rotated-cv2.GaussianBlur(rotated,(0,0),3)
        shift,_,_=phase_cross_correlation(a,b,upsample_factor=10,normalization=None)
        if np.max(np.abs(shift))>max_shift_px:continue
        candidate=np.array([angle,shift[1],shift[0]]);cost=objective(candidate)
        if cost<best:best=cost;initial=candidate
    solution=minimize(objective,initial,method='Powell',bounds=[(-max_angle,max_angle),(-max_shift_px,max_shift_px),(-max_shift_px,max_shift_px)],options={'maxiter':35,'xtol':.005,'ftol':1e-5})
    parameters=solution.x if objective(solution.x)<best else initial
    m=matrix(parameters);after=metrics(ref,transformed(mov,m))
    accepted=after['ncc']>=.55 and after['ncc']-before['ncc']>=.03 and after['valid_pixels']>=max(256,before['valid_pixels']*.65)
    return {'accepted':bool(accepted),'matrix_pixels':m.tolist(),'rotation_deg':float(parameters[0]),'offset_px':[float(parameters[1]),float(parameters[2])],'before':before,'after':after,'meaning':'relative image registration; not IMU correction or surveyed accuracy'}

def footprint_angle(a):
    y,x=np.nonzero(a>0)
    if len(x)<256:return None
    positions=np.column_stack([x,y])[::max(1,len(x)//5000)]
    _,vec=np.linalg.eigh(np.cov(positions.T));axis=vec[:,-1]
    return np.degrees(np.arctan2(axis[1],axis[0]))

def align_tiles(prepared,target,progress=lambda x:None,cancel=lambda:False,max_shift_m=8.,max_angle=3.):
    from osgeo import gdal
    gdal.UseExceptions()
    prepared=Path(prepared);target=Path(target);target.mkdir(parents=True,exist_ok=True)
    files=sorted(prepared.glob('ss_*/rect_wcr/*.tif'));records=[];corrected=[];footprints=[]
    if not files:raise ValueError('Sem blocos retificados para alinhar.')
    for number,path in enumerate(files):
        if cancel():raise InterruptedError('Alinhamento cancelado.')
        with rasterio.open(path) as moving:
            best=None;attempts=[]
            # Only spatially intersecting footprints with near-parallel major axes.
            intersections=[]
            for reference,box in footprints:
                area=max(0,min(moving.bounds.right,box.right)-max(moving.bounds.left,box.left))*max(0,min(moving.bounds.top,box.top)-max(moving.bounds.bottom,box.bottom))
                if area>0:intersections.append((area,reference))
            for _,reference in sorted(intersections,reverse=True)[:4]:
                with rasterio.open(reference) as ref:
                    if moving.crs!=ref.crs:
                        attempts.append({'reference':str(reference),'reason':'different CRS'});continue
                    left=max(moving.bounds.left,ref.bounds.left);right=min(moving.bounds.right,ref.bounds.right)
                    bottom=max(moving.bounds.bottom,ref.bounds.bottom);top=min(moving.bounds.top,ref.bounds.top)
                    if right-left<20*moving.res[0] or top-bottom<20*moving.res[0]:
                        attempts.append({'reference':str(reference),'reason':'overlap narrower than 20 native pixels'});continue
                    resolution=max(moving.res[0],(right-left)/512,(top-bottom)/512)
                    width=max(1,round((right-left)/resolution));height=max(1,round((top-bottom)/resolution))
                    if width*height<1024:
                        attempts.append({'reference':str(reference),'reason':'fewer than 1024 analysis pixels'});continue
                    a=ref.read(1,window=from_bounds(left,bottom,right,top,ref.transform),out_shape=(height,width))
                    b=moving.read(1,window=from_bounds(left,bottom,right,top,moving.transform),out_shape=(height,width))
                    ar=footprint_angle(a);br=footprint_angle(b)
                    if ar is None or br is None or abs((ar-br+90)%180-90)>20:
                        attempts.append({'reference':str(reference),'reason':'insufficient support or nonparallel footprint axes','reference_axis_deg':ar,'moving_axis_deg':br});continue
                    result=estimate_rigid(a,b,max_angle,max_shift_m/resolution,cancel)
                    result.update(reference=str(reference),moving=str(path),analysis_resolution_m=resolution)
                    attempts.append(result)
                    if result['accepted'] and (best is None or result['after']['ncc']>best[0]['after']['ncc']):
                        grid=from_origin(left,top,(right-left)/width,(top-bottom)/height)
                        m=np.asarray(result['matrix_pixels']);pixel=Affine(m[0,0],m[0,1],m[0,2],m[1,0],m[1,1],m[1,2]);world=grid*pixel*~grid
                        best=(result,world)
                if best:break
            destination=target/path.relative_to(prepared);destination.parent.mkdir(parents=True,exist_ok=True)
            if best:
                result,world=best;transform=world*moving.transform
                intermediate=destination.with_suffix('.vrt');vrt=gdal.Translate(str(intermediate),str(path),format='VRT');vrt.SetGeoTransform(transform.to_gdal());vrt=None
                out=gdal.Warp(str(destination),str(intermediate),dstSRS=moving.crs.to_wkt(),xRes=moving.res[0],yRes=moving.res[1],srcNodata=0,dstNodata=0,resampleAlg='bilinear',warpMemoryLimit=64,creationOptions=['TILED=YES','COMPRESS=DEFLATE','BIGTIFF=IF_SAFER'])
                if out is None:raise RuntimeError('Falha ao aplicar transformação ao bloco.')
                out=None;result['world_matrix']=[world.a,world.b,world.c,world.d,world.e,world.f];records.append(dict(result,attempts=attempts))
            else:
                import shutil
                shutil.copy2(path,destination);records.append({'accepted':False,'moving':str(path),'reason':'No reliable parallel overlap or insufficient NCC improvement','attempts':attempts})
            corrected.append(destination)
            with rasterio.open(destination) as ds:footprints.append((destination,ds.bounds))
        progress({'stage':'alignment','percent':100*(number+1)/len(files)})
    original=json.loads((prepared/'resultado.json').read_text(encoding='utf-8'))
    (target/'resultado.json').write_text(json.dumps(original,indent=2),encoding='utf-8')
    (target/'alignment-evidence.json').write_text(json.dumps({'method':'rigid cross-correlation + bounded NCC refinement','source':str(prepared),'tiles':records,'accepted':sum(r['accepted'] for r in records),'absolute_accuracy_measured':False},indent=2),encoding='utf-8')
    return target
