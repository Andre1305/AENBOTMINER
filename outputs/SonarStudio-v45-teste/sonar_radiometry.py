"""Optional intensity corrections and review-only echo/shadow evidence."""
import numpy as np
from scipy.ndimage import gaussian_filter1d

def radiometry(array,part_width,gain=False,destripe=False):
    original=np.asarray(array);a=original.astype(np.float32).copy();valid=a>0
    if gain:
        for start,end in [(0,min(part_width,a.shape[1])),(min(part_width,a.shape[1]),a.shape[1])]:
            if end<=start:continue
            region=a[:,start:end];mask=valid[:,start:end]
            profile=np.divide((region*mask).sum(0),mask.sum(0),out=np.zeros(end-start,dtype=np.float32),where=mask.sum(0)>0)
            positive=profile[profile>0]
            if len(positive):
                baseline=float(np.median(positive));profile=np.where(profile>0,profile,baseline)
                smooth=gaussian_filter1d(profile,max(2,(end-start)/80))
                factors=np.clip(baseline/np.maximum(smooth,1),.5,2.)
                region*=factors[None,:]
    if destripe and len(a)>=16:
        # Remove only the high-frequency portion of ping-wide additive gain.
        count=valid.sum(1);profile=np.divide((a*valid).sum(1),count,out=np.zeros(len(a),np.float32),where=count>0)
        spectrum=np.fft.rfft(profile);frequencies=np.fft.rfftfreq(len(profile));spectrum[frequencies<.2]=0
        stripe=np.fft.irfft(spectrum,n=len(profile));a-=.35*stripe[:,None]
    a[~valid]=0
    return np.clip(a,0,255).astype(np.uint8)

def shadow_evidence(array,item,info):
    x0,y0,x1,y1=[int(round(v)) for v in item['box']];h,w=array.shape
    x0=max(0,x0);x1=min(w,x1);y0=max(0,y0);y1=min(h,y1)
    if x1<=x0 or y1<=y0:return {'status':'insufficient_pixels'}
    band=max(4,min(64,x1-x0));port=item['channel'].startswith('ss_port')
    left,right=(max(0,x0-band),x0) if port else (x1,min(w,x1+band))
    echo=array[y0:y1,x0:x1].astype(float);shadow=array[y0:y1,left:right].astype(float)
    echo=echo[echo>0];shadow=shadow[shadow>0]
    if len(echo)<8 or len(shadow)<8:return {'status':'insufficient_valid_pixels'}
    bright=float(np.percentile(echo,80));dark=float(np.median(shadow))
    return {'status':'review_only','echo_p80':bright,'outward_band_median':dark,'contrast':round((bright-dark)/max(bright,1),4),'outward_band_pixels':band,'height_estimated':False}
