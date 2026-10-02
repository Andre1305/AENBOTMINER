"""Reproducible, non-generative sonar filters. Original arrays remain immutable."""
from dataclasses import dataclass, asdict
import numpy as np
from scipy.ndimage import median_filter, gaussian_filter


@dataclass(frozen=True)
class ImageSettings:
    method: str = 'Original'
    strength: float = .35
    destripe: float = 0.
    clahe: float = 0.
    sharpen: float = 0.
    black: float = 0.
    white: float = 255.
    gamma: float = 1.
    palette: str = 'Âmbar'

    def json(self):
        return asdict(self)


def copper(values):
    """Matplotlib's copper transfer curve, independent of plotting runtime."""
    x=np.clip(np.asarray(values),0,1)
    return np.stack([np.minimum(1,1.25*x),.7812*x,.4975*x],axis=-1)


def enhance(image, settings=ImageSettings(), valid=None):
    if settings.white <= settings.black:
        raise ValueError('O nível branco precisa ser maior que o nível preto.')
    src=np.asarray(image,dtype=np.float32)
    mask=np.ones(src.shape,bool) if valid is None else np.asarray(valid,bool)
    x=src.copy()
    strength=float(np.clip(settings.strength,0,1))
    if settings.destripe > 0:
        # Sonar waterfall rows are individual pings. Robust gain suppresses banding.
        a=np.where(mask & (x>0),x,np.nan)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',RuntimeWarning)
            med=np.nanmedian(a,axis=1)
        good=np.isfinite(med)&(med>0)
        if good.any():
            fill=np.interp(np.arange(len(med)),np.flatnonzero(good),med[good])
            smooth=median_filter(fill,size=31,mode='nearest')
            gain=np.clip(smooth/np.maximum(fill,1),.5,2)
            x *= (1 + float(settings.destripe)*(gain-1))[:,None]
    if settings.method == 'Mediana':
        x=(1-strength)*x+strength*median_filter(x,size=3,mode='reflect')
    elif settings.method == 'Lee — speckle':
        mean=gaussian_filter(x,1.2); var=np.maximum(gaussian_filter(x*x,1.2)-mean*mean,0)
        noise=float(np.median(var[mask])) if mask.any() else 0
        weight=np.clip((var-noise)/(var+1e-6),0,1)
        filtered=mean+weight*(x-mean)
        x=(1-strength)*x+strength*filtered
    elif settings.method == 'Non-local means':
        from skimage.restoration import denoise_nl_means
        x=denoise_nl_means(x,h=max(.1,strength*18),patch_size=3,patch_distance=4,
                          fast_mode=True,preserve_range=True,channel_axis=None)
    elif settings.method == 'Wavelet':
        from skimage.restoration import denoise_wavelet
        x=denoise_wavelet(x,sigma=max(.1,strength*12),rescale_sigma=True,
                         channel_axis=None,method='BayesShrink',mode='soft')
    elif settings.method != 'Original':
        raise ValueError('Filtro desconhecido: '+settings.method)
    x=np.clip((x-settings.black)/max(1.,settings.white-settings.black),0,1)
    if settings.clahe>0:
        from skimage.exposure import equalize_adapthist
        adjusted=equalize_adapthist(x,kernel_size=64,clip_limit=.015,nbins=256)
        x=(1-settings.clahe)*x+settings.clahe*adjusted
    if settings.sharpen>0:
        x=np.clip(x+settings.sharpen*(x-gaussian_filter(x,1)),0,1)
    x=np.clip(x,0,1)**(1/max(.1,settings.gamma))
    x[~mask]=0
    return np.round(x*255).astype(np.uint8)


def colorize(image, settings=ImageSettings(), valid=None, already_enhanced=False):
    gray=np.asarray(image,dtype=np.uint8) if already_enhanced else enhance(image,settings,valid)
    x=gray.astype(np.float32)/255
    rgb=copper(x) if settings.palette=='Âmbar' else np.repeat(x[...,None],3,axis=-1)
    alpha=np.full(gray.shape,255,np.uint8) if valid is None else np.asarray(valid,np.uint8)*255
    return np.dstack([np.round(rgb*255).astype(np.uint8),alpha])
