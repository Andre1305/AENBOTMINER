"""Depth selection of complete pings; never synthesizes missing acoustic returns."""
import numpy as np

def validate_limits(minimum=None,maximum=None):
    for value in (minimum,maximum):
        if value is not None and (not np.isfinite(value) or value<0):raise ValueError('Profundidade deve ser finita e não negativa.')
    if minimum is not None and maximum is not None and maximum<=minimum:raise ValueError('Profundidade máxima deve superar a mínima.')
    return None if minimum is None and maximum is None else (minimum,maximum)

def depth_selection(metadata,limits):
    if limits is None:return np.ones(len(metadata),dtype=np.bool_)
    name='dep_m' if 'dep_m' in metadata else 'inst_dep_m'
    depths=metadata[name].to_numpy(dtype=float)
    valid=np.isfinite(depths)&(depths>0)
    low,high=limits
    if low is not None:valid &= depths>=low
    if high is not None:valid &= depths<=high
    return valid
