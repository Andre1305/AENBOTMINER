"""Display geometry only: retain original echoes, sample grid and chronology."""
import numpy as np

def suppress_range_pulses(ranges,max_pings=2,relative_jump=.05):
    source=np.asarray(ranges,dtype=float);result=source.copy()
    if len(source)<3:return result
    boundaries=np.r_[0,np.flatnonzero(source[1:]!=source[:-1])+1,len(source)]
    for first,last in zip(boundaries[:-1],boundaries[1:]):
        if first==0 or last==len(source) or last-first>max_pings:continue
        before,after=source[first-1],source[last]
        if not np.isfinite(before) or not np.isfinite(after) or before<=0 or after<=0:continue
        baseline=max(before,after)
        if source[first]>baseline*(1+relative_jump):result[first:last]=baseline
    return result

def display_crop(info,mode=0):
    if not info['channel'].startswith('ss_') and info['channel']!='both':return None
    part=info['part_width'];visible=info.get('display_part_width',part) if mode==0 else part
    visible=min(part,max(1,int(visible)))
    if info['channel']=='both':return (part-visible,part+visible)
    if info['channel'].startswith('ss_port'):return (part-visible,part)
    return (0,visible)

def display_box(box,crop_left,height,reverse=False):
    x0,y0,x1,y1=box
    return [x0-crop_left,height-y1 if reverse else y0,x1-crop_left,height-y0 if reverse else y1]
