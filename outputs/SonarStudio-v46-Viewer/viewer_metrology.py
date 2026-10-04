"""Viewer measurement geometry. Native samples are not positional accuracy."""
import numpy as np,math

def horizontal_range(slant,altitude):
 r=float(slant);h=float(altitude)
 if not np.isfinite([r,h]).all() or h<=0 or r<h:raise ValueError('Retorno na coluna de água ou altura transdutor-fundo inválida.')
 return math.sqrt(max(0,r*r-h*h))

def point_from_pixel(rec,info,x,y,altitude_override=None):
 ping=int(np.clip(round(info['start']+y),info['start'],info['end']-1));row=rec.data.iloc[ping];part=info['part_width'];channel=info['channel']
 if channel=='both':port=x<part;sample=part-1-x if port else x-part
 else:port=channel.startswith('ss_port');sample=part-1-x if port else x
 key=next((k for k in rec.channels if k.startswith('ss_port' if port else 'ss_star')),None)
 timing=info.get('channel_timing',{}).get(key,{})
 if timing and not timing['valid_rows'][ping-info['start']]:raise ValueError('Canal sem amostra sincronizada neste ping.')
 h=float(altitude_override) if altitude_override is not None else float(row.inst_dep_m)
 ground=sample*info['sampling_m'] if info['remove_water'] else horizontal_range(sample*info['sampling_m'],h)
 if ground<0:raise ValueError('Ponto fora do alcance de fundo.')
 slant=math.hypot(ground,h) if info['remove_water'] else sample*info['sampling_m']
 heading=float(row.get('instr_heading',np.nan));heading_source='instrument'
 if not np.isfinite(heading):
  lo=max(0,ping-5);hi=min(len(rec.xy)-1,ping+5);delta=rec.xy[hi]-rec.xy[lo]
  if not np.isfinite(delta).all() or np.linalg.norm(delta)<.01:raise ValueError('Sem orientação suficiente para projetar o alvo.')
  heading=math.degrees(math.atan2(delta[0],delta[1]));heading_source='navigation'
 angle=math.radians(heading);side=-1 if port else 1;east=float(row.e)+side*math.cos(angle)*ground;north=float(row.n)-side*math.sin(angle)*ground
 if not np.isfinite([east,north]).all():raise ValueError('Navegação inválida neste ping.')
 return {'ping':ping,'time_s':float(row.time_s),'side':'port' if port else 'starboard','slant_m':float(slant),'ground_m':float(ground),'altitude_m':h,'altitude_source':'manual transducer-to-bottom' if altitude_override is not None else 'inst_dep_m; verify transducer reference','east':east,'north':north,'heading_deg':heading,'heading_source':heading_source,'incidence_from_vertical_deg':math.degrees(math.atan2(ground,h)),'accuracy':'approximate navigation projection, not surveyed position'}

def measure_points(rec,a,b):
 lo,hi=sorted([a['ping'],b['ping']]);xy=rec.xy[lo:hi+1];times=rec.data.time_s.iloc[lo:hi+1].to_numpy();source='navigation'
 if np.isfinite(xy).all():along=float(np.linalg.norm(np.diff(xy,axis=0),axis=1).sum())
 else:
  speeds=rec.data.speed_ms.iloc[lo:hi+1].to_numpy();dt=np.diff(times)
  if not np.isfinite(speeds).all() or np.any(dt<0):raise ValueError('Navegação e velocidade indisponíveis para medir.')
  along=float(np.sum((speeds[:-1]+speeds[1:])*.5*dt));source='integrated speed × timestamp (fallback)'
 signed=lambda p:(-1 if p['side']=='port' else 1)*p['ground_m']
 return {'kind':'horizontal distance','distance_m':math.hypot(b['east']-a['east'],b['north']-a['north']),'cross_track_delta_m':abs(signed(b)-signed(a)),'navigation_path_m':along,'longitudinal_source':source,'a':a,'b':b,'assumptions':['flat bottom at each selected return','raw instrument altitude, no tide datum substitution','navigation/heading not independently surveyed'],'accuracy_verified':False}

def shadow_scenarios(a,b,sampling_m,depth_uncertainty=.2,picking_samples=2,slope_deg=2.):
 if a['side']!=b['side'] or abs(a['ping']-b['ping'])>2:raise ValueError('Selecione eco e fim da sombra no mesmo lado e em até dois pings de diferença.')
 r=a['slant_m'];end=b['slant_m'];h=a['altitude_m']
 if end<=r:raise ValueError('O fim da sombra deve estar mais distante do sonar que o topo do eco.')
 estimate=h*(1-r/end);picking=abs(float(sampling_m)*picking_samples)
 terrain=abs(a['ground_m']*math.tan(math.radians(slope_deg)));values=[]
 for height in [max(.01,h-depth_uncertainty-terrain),h+depth_uncertainty+terrain]:
  for echo in [max(0,r-picking),r+picking]:
   for finish in [end-picking,end+picking]:
    if finish>echo and finish>0:values.append(height*(1-echo/finish))
 if not values:raise ValueError('Cenários de sombra incompatíveis.')
 return {'kind':'shadow height scenarios','height_m':estimate,'scenario_min_m':min(values),'scenario_max_m':max(values),'interval_type':'scenario envelope, not statistical confidence','formula':'H * (1 - slant_top_echo / slant_shadow_end)','depth_uncertainty_m':depth_uncertainty,'picking_uncertainty_samples':picking_samples,'terrain_slope_scenario_deg':slope_deg,'terrain_policy':'conservative local-altitude variation ± ground_range*tan(slope), not solved bathymetric surface','incidence_from_vertical_deg':b['incidence_from_vertical_deg'],'a':a,'b':b,'assumptions':['first click identifies top echo','second click identifies acoustic shadow end','same radial direction','flat nominal bottom; scenario variation is explicit','height above local floor, not water datum'],'accuracy_verified':False}
