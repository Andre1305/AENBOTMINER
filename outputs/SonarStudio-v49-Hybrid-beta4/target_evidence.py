"""Native target images and private evidence export, separate from display previews."""
import io,base64,json,subprocess,hashlib
from pathlib import Path
import numpy as np
from PIL import Image
from image_processing import colorize,ImageSettings

def target_image(rec,ping,channel='both',box=None,frame_start=None,part_width=None,remove_water=False,focus=None):
 count=min(160,len(rec.data));center=int(ping);raw,info=rec.waterfall(center,channel,remove_water,count)
 if focus and box is None:
  sample=focus['ground_m']/info['sampling_m'] if remove_water else focus['slant_m']/info['sampling_m'];x=info['part_width']-1-sample if focus['side']=='port' else sample+(info['part_width'] if channel=='both' else 0);y=focus['ping']-info['start'];lo=max(0,int(x)-256);hi=min(raw.shape[1],int(x)+257);top=max(0,int(y)-32);bottom=min(len(raw),int(y)+33)
  if hi<=lo or bottom<=top:raise ValueError('Ponto de interesse fora do trecho nativo.')
  raw=raw[top:bottom,lo:hi];info=dict(info,crop=[lo,top,hi,bottom])
 elif box is not None:
  x0,y0,x1,y1=box;y0+=int(frame_start)-info['start'];y1+=int(frame_start)-info['start']
  if part_width and part_width!=info['part_width'] and (channel=='both' or channel.startswith('ss_port')):
   # Local reach differs: port is aligned to nadir, starboard to its left edge.
   x0+=info['part_width']-part_width;x1+=info['part_width']-part_width
  margin=max(8,round((x1-x0)*.2));lo=max(0,int(x0)-margin);hi=min(raw.shape[1],int(np.ceil(x1))+margin);top=max(0,int(y0)-8);bottom=min(len(raw),int(np.ceil(y1))+8)
  if hi<=lo or bottom<=top:raise ValueError('A caixa não intersecta o trecho nativo do alvo.')
  raw=raw[top:bottom,lo:hi];info=dict(info,crop=[lo,top,hi,bottom])
 return raw,info

def png_echo(array,palette='Âmbar'):
 image=Image.fromarray(colorize(array,ImageSettings(palette=palette),already_enhanced=True));buffer=io.BytesIO();image.save(buffer,format='PNG');return buffer.getvalue()

def export_pdf(rec,item,output,palette='Âmbar'):
 from private_pdf import key_path,pdf_python
 manual='metadata' in item;meta=dict(item['metadata']) if manual else dict(item);ping=int(item['ping']);row=rec.data.iloc[ping]
 array,info=target_image(rec,ping,meta.get('frame_channel',meta.get('channel','both')),None if manual else meta.get('box'),meta.get('frame_start'),meta.get('frame_part_width'),meta.get('frame_water',False),meta.get('target_point'))
 # Only explicitly stored measurements may be reported as target height.
 meta.update(source_name=rec.dat.name,ping=ping,label=item.get('tag',meta.get('label','Alvo')),status=meta.get('status','manual'),date=str(row.get('date','')),time=str(row.get('time','')),depth_instrument_m=float(row.inst_dep_m),latitude=float(meta.get('latitude',meta.get('lat',row.lat))),longitude=float(meta.get('longitude',meta.get('lon',row.lon))),channel=info['channel'],native_frame=info,source_dat_sha256=hashlib.sha256(rec.dat.read_bytes()).hexdigest(),preview=False,photo_processing='native DN; palette only; no contrast/gamma/denoise',coordinate_reference=meta.get('coordinate_reference','Projeção aproximada do alvo, não posição aferida' if not manual else 'Navegação do barco; alvo não projetado'))
 photo=png_echo(array,palette);job={'output':str(output),'key':str(key_path()),'metadata':meta,'png':base64.b64encode(photo).decode('ascii')}
 result=subprocess.run([str(pdf_python()),str(Path(__file__).parent/'card_pdf.py')],input=json.dumps(job,ensure_ascii=False,allow_nan=False).encode('utf-8'),stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW,timeout=120)
 if result.returncode:raise RuntimeError(result.stderr.decode('utf-8',errors='replace')[-2000:])
 return json.loads(result.stdout.decode('utf-8'))
