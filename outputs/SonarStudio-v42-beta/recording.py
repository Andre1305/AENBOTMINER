"""Humminbird recordings, persistent metadata cache, and direct sonar reads."""
from __future__ import annotations

import hashlib
import json
import pathlib
import threading
from collections import OrderedDict

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

ROOT = pathlib.Path(__file__).resolve().parents[2]
PROCESS_LOCK = threading.Lock()


def source_files(dat):
    dat = pathlib.Path(dat).resolve()
    if not dat.is_file() or dat.suffix.lower() != '.dat':
        raise ValueError('Selecione um arquivo .DAT extraído da gravação.')
    folder = dat.with_suffix('')
    files = sorted(p for p in folder.glob('*') if p.suffix.lower() == '.son')
    if not files:
        raise ValueError(f'Não encontrei os .SON na pasta {folder}. Mantenha o .DAT ao lado da pasta de mesmo nome.')
    return dat, files


def cache_project(dat, temperature=10.0):
    dat, files = source_files(dat)
    signature = [str(dat), hashlib.sha256(dat.read_bytes()).hexdigest(), float(temperature)]
    signature += [(str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in files]
    digest = hashlib.sha256(json.dumps(signature).encode()).hexdigest()[:16]
    return ROOT / 'outputs' / 'sonar-projects' / f'{dat.stem}-{digest}'


def import_recording(dat, project=None, note=lambda s: None, temperature=10.0):
    dat, companions = source_files(dat)
    note(f'{dat.name}: pasta associada encontrada automaticamente, {len(companions)} canais SON. Apenas metadados e trechos solicitados serão lidos.')
    project = pathlib.Path(project) if project else cache_project(dat, temperature)
    project.parent.mkdir(parents=True,exist_ok=True)
    complete=(project/'meta/DAT_meta.csv').is_file() and all(list((project/'meta').glob(son.stem+'_*_meta.csv')) for son in companions)
    if not complete:
        from joblib import Parallel
        import pingverter.converter as converter
        from pingverter import hum2pingmapper

        def sequential(*args, **kwargs):
            kwargs['n_jobs'] = 1
            return Parallel(*args, **kwargs)

        with PROCESS_LOCK:
            converter.Parallel = sequential
            note('Lendo cabeçalhos dos canais. A primeira importação pode levar alguns minutos…')
            hum2pingmapper(str(dat), str(project), 500, float(temperature), False)
    (project / 'source.json').write_text(json.dumps({'dat': str(dat), 'temperature_c': temperature}, indent=2), encoding='utf-8')
    return Recording(dat, project)


class Recording:
    def __init__(self, dat, project):
        self.dat, files = source_files(dat)
        self.project = pathlib.Path(project)
        self.channels = {}
        self.paths = {}
        self.maps = {}
        self.block_cache=OrderedDict()
        self.cache_bytes=0
        self.read_lock=threading.RLock()
        for son in files:
            matches = sorted((self.project / 'meta').glob(son.stem + '_*_meta.csv'))
            if not matches:
                continue
            name = matches[0].stem[len(son.stem) + 1:-len('_meta')]
            data = pd.read_csv(matches[0])
            if len(data) == 0:
                continue
            offsets = data['index'].to_numpy(dtype=np.int64) + data['son_offset'].to_numpy(dtype=np.int64)
            ends = offsets + data['ping_cnt'].to_numpy(dtype=np.int64)
            if offsets.min() < 0 or ends.max() > son.stat().st_size:
                raise ValueError(f'Índices do canal {son.stem} não correspondem ao arquivo SON.')
            self.channels[name] = data
            self.paths[name] = son
        if not self.channels:
            raise ValueError('A gravação não contém canais decodificados utilizáveis.')
        self.primary = next((k for k in self.channels if k.startswith('ss_port')), next(iter(self.channels)))
        self.data = self.channels[self.primary]
        self.sampling_m=float(self.data.pixM.median())
        from pyproj import CRS
        metadata = pd.read_csv(self.project / 'meta' / 'DAT_meta.csv')
        self.crs = CRS.from_user_input(str(metadata.iloc[0]['epsg']))
        self.temperature = float(self.data.tempC.median())
        self.xy = self.data[['e', 'n']].to_numpy(dtype=float)
        valid = np.isfinite(self.xy).all(axis=1)
        self.valid_indices = np.flatnonzero(valid)
        self.tree = cKDTree(self.xy[valid]) if valid.any() else None

    def close(self):
        with self.read_lock:
            self.block_cache.clear();self.cache_bytes=0
            for mm in self.maps.values():mm._mmap.close()
            self.maps.clear()

    def nearest(self, east, north):
        if self.tree is None:
            return None
        distance, index = self.tree.query([east, north])
        return int(self.valid_indices[index]), float(distance)

    def raw_ping(self, channel, index):
        data = self.channels[channel]
        index = min(max(0, int(index)), len(data) - 1)
        row = data.iloc[index]
        if channel not in self.maps:
            self.maps[channel] = np.memmap(self.paths[channel], dtype='uint8', mode='r')
        start = int(row['index']) + int(row['son_offset'])
        count = int(row['ping_cnt'])
        return np.asarray(self.maps[channel][start:start + count]), row

    def read_rows(self,channel,start,stop,width):
        """Cache only viewed ping blocks, bounded to 64 MiB; no image exports."""
        block=256;parts=[]
        for first in range(start//block*block,stop,block):
            key=(channel,first,width)
            if key in self.block_cache:
                a=self.block_cache.pop(key);self.block_cache[key]=a
            else:
                if channel not in self.maps:self.maps[channel]=np.memmap(self.paths[channel],dtype='uint8',mode='r')
                frame=self.channels[channel].iloc[first:min(first+block,len(self.channels[channel]))]
                offsets=frame['index'].to_numpy(dtype=np.int64)+frame.son_offset.to_numpy(dtype=np.int64)
                lengths=frame.ping_cnt.to_numpy(dtype=np.int64)
                a=np.zeros((len(frame),width),dtype=np.uint8)
                for j,(offset,n) in enumerate(zip(offsets,lengths)):
                    n=min(width,int(n));a[j,:n]=self.maps[channel][offset:offset+n]
                self.block_cache[key]=a;self.cache_bytes+=a.nbytes
                while self.cache_bytes>64*1024**2 and len(self.block_cache)>1:
                    _,old=self.block_cache.popitem(last=False);self.cache_bytes-=old.nbytes
            parts.append(a[max(0,start-first):min(len(a),stop-first)])
        return np.concatenate(parts,axis=0)

    def waterfall(self, index, channel='both', remove_water=False, count=280):
        with self.read_lock:return self._waterfall(index,channel,remove_water,count)

    def _waterfall(self, index, channel='both', remove_water=False, count=280):
        port = next((k for k in self.channels if k.startswith('ss_port')), None)
        star = next((k for k in self.channels if k.startswith('ss_star')), None)
        chosen = [port, star] if channel == 'both' and port and star else [channel if channel in self.channels else self.primary]
        start = max(0, min(int(index) - count // 2, len(self.data) - count))
        indices = range(start, min(start + count, len(self.data)))
        scale = self.sampling_m
        end = min(start + count, len(self.data))
        channel_indices={}
        for k in chosen:
            d=self.channels[k];times=d.time_s.to_numpy();wanted=self.data.iloc[start:end].time_s.to_numpy()
            mapped=np.clip(np.searchsorted(times,wanted),0,len(d)-1);previous=np.maximum(mapped-1,0)
            channel_indices[k]=np.where(np.abs(times[previous]-wanted)<np.abs(times[mapped]-wanted),previous,mapped) if k!=self.primary else np.arange(start,end)
        ranges=[float(self.channels[k].iloc[channel_indices[k]]['max_range'].max()) for k in chosen]
        ranges=[r for r in ranges if np.isfinite(r) and r>0]
        if not ranges:raise ValueError('O trecho não contém alcance sonar válido.')
        max_range=max(ranges)
        width = max(1, int(round(max_range / scale)))
        ground = np.arange(width) * scale
        parts = []
        for k in chosen:
            d=self.channels[k]
            mapped=channel_indices[k]
            if len(mapped) and np.array_equal(mapped,np.arange(mapped[0],mapped[0]+len(mapped))):
                array=self.read_rows(k,int(mapped[0]),int(mapped[-1])+1,width)
            elif len(mapped) and int(mapped.max()-mapped.min())<4096:
                first=int(mapped.min());array=self.read_rows(k,first,int(mapped.max())+1,width)[mapped-first]
            else:
                array=np.zeros((len(indices),width),dtype='uint8')
                for y,i in enumerate(mapped):
                    raw,_=self.raw_ping(k,int(i));array[y,:min(width,len(raw))]=raw[:width]
            if remove_water:
                corrected=np.zeros_like(array)
                for y,i in enumerate(mapped):
                    row=d.iloc[int(i)];raw=array[y]
                    depth = float(row.get('dep_m', row.inst_dep_m))
                    positions = np.sqrt(ground * ground + depth * depth) / float(row.pixM)
                    corrected[y] = np.interp(positions, np.arange(len(raw)), raw, left=0, right=0).astype(np.uint8)
                array=corrected
            if k.startswith('ss_port'):
                array = array[:, ::-1]
            parts.append(array)
        result = np.concatenate(parts, axis=1)
        distances = np.linalg.norm(np.diff(self.xy[start:end], axis=0), axis=1)
        finite = distances[np.isfinite(distances)]
        along = float(finite.mean()) if len(finite) and finite.sum() > 0 else scale
        return result, {'start': start, 'end': end, 'width_m': max_range * len(chosen), 'sampling_m': scale, 'along_track_m': along,'channel':channel,'remove_water':remove_water,'part_width':width}

    def export_csv(self, path):
        names = ['record_num', 'date', 'time', 'time_s', 'lat', 'lon', 'e', 'n', 'inst_dep_m', 'dep_m', 'dep_m_raw', 'dep_m_interp', 'speed_ms', 'f', 'pixM']
        self.data[[n for n in names if n in self.data]].to_csv(path, index=False)


def prepare_mapping(recording, temperature, note=lambda s: None):
    project = recording.project
    if not list((project / 'meta').glob('*.meta')):
        project = project.with_name(project.name + '-mapped')
    ready = list((project / 'meta').glob('*.meta')) and list((project / 'meta').glob('Trackline_Smth_*.csv'))
    if ready:
        return project
    if project.exists() and not list((project / 'meta').glob('*.meta')):
        import time
        resolved = project.resolve()
        resolved.relative_to(ROOT.resolve())
        preserved = resolved.with_name(resolved.name + '-incomplete-' + str(time.time_ns()))
        resolved.rename(preserved)
    import pingmapper.funcs_common as common
    models = ROOT / 'work' / 'models-unused'
    models.mkdir(exist_ok=True)
    common.get_segmentation_model_dir = lambda: str(models)
    from pingmapper.doWork import doWork
    from joblib import Parallel
    import pingverter.converter as converter
    def sequential(*args, **kwargs):
        kwargs['n_jobs'] = 1
        return Parallel(*args, **kwargs)
    converter.Parallel = sequential
    import pingmapper.main_rectify as rectify_module
    original_smooth = rectify_module.smoothTrackline

    def smoothed(**kwargs):
        result = original_smooth(**kwargs)
        if result is not None:
            return result
        return {f.stem.replace('Trackline_Smth_', ''): str(f) for f in (pathlib.Path(kwargs['projDir']) / 'meta').glob('Trackline_Smth_*.csv')}

    rectify_module.smoothTrackline = smoothed
    note('Preparando profundidades e trajetória para o mosaico…')
    result = doWork(in_file=str(recording.dat), out_dir=str(project.parent), proj_name=project.name,
                    params=dict(project_mode=2 if project.exists() else 0, threadCnt=1, nchunk=500, tempC=float(temperature),
                                wcp=False, wcr=False, detectDep=0, smthDep=False, remShadow=0, egn=False,
                                pred_sub=False, map_sub=False, coverage=True, rect_wcr=False, mosaic=0))
    if not all(r['success'] for r in result):
        raise RuntimeError(f'Não foi possível preparar o projeto. Consulte {project / "logs"}.')
    return project
