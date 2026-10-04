"""Explicit UTC tide interpolation, no implicit timezone or extrapolated datum."""
from pathlib import Path
from datetime import datetime,timezone
import csv,json,hashlib
import numpy as np

def utc_seconds(value):
    if isinstance(value,(int,float)):
        if not np.isfinite(value):raise ValueError('Timestamp inválido.')
        return float(value)
    stamp=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Timestamp deve indicar fuso: Z ou -03:00, por exemplo.')
    return stamp.astimezone(timezone.utc).timestamp()

def import_tides(path,recording_start_timestamp,recording_start_time_s,datum):
    path=Path(path)
    if not str(datum).strip():raise ValueError('Informe datum/origem das alturas da tabela.')
    if path.suffix.lower()=='.csv':
        with path.open(encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
    elif path.suffix.lower()=='.json':
        value=json.loads(path.read_text(encoding='utf-8-sig'));rows=value.get('tides',[]) if isinstance(value,dict) else value
    else:raise ValueError('Use tabela CSV ou JSON.')
    values=sorted([[utc_seconds(row['timestamp']),float(row['height_m'])] for row in rows])
    a=np.asarray(values,float)
    if len(values)<2 or not np.isfinite(a).all() or np.any(np.diff(a[:,0])<=0):raise ValueError('Exige duas alturas finitas ou mais, com timestamps únicos.')
    anchor=utc_seconds(recording_start_timestamp)
    if not np.isfinite(recording_start_time_s):raise ValueError('Tempo inicial da gravação inválido.')
    return {'format':'SonarStudio-tides-v1','values_utc':values,'recording_anchor_utc':anchor,
            'recording_anchor_time_s':float(recording_start_time_s),'datum':str(datum).strip(),
            'source':str(path.resolve()),'source_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'outside_coverage':'error','datum_independently_verified':False}

def tide_at(time_s,table):
    values=np.asarray(table['values_utc'],float);t=np.asarray(time_s,float)
    if values.ndim!=2 or values.shape[1]!=2 or len(values)<2 or not np.isfinite(values).all() or np.any(np.diff(values[:,0])<=0):raise ValueError('Tabela de maré inválida.')
    wanted=table['recording_anchor_utc']+t-table['recording_anchor_time_s']
    if not np.isfinite(wanted).all() or np.any(wanted<values[0,0]) or np.any(wanted>values[-1,0]):raise ValueError('Tabela de maré não cobre todos os pings selecionados. Amplie a tabela ou limite o intervalo.')
    return np.interp(wanted,values[:,0],values[:,1])


def suggested_anchor(recording):
    """Raw DAT Unix clock avoids host-local CSV date/time conversions; user verifies clock."""
    import pandas as pd
    from datetime import datetime,timezone
    try:
        header=pd.read_csv(recording.project/'meta/DAT_meta.csv')
        stamp=float(header.iloc[0]['unix_time'])+float(recording.data.iloc[0].time_s)
        return datetime.fromtimestamp(stamp,timezone.utc).isoformat()
    except (KeyError,ValueError,OSError,IndexError,OverflowError):
        return str(recording.data.iloc[0]['date'])+'T'+str(recording.data.iloc[0]['time'])+'-03:00'
