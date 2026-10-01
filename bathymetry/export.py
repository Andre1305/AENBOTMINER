"""Exportadores portáveis; não exigem GDAL para os formatos de intercâmbio comuns."""

from __future__ import annotations
import json
from html import escape
from pathlib import Path
from .models import Sounding


def export_geojson(soundings: list[Sounding], destination: str | Path) -> Path:
    features = [{"type": "Feature", "geometry": {"type": "Point",
                 "coordinates": [s.longitude, s.latitude]},
                 "properties": {"depth": s.depth, "timestamp": s.timestamp.isoformat()}}
                for s in soundings]
    path = Path(destination)
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features},
                               ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def export_gpx(soundings: list[Sounding], destination: str | Path) -> Path:
    points = "".join(f'<wpt lat="{s.latitude:.8f}" lon="{s.longitude:.8f}">'
                     f'<name>Profundidade {s.depth:.2f} m</name><ele>{-s.depth:.3f}</ele>'
                     f'<time>{escape(s.timestamp.isoformat())}</time></wpt>' for s in soundings)
    xml = '<?xml version="1.0" encoding="UTF-8"?>' \
          '<gpx version="1.1" creator="AEN Bathymetry" xmlns="http://www.topografix.com/GPX/1/1">' \
          f'{points}</gpx>'
    path = Path(destination)
    path.write_text(xml, encoding="utf-8")
    return path


def export_kml(soundings: list[Sounding], destination: str | Path) -> Path:
    placemarks = "".join(f'<Placemark><name>{s.depth:.2f} m</name><Point><coordinates>'
                         f'{s.longitude:.8f},{s.latitude:.8f},{-s.depth:.3f}'
                         '</coordinates></Point></Placemark>' for s in soundings)
    xml = '<?xml version="1.0" encoding="UTF-8"?>' \
          '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>' \
          f'{placemarks}</Document></kml>'
    path = Path(destination)
    path.write_text(xml, encoding="utf-8")
    return path
