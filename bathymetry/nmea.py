"""Leitor incremental de posições GGA e profundidades DPT/DBT em NMEA 0183."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .models import DepthSample, PositionFix


def _checksum(sentence: str) -> bool:
    if "*" not in sentence:
        return True
    payload, expected = sentence.lstrip("$").split("*", 1)
    value = 0
    for char in payload:
        value ^= ord(char)
    try:
        return value == int(expected[:2], 16)
    except ValueError:
        return False


def _coordinate(raw: str, hemisphere: str) -> float:
    if not raw:
        raise ValueError("coordenada NMEA vazia")
    split = 2 if hemisphere in {"N", "S"} else 3
    result = int(raw[:split]) + float(raw[split:]) / 60.0
    return -result if hemisphere in {"S", "W"} else result


class NMEAParser:
    """Mantém a data e a última posição para associar mensagens de profundidade."""

    def __init__(self, recording_date: date | None = None):
        self.recording_date = recording_date or datetime.now(timezone.utc).date()
        self.last_timestamp: datetime | None = None

    def _timestamp(self, raw: str | None) -> datetime:
        if raw:
            parsed = time.fromisoformat(f"{raw[0:2]}:{raw[2:4]}:{raw[4:]}")
            candidate = datetime.combine(self.recording_date, parsed, timezone.utc)
            if self.last_timestamp and candidate < self.last_timestamp - timedelta(hours=12):
                self.recording_date += timedelta(days=1)
                candidate += timedelta(days=1)
            self.last_timestamp = candidate
        if self.last_timestamp is None:
            raise ValueError("profundidade recebida antes de um timestamp GGA")
        return self.last_timestamp

    def parse_line(self, line: str) -> PositionFix | DepthSample | None:
        sentence = line.strip()
        if not sentence.startswith("$") or not _checksum(sentence):
            return None
        fields = sentence.split("*", 1)[0].split(",")
        kind = fields[0][-3:]
        try:
            if kind == "GGA" and len(fields) >= 10:
                timestamp = self._timestamp(fields[1])
                return PositionFix(timestamp, _coordinate(fields[2], fields[3]),
                                   _coordinate(fields[4], fields[5]),
                                   float(fields[9]) if fields[9] else None,
                                   int(fields[6] or 0))
            if kind == "DPT" and len(fields) >= 2:
                return DepthSample(self._timestamp(None), float(fields[1]))
            if kind == "DBT" and len(fields) >= 4:
                return DepthSample(self._timestamp(None), float(fields[3]))
        except (ValueError, IndexError):
            return None
        return None


def parse_nmea(source: str | Path | Iterable[str], recording_date: date | None = None):
    """Retorna duas listas ``(posições, profundidades)`` de arquivo ou iterável."""
    parser = NMEAParser(recording_date)
    lines = Path(source).read_text(encoding="utf-8", errors="replace").splitlines() \
        if isinstance(source, (str, Path)) else source
    positions, depths = [], []
    for line in lines:
        value = parser.parse_line(line)
        if isinstance(value, PositionFix):
            positions.append(value)
        elif isinstance(value, DepthSample):
            depths.append(value)
    return positions, depths
