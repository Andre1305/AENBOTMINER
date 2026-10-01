"""Modelos de dados usados em todo o pipeline."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PositionFix:
    timestamp: datetime
    latitude: float
    longitude: float
    altitude: float | None = None
    quality: int = 0


@dataclass(frozen=True)
class DepthSample:
    timestamp: datetime
    depth: float


@dataclass(frozen=True)
class Sounding:
    timestamp: datetime
    latitude: float
    longitude: float
    depth: float
