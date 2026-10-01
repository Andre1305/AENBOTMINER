"""Sincronização temporal entre sensores de frequências diferentes."""

from bisect import bisect_right
from .models import DepthSample, PositionFix, Sounding


def synchronize(positions: list[PositionFix], depths: list[DepthSample]) -> list[Sounding]:
    """Interpola linearmente posições para cada amostra, sem extrapolar a rota."""
    fixes = sorted(positions, key=lambda item: item.timestamp)
    if len(fixes) < 2:
        return []
    stamps = [item.timestamp for item in fixes]
    result = []
    for sample in sorted(depths, key=lambda item: item.timestamp):
        right = bisect_right(stamps, sample.timestamp)
        if right == 0 or right == len(fixes):
            continue
        before, after = fixes[right - 1], fixes[right]
        duration = (after.timestamp - before.timestamp).total_seconds()
        ratio = 0.0 if duration == 0 else (sample.timestamp - before.timestamp).total_seconds() / duration
        result.append(Sounding(sample.timestamp,
            before.latitude + (after.latitude - before.latitude) * ratio,
            before.longitude + (after.longitude - before.longitude) * ratio,
            sample.depth))
    return result
