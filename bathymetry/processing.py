"""Limpeza, interpolação IDW e extração de isolinhas."""

from __future__ import annotations
import math
from statistics import median
from .models import Sounding


def median_filter_depths(soundings: list[Sounding], window: int = 5) -> list[Sounding]:
    if window < 1 or window % 2 == 0:
        raise ValueError("window deve ser um inteiro ímpar positivo")
    radius = window // 2
    return [Sounding(s.timestamp, s.latitude, s.longitude,
                     median(x.depth for x in soundings[max(0, i-radius):i+radius+1]))
            for i, s in enumerate(soundings)]


def idw_grid(soundings: list[Sounding], width: int = 100, height: int = 100,
             power: float = 2.0, bounds: tuple[float, float, float, float] | None = None):
    """Gera ``(xs, ys, grid)`` por inverse-distance weighting, sem dependências."""
    if not soundings or width < 2 or height < 2 or power <= 0:
        raise ValueError("amostras, dimensões >= 2 e power positivo são obrigatórios")
    bounds = bounds or (min(s.longitude for s in soundings), min(s.latitude for s in soundings),
                        max(s.longitude for s in soundings), max(s.latitude for s in soundings))
    min_x, min_y, max_x, max_y = bounds
    xs = [min_x + i * (max_x-min_x)/(width-1) for i in range(width)]
    ys = [min_y + i * (max_y-min_y)/(height-1) for i in range(height)]
    grid = []
    for y in ys:
        row = []
        for x in xs:
            weighted = total = 0.0
            exact = None
            for sample in soundings:
                distance = math.hypot(x-sample.longitude, y-sample.latitude)
                if distance == 0:
                    exact = sample.depth
                    break
                weight = distance ** -power
                weighted += weight * sample.depth
                total += weight
            row.append(exact if exact is not None else weighted / total)
        grid.append(row)
    return xs, ys, grid


def marching_squares(xs, ys, grid, levels):
    """Retorna segmentos de isolinha como ``{nível: [((x,y),(x,y)), ...]}``."""
    result = {level: [] for level in levels}
    def crossing(a, b, pa, pb, level):
        if (a < level) == (b < level) or a == b:
            return None
        t = (level-a)/(b-a)
        return (pa[0]+t*(pb[0]-pa[0]), pa[1]+t*(pb[1]-pa[1]))
    for level in levels:
        for row in range(len(ys)-1):
            for col in range(len(xs)-1):
                corners = [(xs[col], ys[row]), (xs[col+1], ys[row]),
                           (xs[col+1], ys[row+1]), (xs[col], ys[row+1])]
                values = [grid[row][col], grid[row][col+1],
                          grid[row+1][col+1], grid[row+1][col]]
                points = [crossing(values[i], values[(i+1)%4], corners[i],
                                   corners[(i+1)%4], level) for i in range(4)]
                points = [point for point in points if point is not None]
                if len(points) == 2:
                    result[level].append((points[0], points[1]))
                elif len(points) == 4:  # sela: preserve dois ramos, sem unir diagonalmente
                    result[level].extend(((points[0], points[1]), (points[2], points[3])))
    return result
