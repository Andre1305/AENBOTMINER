"""CLI para validar o pipeline com um arquivo NMEA."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .export import export_geojson, export_gpx, export_kml
from .nmea import parse_nmea
from .processing import idw_grid, marching_squares, median_filter_depths
from .sync import synchronize


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Processa um levantamento NMEA batimétrico")
    parser.add_argument("input", type=Path, help="arquivo com sentenças GGA e DPT/DBT")
    parser.add_argument("--output", type=Path, default=Path("build/bathymetry"),
                        help="prefixo dos arquivos de saída")
    parser.add_argument("--grid-size", type=int, default=100, help="largura/altura da grade IDW")
    parser.add_argument("--median-window", type=int, default=3, help="janela ímpar do filtro")
    parser.add_argument("--contour-interval", type=float, default=1.0,
                        help="intervalo de profundidade das isolinhas")
    return parser


def run(args: argparse.Namespace) -> dict[str, int]:
    positions, depths = parse_nmea(args.input)
    soundings = synchronize(positions, depths)
    if not soundings:
        raise ValueError("não há amostras sincronizáveis; verifique GGA e DPT/DBT")
    soundings = median_filter_depths(soundings, args.median_window)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    export_geojson(soundings, args.output.with_suffix(".geojson"))
    export_gpx(soundings, args.output.with_suffix(".gpx"))
    export_kml(soundings, args.output.with_suffix(".kml"))

    xs, ys, grid = idw_grid(soundings, args.grid_size, args.grid_size)
    minimum, maximum = min(s.depth for s in soundings), max(s.depth for s in soundings)
    interval = args.contour_interval
    if interval <= 0:
        raise ValueError("contour-interval deve ser positivo")
    levels = []
    level = (int(minimum / interval) + 1) * interval
    while level < maximum:
        levels.append(level)
        level += interval
    contours = marching_squares(xs, ys, grid, levels)
    payload = {"x": xs, "y": ys, "depth": grid,
               "contours": {str(key): value for key, value in contours.items()}}
    args.output.with_name(args.output.name + "-grid").with_suffix(".json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8")
    return {"positions": len(positions), "depths": len(depths), "soundings": len(soundings)}


def main() -> int:
    args = build_parser().parse_args()
    try:
        summary = run(args)
    except (OSError, ValueError) as exc:
        print(f"Erro: {exc}")
        return 2
    print("Processamento concluído: " + ", ".join(f"{key}={value}" for key, value in summary.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
