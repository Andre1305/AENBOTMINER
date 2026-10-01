from datetime import date, datetime, timedelta, timezone
import json

import pytest

from bathymetry.export import export_geojson
from bathymetry.models import DepthSample, PositionFix, Sounding
from bathymetry.nmea import NMEAParser
from bathymetry.processing import idw_grid, marching_squares, median_filter_depths
from bathymetry.sonogram import render_waterfall
from bathymetry.sync import synchronize


def test_parses_gga_and_dpt():
    parser = NMEAParser(date(2024, 1, 2))
    fix = parser.parse_line("$GPGGA,120000.00,2254.000,S,04312.000,W,1,08,0.9,5.0,M,0,M,,")
    depth = parser.parse_line("$SDDPT,12.4,0.0")
    assert (fix.latitude, fix.longitude, fix.quality) == (-22.9, -43.2, 1)
    assert depth.depth == 12.4
    assert depth.timestamp == fix.timestamp


def test_rejects_invalid_checksum():
    assert NMEAParser().parse_line("$GPGGA,120000,,,,,,*00") is None


def test_temporal_interpolation():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    fixes = [PositionFix(start, 0, 0), PositionFix(start + timedelta(seconds=10), 10, 20)]
    result = synchronize(fixes, [DepthSample(start + timedelta(seconds=5), 3)])
    assert (result[0].latitude, result[0].longitude) == (5, 10)


def test_processing_pipeline():
    now = datetime.now(timezone.utc)
    samples = [Sounding(now, 0, i, depth) for i, depth in enumerate([10, 100, 10])]
    assert [s.depth for s in median_filter_depths(samples, 3)] == [55, 10, 55]
    square = [Sounding(now, y, x, x+y) for y in (0, 1) for x in (0, 1)]
    xs, ys, grid = idw_grid(square, 3, 3)
    contours = marching_squares(xs, ys, grid, [1.0])
    assert grid[0][0] == 0
    assert contours[1.0]


def test_waterfall_and_geojson(tmp_path):
    assert render_waterfall([[0, 255]]) == [[(0, 0, 0), (255, 255, 255)]]
    sample = Sounding(datetime.now(timezone.utc), -22, -43, 7.5)
    path = export_geojson([sample], tmp_path / "depth.geojson")
    assert json.loads(path.read_text())["features"][0]["properties"]["depth"] == 7.5
