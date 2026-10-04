"""Bounded, compiled piecewise-affine inverse rasterization."""
import numpy as np
from numba import njit


@njit(cache=True)
def rasterize(image, points, simplices, matrices, height, width, valid_rows):
    output = np.zeros((height, width), dtype=np.uint8)
    ih, iw = image.shape
    for ti in range(len(simplices)):
        a = points[simplices[ti, 0]]
        b = points[simplices[ti, 1]]
        c = points[simplices[ti, 2]]
        determinant = (b[1] - c[1]) * (a[0] - c[0]) + (c[0] - b[0]) * (a[1] - c[1])
        if abs(determinant) < 1e-12:
            continue
        x0 = max(0, int(np.ceil(min(a[0], b[0], c[0]))))
        x1 = min(width - 1, int(np.floor(max(a[0], b[0], c[0]))))
        y0 = max(0, int(np.ceil(min(a[1], b[1], c[1]))))
        y1 = min(height - 1, int(np.floor(max(a[1], b[1], c[1]))))
        m = matrices[ti]
        for y in range(y0, y1 + 1):
            for x in range(x0, x1 + 1):
                l1 = ((b[1] - c[1]) * (x - c[0]) + (c[0] - b[0]) * (y - c[1])) / determinant
                l2 = ((c[1] - a[1]) * (x - c[0]) + (a[0] - c[0]) * (y - c[1])) / determinant
                if l1 < -1e-8 or l2 < -1e-8 or l1 + l2 > 1 + 1e-8:
                    continue
                sx = np.float32(m[0, 0] * x + m[0, 1] * y + m[0, 2])
                sy = np.float32(m[1, 0] * x + m[1, 1] * y + m[1, 2])
                if sx < 0 or sy < 0 or sx > iw - 1 or sy > ih - 1:
                    continue
                ix = int(np.floor(sx))
                iy = int(np.floor(sy))
                jx = min(iw - 1, ix + 1)
                jy = min(ih - 1, iy + 1)
                if not valid_rows[iy] or (not valid_rows[jy] and float(sy)-iy>1e-6):
                    continue
                fx = float(sx) - ix
                fy = float(sy) - iy
                val = (float(image[iy, ix]) * (1-fx) * (1-fy) + float(image[iy, jx]) * fx * (1-fy)
                       + float(image[jy, ix]) * (1-fx) * fy + float(image[jy, jx]) * fx * fy)
                output[y, x] = min(255, max(0, int(val)))
    return output


def triangle_warp(image, inverse_map, output_shape, **kwargs):
    height, width = map(int, output_shape)
    if height * width > 500_000_000:
        raise ValueError('Bloco excede 500 milhões de pixels. Reduza o tamanho dos chunks.')
    valid_rows=np.asarray(kwargs.get('valid_rows',np.ones(image.shape[0],dtype=np.bool_)),dtype=np.bool_)
    if len(valid_rows)!=image.shape[0]:raise ValueError('Máscara de profundidade incompatível com os pings retificados.')
    matrices = np.array([a.params for a in inverse_map.affines])
    return rasterize(np.ascontiguousarray(image), np.ascontiguousarray(inverse_map._tesselation.points),
                     np.ascontiguousarray(inverse_map._tesselation.simplices), matrices, height, width, np.ascontiguousarray(valid_rows))
