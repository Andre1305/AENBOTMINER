"""Conversão de pings acústicos em raster RGB (waterfall)."""

from __future__ import annotations


PALETTE = ((0, 0, 0), (0, 48, 180), (245, 190, 20), (255, 255, 255))


def render_waterfall(pings, gain: float = 1.0, contrast: float = 1.0,
                     palette=PALETTE):
    """Mapeia matriz de amplitudes 0..255 para uma lista RGB pronta para Canvas/Pillow."""
    if gain < 0 or contrast < 0 or len(palette) < 2:
        raise ValueError("gain/contrast inválidos ou paleta insuficiente")
    image = []
    for ping in pings:
        row = []
        for amplitude in ping:
            value = max(0.0, min(1.0, float(amplitude) / 255 * gain))
            value = max(0.0, min(1.0, (value - .5) * contrast + .5))
            scaled = value * (len(palette)-1)
            index = min(int(scaled), len(palette)-2)
            fraction = scaled-index
            a, b = palette[index], palette[index+1]
            row.append(tuple(round(a[channel] + fraction*(b[channel]-a[channel]))
                             for channel in range(3)))
        image.append(row)
    return image
