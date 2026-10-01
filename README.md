# AEN Bathymetry

Pipeline Python para transformar registros de GPS/sonar em produtos batimétricos.
O projeto é focado exclusivamente em ingestão, processamento, sonograma e
exportação cartográfica.

## Recursos disponíveis

- parser NMEA 0183 para posição (`GGA`) e profundidade (`DPT`/`DBT`);
- validação de checksum e conversão de latitude/longitude;
- sincronização temporal por interpolação linear;
- limpeza de spikes por filtro de mediana;
- superfície interpolada por IDW;
- isolinhas por Marching Squares;
- waterfall RGB com ganho, contraste e paleta configurável;
- exportação de pontos para GeoJSON, GPX e KML.

> O formato Humminbird `.DAT`/`.SON` varia entre modelos e versões. Um parser
> binário só deve ser incluído depois de definir o equipamento e obter arquivos
> reais para validar offsets, escala, endianness e timestamps.

## Executar os testes

Requer Python 3.10 ou mais recente. Nenhuma dependência é necessária em runtime.

```bash
python -m pip install -e '.[dev]'
python -m pytest -v
```

Também é possível testar o fluxo completo com o registro de exemplo:

```bash
python -m bathymetry examples/sample.nmea --output build/sample --grid-size 25
```

O comando cria `build/sample.geojson`, `build/sample.gpx`, `build/sample.kml` e
`build/sample-grid.json`. O último arquivo contém os eixos, a grade IDW e os
segmentos das isolinhas e pode ser aberto diretamente para inspeção.

## Uso como biblioteca

```python
from bathymetry import idw_grid, median_filter_depths, parse_nmea, synchronize

positions, depths = parse_nmea("levantamento.nmea")
soundings = median_filter_depths(synchronize(positions, depths), window=3)
xs, ys, grid = idw_grid(soundings, width=250, height=250)
```

As coordenadas da grade são longitude/latitude em WGS 84. Para levantamentos de
engenharia, reprojete os pontos para um CRS métrico adequado antes de interpolar;
IDW sobre graus não representa distâncias uniformes em áreas extensas.
