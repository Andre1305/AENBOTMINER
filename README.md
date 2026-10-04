# SonarStudio e AEN Bathymetry

SonarStudio **4.8-HumViewer-test.1**: aplicativo Windows para arquivos Humminbird DAT/SON, Sonar Viewer, mosaicos SideScan, batimetria TIN e triagem local por modelos especÃ­ficos de sonar.

**[Baixar versÃµes de teste](https://github.com/Andre1305/AENBOTMINER/releases)** Â· [Guia do SonarStudio](docs/SONARSTUDIO.md)

## InstalaÃ§Ã£o Windows

1. Baixe o ZIP da versÃ£o nas Releases e extraia mantendo a estrutura de pastas.
2. Instale [Pixi](https://pixi.sh) pelo site oficial ou `winget install prefix-dev.pixi`.
3. Execute `./Instalar-SonarStudio.ps1` no PowerShell. Precisa de internet para instalar as dependÃªncias. Para uma versÃ£o antiga: `./Instalar-SonarStudio.ps1 -Version 4.1-test.1`.
4. Abra `outputs/SonarStudio-v48-HumViewer-test1/SonarStudio.exe` (ou a pasta correspondente Ã  versÃ£o escolhida).

O ZIP contÃ©m modelos e launcher, mas **nÃ£o contÃ©m Python/GDAL instalados**. O script cria o runtime esperado em `work/PINGMapper-main/.pixi/envs/default`. O instalador foi acrescentado para distribuiÃ§Ã£o e requer validaÃ§Ã£o de instalaÃ§Ã£o limpa; o runtime local anterior jÃ¡ foi usado nos testes de integraÃ§Ã£o.

Para novas gravaÃ§Ãµes, extraia o RAR com 7-Zip/WinRAR e mantenha o DAT ao lado da pasta de mesmo nome com SON/IDX. Use **Abrir .DAT** para visualizar ou **Novo levantamento â€” processar tudoâ€¦** para gerar produtos. Nenhuma gravaÃ§Ã£o privada, coordenada de levantamento ou mapa gerado Ã© distribuÃ­do neste repositÃ³rio.

## VersÃµes e validaÃ§Ã£o

| VersÃ£o | Recursos | Testes locais |
|---|---|---|
| 4.0-test.1 | Viewer, modelos ONNX, batimetria TIN e revisÃ£o | 42 |
| 4.1-test.1 | Varredura em lote, retomada, radiometria experimental | 49 |
| 4.2-beta.2 | Fluxo completo, outras gravaÃ§Ãµes, retomada corrigida e novas paletas | 58 |

Os snapshots ficam em `outputs/SonarStudio-v4-teste`, `outputs/SonarStudio-v41-teste` e `outputs/SonarStudio-v42-beta`. Releases sÃ£o reconstruÃ­das desses snapshots e possuem SHA-256. O CI testa o pipeline NMEA pÃºblico; testes de integraÃ§Ã£o do GUI exigem gravaÃ§Ãµes locais, que nÃ£o sÃ£o pÃºblicas.

A IA produz **candidatos para revisÃ£o**. NÃ£o foi aferida precisÃ£o da classificaÃ§Ã£o em um conjunto de alvos anotados. ResoluÃ§Ã£o de amostragem nÃ£o Ã© precisÃ£o de GPS; a batimetria nÃ£o tem datum vertical aferido. NÃ£o hÃ¡ equivalÃªncia profissional certificada ao ReefMaster/SonarWiz.

## AtualizaÃ§Ãµes 4.3â€“4.7

- **4.3-test.1**: Filtro de profundidade do mosaico; somente modelos especializados em sonar.
- **4.4-test.1**: CorreÃ§Ã£o de picos isolados de alcance; cascata reversÃ­vel sem trocar os lados.
- **4.5-test.1**: Triagem automÃ¡tica, feedback de candidatos e mapa de scores.
- **4.5-MAX**: Alinhamento relativo por sobreposiÃ§Ã£o; tabela de marÃ©; dataset YOLO e fine-tuning local.
- **4.6-Viewer-test.1**: Leitura assÃ­ncrona, sincronizaÃ§Ã£o de canais, rÃ©gua e sombra com hipÃ³teses, cache de IA.
- **4.6-Viewer-test.2**: Zoom vertical independente, janela atÃ© 3000 pings, rolagem virtual e Overview.
- **4.8-HumViewer-test.1**: Legado de alta fidelidade, lupa nativa DN/dB digitais relativos, fichas privadas de alvo e correÃ§Ãµes do cache.

[Notas de cada versÃ£o](docs/releases). A versÃ£o 4.7 restaura a ordem e quantizaÃ§Ã£o do processamento legado, preserva a navegaÃ§Ã£o vertical e acrescenta a lupa nativa e fichas pessoais protegidas. As fichas exigem a chave DPAPI local e runtime de PDF do proprietÃ¡rio, ausentes dos pacotes pÃºblicos.

## Pipeline NMEA original

O pacote `bathymetry/`, CLI e testes originais foram preservados. Requer Python 3.10+:

```bash
python -m pip install -e '.[dev]'
python -m pytest -v
python -m bathymetry examples/sample.nmea --output build/sample --grid-size 25
```

Parser NMEA 0183, sincronizaÃ§Ã£o temporal, filtro de mediana, IDW, Marching Squares, sonograma e exportaÃ§Ã£o GeoJSON/GPX/KML. IDW em graus nÃ£o constitui interpolaÃ§Ã£o mÃ©trica para levantamento de engenharia.

## CrÃ©ditos e licenÃ§as

- [PINGMapper](https://github.com/CameronBodine/PINGMapper), versÃ£o fonte fixada no runtime, MIT.
- [GhostVision](https://huggingface.co/PINGEcosystem/gv-yolo12), CC-BY-SA-4.0; proveniÃªncia e hash em cada snapshot.
- [SonarVision](https://huggingface.co/Dinoman1221/sonarvision-yolov8-esi-v6), Apache-2.0 declarado pelo autor; revision e hashes preservados.
- Noto Sans: SIL Open Font License, arquivo OFL incluÃ­do.
