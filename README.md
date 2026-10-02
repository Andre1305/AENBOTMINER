# SonarStudio e AEN Bathymetry

SonarStudio **4.2-beta.2**: aplicativo Windows para arquivos Humminbird DAT/SON, Sonar Viewer, mosaicos SideScan, batimetria TIN e triagem local por modelos específicos de sonar.

**[Baixar versões de teste](https://github.com/Andre1305/AENBOTMINER/releases)** · [Guia do SonarStudio](docs/SONARSTUDIO.md)

## Instalação Windows

1. Baixe o ZIP da versão nas Releases e extraia mantendo a estrutura de pastas.
2. Instale [Pixi](https://pixi.sh) pelo site oficial ou `winget install prefix-dev.pixi`.
3. Execute `./Instalar-SonarStudio.ps1` no PowerShell. Precisa de internet para instalar as dependências. Para uma versão antiga: `./Instalar-SonarStudio.ps1 -Version 4.1-test.1`.
4. Abra `outputs/SonarStudio-v42-beta/SonarStudio.exe` (ou a pasta correspondente à versão escolhida).

O ZIP contém modelos e launcher, mas **não contém Python/GDAL instalados**. O script cria o runtime esperado em `work/PINGMapper-main/.pixi/envs/default`. O instalador foi acrescentado para distribuição e requer validação de instalação limpa; o runtime local anterior já foi usado nos testes de integração.

Para novas gravações, extraia o RAR com 7-Zip/WinRAR e mantenha o DAT ao lado da pasta de mesmo nome com SON/IDX. Use **Abrir .DAT** para visualizar ou **Novo levantamento — processar tudo…** para gerar produtos. Nenhuma gravação privada, coordenada de levantamento ou mapa gerado é distribuído neste repositório.

## Versões e validação

| Versão | Recursos | Testes locais |
|---|---|---|
| 4.0-test.1 | Viewer, modelos ONNX, batimetria TIN e revisão | 42 |
| 4.1-test.1 | Varredura em lote, retomada, radiometria experimental | 49 |
| 4.2-beta.2 | Fluxo completo, outras gravações, retomada corrigida e novas paletas | 58 |

Os snapshots ficam em `outputs/SonarStudio-v4-teste`, `outputs/SonarStudio-v41-teste` e `outputs/SonarStudio-v42-beta`. Releases são reconstruídas desses snapshots e possuem SHA-256. O CI testa o pipeline NMEA público; testes de integração do GUI exigem gravações locais, que não são públicas.

A IA produz **candidatos para revisão**. Não foi aferida precisão da classificação em um conjunto de alvos anotados. Resolução de amostragem não é precisão de GPS; a batimetria não tem datum vertical aferido. Não há equivalência profissional certificada ao ReefMaster/SonarWiz.

## Pipeline NMEA original

O pacote `bathymetry/`, CLI e testes originais foram preservados. Requer Python 3.10+:

```bash
python -m pip install -e '.[dev]'
python -m pytest -v
python -m bathymetry examples/sample.nmea --output build/sample --grid-size 25
```

Parser NMEA 0183, sincronização temporal, filtro de mediana, IDW, Marching Squares, sonograma e exportação GeoJSON/GPX/KML. IDW em graus não constitui interpolação métrica para levantamento de engenharia.

## Créditos e licenças

- [PINGMapper](https://github.com/CameronBodine/PINGMapper), versão fonte fixada no runtime, MIT.
- [GhostVision](https://huggingface.co/PINGEcosystem/gv-yolo12), CC-BY-SA-4.0; proveniência e hash em cada snapshot.
- [SonarVision](https://huggingface.co/Dinoman1221/sonarvision-yolov8-esi-v6), Apache-2.0 declarado pelo autor; revision e hashes preservados.
- Noto Sans: SIL Open Font License, arquivo OFL incluído.
