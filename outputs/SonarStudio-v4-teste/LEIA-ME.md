# SonarStudio — Humminbird XPLORE 12

**Atualização v4:** modelo pré-treinado multiclasse, filtros/limpeza de objetos, leitura em segundo plano, prévia para a tela, PNG nativo, TIN/IDW, revisão de sondagens, limites de água e relevo 3D. Leia [GUIA-V4.md](GUIA-V4.md), que substitui as orientações antigas sobre reprodução e IA.

**Versão atual:** Viewer com reprodução, pausa, parada e velocidade; leitura por trechos; detecção local de candidatos a covos/anomalias; revisão e mesclagem por IA. Veja [SONAR-VIDEO-IA.md](SONAR-VIDEO-IA.md) para usar essas funções. Paleta copper, controles de imagem, batimetria e exportação GIS continuam disponíveis em **Imagem • Batimetria • GIS**, descritos em [Rec00001-profissional/LEIA-ME.md](../Rec00001-profissional/LEIA-ME.md). As preferências são salvas nos projetos JSON.

Dê dois cliques em **SonarStudio.exe**. A gravação Rec00001 já está configurada. Para outra gravação, use **Abrir DAT** e mantenha o arquivo `Nome.DAT` ao lado da pasta `Nome`, contendo os arquivos `.SON` originais e, quando presentes, `.IDX`. Extraia arquivos RAR antes de abrir.

## Imagem na resolução máxima

O mosaico completo está em `../Rec00001-resolucao-nativa/mosaico-nativo.tif`. É um GeoTIFF BigTIFF georreferenciado em UTM 23S (EPSG:32723), com compressão sem perda e pirâmides para navegação. Os blocos originais processados estão nas subpastas dessa pasta. O arquivo VRT reúne esses blocos e depende deles.

A amostragem desta gravação é **0,01922445992924 m por pixel**, aproximadamente **1,92 cm**. O processamento usa diretamente os arquivos SON, remove a coluna d'água e retifica os canais laterais. Não amplia a imagem anterior de 1 m. Uma grade menor não acrescenta informação capturada pelo sonar. Esse valor descreve o tamanho do pixel; não representa a precisão do GPS nem garante separação de objetos dessa dimensão.

`mosaico-visao-geral.png` é uma prévia reduzida. `mosaico-detalhe-nativo.png` é um recorte em pixels nativos. Para conservar toda a imagem e suas coordenadas, use o GeoTIFF.

## Controles

- Roda do mouse: zoom; arrastar: deslocar o mapa ou o sonar.
- Ajustar: enquadrar a imagem; 1:1: mostrar pixels do raster sem redução.
- Clique no mapa: selecionar o ping mais próximo; o marcador, o sonar e o perfil de profundidade acompanham a seleção.
- Medir: clicar em dois pontos do mapa para obter distância em metros.
- Canais: bombordo, boreste, combinação lateral ou canais verticais disponíveis.
- Paleta, contraste e remoção da coluna d'água: ajustar a visualização do sonar.
- Exportar área: salvar em PNG a área do mapa em resolução nativa. Para áreas acima de 100 milhões de pixels, aumente o zoom ou utilize o GeoTIFF completo.
- Exportar sonar: salvar o trecho exibido; exportar CSV: salvar navegação e profundidades, incluindo valores originais quando disponíveis.
- Salvar projeto: guardar seleção e preferências em JSON; os dados originais continuam nos seus caminhos atuais.
- Gerar mosaico: escolher resolução nativa ou grades menores em tamanho de arquivo. A interface continua responsiva; cancelar preserva blocos concluídos.

## Instalação e limitações

O executável inicia o ambiente Python instalado em `../../work/PINGMapper-main/.pixi/envs/default`. Mantenha esta pasta de trabalho completa: o executável não é um pacote autônomo para copiar isoladamente a outro computador. Fontes e licença estão em `assets`.

O motor é PINGMapper 5.5.5 com PINGVerter 2.1.0. A interface foi criada para o fluxo de leitura, navegação e mosaico solicitado. Ainda não oferece todos os recursos do ReefMaster, como edição de curvas batimétricas, visualização 3D ou exportação proprietária para cartões de navegação.

A temperatura usada no processamento inicial é **10 °C**, valor padrão do decodificador, sem confirmação de medição durante a coleta. Ela influencia a conversão acústica de distância. Informe o valor correto ao gerar um novo mosaico, se disponível. Dados originais e projetos anteriores são preservados.

Os **36 testes da versão atual passaram**: 15 em `test-results-v3.log`, 7 em `professional-tests.log`, 5 em `professional-gui-tests.log` e 9 em `video-ai-tests.log`. O lançador também foi testado iniciando efetivamente o aplicativo com o mosaico completo e encerrando sem erro. Os testes anteriores de importação nova, preparação de trajetória e geração de mosaico estão em `fresh-import-test.log` e `fresh-mosaic-test.log`. A validação de resolução, CRS, leitura dos 312 blocos e igualdade dos pixels do recorte está em `../Rec00001-resolucao-nativa/validacao.json`.

O mosaico tem **94.269 × 146.683 pixels** e ocupa **6.167.374.637 bytes**. Ele contém sete níveis de visão reduzida, de 2 a 128 vezes. Em áreas de sobreposição, a ordem dos blocos determina a passagem visível; diferenças de intensidade e emendas podem permanecer.
