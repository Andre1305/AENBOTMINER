# Usar SonarStudio

Abra DAT extraído junto da pasta SON/IDX. O app encontra a pasta automaticamente e mantém os arquivos originais somente para leitura. Novos projetos recebem pasta de cache própria.

No menu **Novo levantamento — processar tudo**, escolha temperatura da água, resolução do mosaico e célula da batimetria, e marque os produtos desejados. O modo nativo pode gerar arquivos grandes; use 25 cm ou 1 m em um primeiro teste. A exportação Google Earth pode ter resolução diferente do GeoTIFF.

O Sonar Viewer tem play, pausa, parar, velocidades, zoom, ajuste à tela, paletas e filtros não generativos. A paleta copper foi mantida; a versão 4.2 acrescenta Âmbar profundo, Azul oceano, Verde fósforo e Cinza invertido. Paletas alteram a visualização, não a entrada da IA.

A varredura IA pode usar GhostVision, SonarVision, Isolation Forest ou cascata. Ajuste intervalo, score e cobertura. Cancelar preserva os blocos concluídos; retomar exige a mesma configuração, gravação e pesos. Amostragem rápida não cobre todos os pings. Filtros radiométricos da IA são opcionais e experimentais.

Em **Objetos**, selecione para inspecionar no sonar/mapa, filtre, confirme/rejeite, nomeie e exporte. Uma anomalia não é classificação semântica e os scores não são probabilidades calibradas. Corrigir ruído pode apagar ecos; compare sempre com o original.

Validação 4.2: Rec00002, 4 canais, 31.971 pings, 54min36s. Mosaico nativo 8.201 × 27.942 pixels; batimetria TIN de 2 m. A varredura integral produziu 1.187 candidatos, sujeitos a revisão. Os testes verificaram formatos GIS, integridade do RAR, importações parciais, durações desiguais de canais e retomada sem sobrescrever saídas válidas. Os dados do levantamento não estão publicados.

As versões antigas preservam comportamento histórico. A 4.2 inicia sem gravação pré-carregada. Snapshots antigos podem procurar os exemplos locais de validação; se não existirem, escolha seu próprio DAT.

Instalação limpa: o runtime é criado pelo instalador PowerShell, com Pixi previamente instalado. O ZIP não é executável autônomo. Dependências GIS/Python e modelos podem exigir downloads grandes; o cache evita repetições. GhostVision é validado pelo hash do peso usado localmente e recusa um download diferente. SonarVision usa revision fixa.

Correção de roll/pitch, fusão multivisão e treinamento novo com gravações do usuário permanecem pendentes. Não publique gravações ou coordenadas ao abrir um relatório de erro; prefira descrição do erro, versão, modelo do equipamento e trecho de log sem dados privados.
