# AENBOTMINER

Bot de monitoramento de preços 24/7 para detectar possíveis **bugs de preço** e enviar alerta no Telegram imediatamente.

## O que este script faz

- Escaneia KaBuM, Pichau, Terabyte e Mercado Livre por tipo de produto.
- Visita múltiplas páginas de busca por site (`MAX_PAGES_PER_SITE`).
- Detecta bug de preço por **2 critérios**:
  1. **Desconto no próprio site** (`old_price` vs `price` quando disponível).
  2. **Histórico no banco SQLite** (queda forte comparada à média anterior).
- Envia mensagem no Telegram assim que encontra um bug (não espera o ciclo terminar).
- Envia notificação de “bot iniciado” apenas na primeira execução.
- Alguns sites podem bloquear scraping por anti-bot em determinados horários/IPs; nesses casos o bot registra aviso e continua nos demais sites.

## Configuração (.env)

```env
SERP_API_KEY=caaec3c97fc463d1fa94c8bd641c9139ab61ed4693ea98ac188fe43c64213e41
TELEGRAM_BOT_TOKEN=7957463898:AAF7OAujnKjeRxYrY6eY4sH6X_X2zq2-Nzw
TELEGRAM_CHAT_ID=6834775938
SERP_API_KEY=xxx-xxx
TELEGRAM_BOT_TOKEN=xxx-xxx
TELEGRAM_CHAT_ID=xxx-xxx
SCAN_INTERVAL_SECONDS=600
MIN_HISTORY_FOR_ALERT=5
BUG_DROP_ALERT=0.50
ALERT_COOLDOWN_HOURS=12
MAX_PAGES_PER_SITE=30
```

> A API está desativada por padrão no código (`SKIP_SERP_API = True`).

## Rodar no Windows 10 (24/7)

1. Instale Python 3.10+.
2. No CMD/PowerShell dentro da pasta do projeto:

```bash
pip install requests beautifulsoup4 python-decouple
python pricebot_com_serp_api_corrigido.py
```

3. Para deixar 24/7, use o **Agendador de Tarefas**:
   - Criar tarefa → “Executar se o usuário estiver conectado ou não”.
   - Gatilho: “Ao iniciar o computador”.
   - Ação:
     - Programa: `python`
     - Argumentos: `pricebot_com_serp_api_corrigido.py`
     - Iniciar em: pasta do projeto.
   - Marque “Reiniciar se falhar”.

## Arquivos

- `pricebot_com_serp_api_corrigido.py` → loop principal, banco e alertas.
- `scraper_requests_final_corrigido.py` → scraping paginado e extração de preços.
