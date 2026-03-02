#!/usr/bin/env python3
import os
import re
import time
import csv
import json
import sqlite3
import hashlib
import requests
import schedule
import html as html_escape_lib
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# Telegram (configure por ENV)
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7957463898:AAF7OAujnKjeRxYrY6eY4sH6X_X2zq2-Nzw")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6834775938")

# =========================
# Config
# =========================
SCAN_INTERVAL_MINUTES = 1
WAIT_TIMEOUT = 20
REQUEST_DELAY = 2
SAVE_CSV_AUDIT = True

SQLITE_DB = "prices.sqlite"
CSV_FILENAME = "log_precos.csv"

BUG_DROP_STRICT = 0.80
BUG_DROP_ALERT  = 0.85
MIN_HISTORY_FOR_COMPARE = 3

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


PICHAU_STRATEGY = os.getenv("PICHAU_STRATEGY", "auto").strip().lower()  # auto | requests | selenium
SITES = {
    "kabum": {
        "type": "selenium",
        "url": "https://www.kabum.com.br/hardware",
        "max_pages": 10,
    },
    "pichau": {
        "type": "pichau_auto",
        "url": "https://www.pichau.com.br/hardware",
        "max_pages": 8,
    },
    "terabyte": {
        "type": "selenium",
        "url": "https://www.terabyteshop.com.br/hardware",
        "max_pages": 5,
        "validate_if_suspicious": True,
    },
    "mercadolivre": {
        "type": "selenium",
        "url": "https://lista.mercadolivre.com.br/hardware",
        "max_pages": 8,
    }
}

# =========================
# Telegram helpers
# =========================
def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def telegram_send_message(text_html: str, disable_preview: bool = False) -> bool:
    """
    Bot API sendMessage: chat_id + text + parse_mode=HTML + link_preview_options(opcional)
    """
    if not telegram_enabled():
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text_html,
        "parse_mode": "HTML",
    }

    # Bot API recente usa link_preview_options
    if disable_preview:
        payload["link_preview_options"] = {"is_disabled": True}

    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.ok
    except Exception:
        return False

def send_bug_alert(site: str, name: str, price_cents: int, avg_cents: int, discount: float, url: str):
    # Escape de HTML pra não quebrar parse_mode
    safe_site = html_escape_lib.escape(site)
    safe_name = html_escape_lib.escape(name)
    safe_url = html_escape_lib.escape(url or "")

    msg = (
        "🐛 <b>BUG DE PREÇO DETECTADO!</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🖥️ <b>Site:</b> {safe_site}\n"
        f"📦 <b>Produto:</b> {safe_name}\n"
        f"💰 <b>Preço:</b> R$ {price_cents/100:.2f}\n"
        f"📊 <b>Média (hist.):</b> R$ {avg_cents/100:.2f}\n"
        f"📉 <b>Diferença:</b> {discount:.1f}% abaixo da média\n"
        f"⏰ <b>Horário:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        "━━━━━━━━━━━━━━━━\n"
    )
    if safe_url:
        msg += f'🔗 <a href="{safe_url}">🛒 LINK PARA COMPRA</a>'

    telegram_send_message(msg, disable_preview=False)

def send_status_report(results: dict, bugs_total: int, elapsed_s: float):
    if not telegram_enabled():
        return

    total = sum(results.values())
    lines = [
        "📊 <b>RELATÓRIO DE VARREDURA</b>",
        "━━━━━━━━━━━━━━━━",
        f"⏰ <b>Horário:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        f"📦 <b>Total de produtos:</b> {total}",
        f"🐛 <b>Bugs/alertas:</b> {bugs_total}",
        f"⏱️ <b>Tempo:</b> {elapsed_s:.1f}s",
        "",
        "<b>Produtos por site:</b>",
    ]
    for s, c in results.items():
        lines.append(f"• {html_escape_lib.escape(s)}: {c}")
    lines.append("━━━━━━━━━━━━━━━━\n🤖 Bot operando 24/7")

    telegram_send_message("\n".join(lines), disable_preview=True)

# =========================
# DB + CSV
# =========================
def db_connect():
    conn = sqlite3.connect(SQLITE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product (
            site TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT,
            url TEXT,
            PRIMARY KEY(site, product_id)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_log (
            ts REAL NOT NULL,
            site TEXT NOT NULL,
            product_id TEXT NOT NULL,
            price_cents INTEGER NOT NULL,
            FOREIGN KEY(site, product_id) REFERENCES product(site, product_id)
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_log ON price_log(site, product_id, ts);")
    conn.commit()
    return conn

def db_get_last_prices(conn, site: str, product_id: str, n=3):
    rows = conn.execute(
        "SELECT price_cents FROM price_log WHERE site=? AND product_id=? ORDER BY ts DESC LIMIT ?",
        (site, product_id, n)
    ).fetchall()
    return [r[0] for r in rows]

def db_insert_price(conn, site: str, product_id: str, name: str, url: str, price_cents: int, ts: float):
    conn.execute(
        "INSERT INTO product(site, product_id, name, url) VALUES(?,?,?,?) "
        "ON CONFLICT(site, product_id) DO UPDATE SET name=excluded.name, url=excluded.url",
        (site, product_id, name, url)
    )
    conn.execute(
        "INSERT INTO price_log(ts, site, product_id, price_cents) VALUES(?,?,?,?)",
        (ts, site, product_id, price_cents)
    )
    conn.commit()

def save_to_csv_audit(row):
    if not SAVE_CSV_AUDIT:
        return
    file_exists = os.path.isfile(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp", "site", "product_id", "product_name", "price_cents", "url"])
        w.writerow(row)

# =========================
# Helpers
# =========================
def normalize_url(url: str) -> str:
    return (url or "").split("#")[0].strip()

def make_product_id(name: str, url: str) -> str:
    base = normalize_url(url) or (name or "").strip().lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def br_to_cents(num_str: str) -> int:
    num_str = num_str.replace(".", "").replace(",", ".")
    return int(round(float(num_str) * 100))

def extract_price_in_cents(text: str):
    """
    Extrator forte (evita pegar 'R$ 4,00' de cupom/desconto):
    1) R$ X à vista / a vista
    2) por R$ X
    3) pega todos R$ X e filtra contexto promocional, retorna o menor restante
    """
    if not text:
        return None

    t = clean_text(text)

    m = re.search(r"R\$\s*([\d\.\,]+)\s*[aà]\s*vista", t, flags=re.I)
    if m:
        return br_to_cents(m.group(1))

    m = re.search(r"\bpor\b\s*[:\-]?\s*R\$\s*([\d\.\,]+)", t, flags=re.I)
    if m:
        return br_to_cents(m.group(1))

    promo_words = ("off", "desconto", "cupom", "cashback", "frete", "%", "economize", "cupons")
    matches = list(re.finditer(r"R\$\s*([\d\.\,]+)", t, flags=re.I))
    if not matches:
        return None

    values = []
    for mm in matches:
        val = br_to_cents(mm.group(1))
        start = max(0, mm.start() - 25)
        end = min(len(t), mm.end() + 25)
        ctx = t[start:end].lower()
        if any(w in ctx for w in promo_words):
            continue
        values.append(val)

    if not values:
        return br_to_cents(matches[0].group(1))
    return min(values)



# =========================
# KaBuM helpers (evitar preço de parcela)
# =========================


def extract_price_by_site(site: str, raw_text: str, product_url: str = ""):
    """Escolhe o melhor extrator de preço por site.

    - KaBuM: regra especial para priorizar PIX e ignorar parcelas
    - Demais: extrator genérico por texto (fallback)
    """
    raw_text = clean_text(raw_text or "")

    if site == "kabum":
        return extract_kabum_price_in_cents(raw_text, product_url)

    if site == "terabyte":
        return extract_price_in_cents(raw_text)

    if site == "mercadolivre":
        return extract_price_in_cents(raw_text)

    if site == "pichau":
        return extract_price_in_cents(raw_text)

    return extract_price_in_cents(raw_text)

def _requests_get(url: str, timeout: int = 25, tries: int = 3):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            return r
        except Exception as e:
            last = e
            time.sleep(1)
    raise last

def _kabum_parcel_cents_from_text(raw_text: str):
    """Extrai o valor da parcela (ex.: '10 x de R$ 69,99') se existir."""
    t = clean_text(raw_text).lower()
    m = re.search(r"\b\d+\s*x\s*de\s*R\$\s*([\d\.,]+)", t, flags=re.I)
    if not m:
        return None
    try:
        return br_to_cents(m.group(1))
    except Exception:
        return None

def _kabum_price_from_listing_text(raw_text: str):
    """Tenta pegar o preço à vista no PIX / preço total (sem entrar no produto)."""
    t = clean_text(raw_text)
    tl = t.lower()

    def last_price_before(idx: int):
        before = t[:idx]
        ms = list(re.finditer(r"R\$\s*([\d\.,]+)", before, flags=re.I))
        if not ms:
            return None
        return br_to_cents(ms[-1].group(1))

    # 1) Se tiver PIX no card, pega o último preço antes de 'PIX'
    pix_pos = tl.find("pix")
    if pix_pos != -1:
        p = last_price_before(pix_pos)
        if p:
            return p

    # 2) Se tiver 'em até', pega o último preço antes de 'em até' (total no cartão)
    emate_pos = tl.find("em até")
    if emate_pos != -1:
        p = last_price_before(emate_pos)
        if p:
            return p

    return None

def kabum_pix_price_from_product_page(product_url: str):
    """Busca o preço à vista no PIX diretamente na página do produto (mais confiável)."""
    try:
        r = _requests_get(product_url, timeout=25, tries=2)
        if getattr(r, "status_code", 0) != 200:
            return None

        html = r.text or ""
        hl = html.lower()

        # A KaBuM normalmente exibe:
        # '#### R$ 629,99' seguido de 'À vista no PIX ...'
        # então pegamos o último R$ antes do trecho 'vista no pix'.
        key_pos = hl.find("vista no pix")
        if key_pos != -1:
            window = html[max(0, key_pos - 1200):key_pos]
            ms = re.findall(r"R\$\s*([\d\.,]+)", window, flags=re.I)
            if ms:
                return br_to_cents(ms[-1])

        # fallback: tenta pegar o primeiro <h4> que contém R$
        m = re.search(r"<h4[^>]*>\s*R\$\s*([\d\.,]+)\s*</h4>", html, flags=re.I)
        if m:
            return br_to_cents(m.group(1))

        # último fallback: usa extrator geral (pode pegar preço total/parcela, mas raramente)
        return extract_price_in_cents(clean_text(html))

    except Exception:
        return None

def extract_kabum_price_in_cents(raw_text: str, product_url: str):
    """
    Estratégia:
    - tenta pegar no texto do card (PIX / total)
    - se não der OU se parecer que pegou parcela -> confirma na página do produto e usa o PIX
    """
    guessed = _kabum_price_from_listing_text(raw_text)
    parcel = _kabum_parcel_cents_from_text(raw_text)

    # Se não conseguiu pelo card, ou se bateu com o valor da parcela -> busca no produto
    if guessed is None or (parcel is not None and guessed == parcel):
        confirmed = kabum_pix_price_from_product_page(product_url)
        if confirmed:
            return confirmed

    return guessed

def detect_bug(current_price: int, last_prices: list[int]):
    if len(last_prices) < MIN_HISTORY_FOR_COMPARE:
        return (False, 0.0, 0)

    avg = sum(last_prices[:3]) / min(3, len(last_prices))
    if current_price < avg * BUG_DROP_STRICT:
        disc = (1 - (current_price / avg)) * 100
        return (True, disc, int(avg))
    if current_price < avg * BUG_DROP_ALERT:
        disc = (1 - (current_price / avg)) * 100
        return (True, disc, int(avg))

    return (False, 0.0, int(avg))

def debug_dump(site: str, driver, tag=""):
    os.makedirs("debug", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        driver.save_screenshot(f"debug/{site}_{tag}_{ts}.png")
    except Exception:
        pass
    try:
        with open(f"debug/{site}_{tag}_{ts}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass

# =========================
# Selenium setup
# =========================
def setup_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1366,900")
    options.add_argument("--lang=pt-BR")
    options.add_argument(f"user-agent={DEFAULT_HEADERS['User-Agent']}")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--proxy-server=http://IP:PORTA")
    options.add_argument("--host-resolver-rules=MAP * 0.0.0.0, EXCLUDE 127.0.0.1")

    return webdriver.Chrome(options=options)

def safe_click(driver, xpath_list):
    for xp in xpath_list:
        try:
            el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xp)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.3)
            el.click()
            return True
        except Exception:
            continue
    return False

# =========================
# Pichau (requests + JSON-LD + page=)
# =========================
def scan_pichau_jsonld_requests(conn, base_url: str, max_pages: int):
    print("\n🔍 Pichau (JSON-LD + page=)")
    processed = 0
    bugs = 0
    seen = set()

    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        r = _requests_get(url, timeout=25, tries=3)
        if r.status_code != 200:
            print(f"  ⚠️ HTTP {r.status_code} na página {page}")
            break

        html = r.text or ""
        scripts = re.findall(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            flags=re.DOTALL | re.I
        )

        products = []
        for s in scripts:
            s = s.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            main = obj.get("mainEntity") or {}
            items = main.get("itemListElement")
            if isinstance(items, list) and items:
                for it in items:
                    try:
                        name = (it.get("name") or "").strip()
                        offers = it.get("offers") or {}
                        price = offers.get("price", None)
                        link = offers.get("url") or it.get("url") or ""
                        if name and price is not None:
                            products.append((name, f"R$ {price}".replace(".", ","), link))
                    except Exception:
                        continue

        if not products:
            print(f"  ⛔ Sem produtos em page={page}. Parando.")
            break

        page_new = 0
        for name, price_txt, link in products:
            price_cents = extract_price_in_cents(price_txt)
            if not price_cents:
                continue

            pid = make_product_id(name, link)
            if pid in seen:
                continue
            seen.add(pid)
            page_new += 1

            last_prices = db_get_last_prices(conn, "pichau", pid, 3)
            is_bug, disc, avg = detect_bug(price_cents, last_prices)

            ts = time.time()
            db_insert_price(conn, "pichau", pid, name, link, price_cents, ts)
            save_to_csv_audit([ts, "pichau", pid, name, price_cents, link])
            processed += 1

            if is_bug and avg > 0:
                bugs += 1
                print(f"🐛 BUG: {name[:60]} - R$ {price_cents/100:.2f} ({disc:.1f}% abaixo)")
                send_bug_alert("pichau", name, price_cents, avg, disc, link)

        print(f"  📄 page={page}: +{page_new} | total={processed}")
        time.sleep(1)

    return processed, bugs



# =========================
# Pichau - alternativas para contornar 403/anti-bot
# =========================

def scan_pichau_selenium(conn, base_url: str, max_pages: int):
    """Scanner Selenium para Pichau.
    Útil quando o requests toma 403/captcha.
    """
    print("\n🔍 Pichau (Selenium)")
    processed = 0
    bugs = 0
    seen = set()
    driver = setup_driver(headless=True)

    try:
        for page in range(1, max_pages + 1):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            driver.get(url)
            time.sleep(REQUEST_DELAY)

            src = (driver.page_source or "").lower()
            if any(k in src for k in ["access denied", "acesso negado", "captcha", "cloudflare", "verifique se você é humano"]):
                debug_dump("pichau", driver, f"blocked_page_{page}")
                print(f"  ⚠️ Possível bloqueio/anti-bot (captcha/deny) na página {page}. Parando.")
                break

            # Tenta localizar cards por links de produto
            links = driver.find_elements(By.XPATH, "//a[contains(@href,'/produto/')]")
            if not links:
                # fallback: qualquer link que pareça produto
                links = driver.find_elements(By.XPATH, "//a[contains(@href,'pichau.com.br') and (contains(@href,'/produto/') or contains(@href,'/produtos/') or contains(@href,'/product/'))]")
            if not links:
                debug_dump("pichau", driver, f"no_cards_{page}")
                print(f"  ⚠️ Nenhum card encontrado na página {page}.")
                break

            page_new = 0
            for a in links:
                try:
                    link = normalize_url(a.get_attribute("href") or "")
                    if not link or "/produto/" not in link:
                        continue

                    # Pega texto do card (subindo um nível) para capturar preço
                    raw = ""
                    try:
                        container = a.find_element(By.XPATH, "ancestor::div[1]")
                        raw = clean_text(container.text or "")
                    except Exception:
                        raw = clean_text(a.text or "")

                    if not raw:
                        continue

                    # Nome: antes do primeiro R$ se existir
                    name = raw.split("R$")[0].strip(" -|•\t")[:160]
                    if not name:
                        continue

                    pid = make_product_id(name, link)
                    if pid in seen:
                        continue
                    seen.add(pid)

                    price_cents = extract_price_by_site("pichau", raw, link)
                    if not price_cents:
                        continue

                    last_prices = db_get_last_prices(conn, "pichau", pid, 3)
                    is_bug, disc, avg = detect_bug(price_cents, last_prices)

                    ts = time.time()
                    db_insert_price(conn, "pichau", pid, name, link, price_cents, ts)
                    save_to_csv_audit([ts, "pichau", pid, name, price_cents, link])
                    processed += 1
                    page_new += 1

                    if is_bug and avg > 0:
                        bugs += 1
                        print(f"🐛 BUG: {name[:60]} - R$ {price_cents/100:.2f} ({disc:.1f}% abaixo)")
                        send_bug_alert("pichau", name, price_cents, avg, disc, link)

                except Exception:
                    continue

            print(f"  📄 página {page}: +{page_new} | total={processed}")
            time.sleep(1)

    finally:
        driver.quit()

    return processed, bugs


def scan_pichau_auto(conn, base_url: str, max_pages: int):
    """Modo 'auto': tenta requests(JSON-LD) e, se falhar (403/0 produtos), cai para Selenium."""
    # 1) tenta requests
    try:
        processed, bugs = scan_pichau_jsonld_requests(conn, base_url, max_pages)
        if processed > 0:
            return processed, bugs
    except Exception:
        processed, bugs = 0, 0

    # 2) fallback selenium
    return scan_pichau_selenium(conn, base_url, max_pages)

# =========================
# Terabyte (Selenium + validação pontual na página do produto)
# =========================
def fetch_terabyte_price_from_product_page(url: str):
    try:
        r = _requests_get(url, timeout=25, tries=3)
        if r.status_code != 200:
            return None
        text = clean_text(r.text)
        m = re.search(r"Por:\s*R\$\s*([\d\.\,]+)\s*[aà]\s*vista", text, flags=re.I)
        if m:
            return br_to_cents(m.group(1))
        return extract_price_in_cents(text)
    except Exception:
        return None

def scan_terabyte_selenium(conn, url: str, max_pages: int, validate_if_suspicious=True):
    print("\n🔍 Terabyte (Selenium)")
    processed = 0
    bugs = 0
    seen = set()
    driver = setup_driver(headless=True)

    try:
        driver.get(url)
        time.sleep(REQUEST_DELAY)

        for page in range(1, max_pages + 1):
            cards = driver.find_elements(By.XPATH, "//a[contains(@href,'/produto/')]")
            if not cards:
                debug_dump("terabyte", driver, "no_cards")
                print("  ⚠️ Nenhum card encontrado.")
                break

            page_new = 0
            for a in cards:
                try:
                    link = normalize_url(a.get_attribute("href") or "")
                    raw = clean_text(a.text or "")
                    if not link or not raw:
                        continue

                    name = raw.split("R$")[0].strip(" -|•\t")[:160]
                    if not name:
                        continue

                    pid = make_product_id(name, link)
                    if pid in seen:
                        continue
                    seen.add(pid)

                    price_cents = extract_price_by_site("kabum", raw, link)
                    if not price_cents:
                        continue

                    # validação quando vier preço muito baixo (ex.: cupom R$ 4,00)
                    if validate_if_suspicious and price_cents < 20000:  # < R$ 200 (muito baixo p/ hardware)
                        confirmed = fetch_terabyte_price_from_product_page(link)
                        if confirmed and confirmed != price_cents:
                            price_cents = confirmed

                    last_prices = db_get_last_prices(conn, "terabyte", pid, 3)
                    is_bug, disc, avg = detect_bug(price_cents, last_prices)

                    ts = time.time()
                    db_insert_price(conn, "terabyte", pid, name, link, price_cents, ts)
                    save_to_csv_audit([ts, "terabyte", pid, name, price_cents, link])
                    processed += 1
                    page_new += 1

                    if is_bug and avg > 0:
                        bugs += 1
                        print(f"🐛 BUG: {name[:60]} - R$ {price_cents/100:.2f} ({disc:.1f}% abaixo)")
                        send_bug_alert("terabyte", name, price_cents, avg, disc, link)

                except Exception:
                    continue

            print(f"  📄 página {page}: +{page_new} | total={processed}")

            moved = safe_click(driver, [
                "//a[contains(@class,'next')]",
                "//a[contains(.,'Próxima')]",
                "//a[@rel='next']",
                "//button[contains(.,'Próxima')]",
            ])
            if not moved:
                break
            time.sleep(REQUEST_DELAY)

    finally:
        driver.quit()

    return processed, bugs

# =========================
# KaBuM (Selenium + paginação robusta)
# =========================
def scan_kabum_selenium(conn, url: str, max_pages: int):
    print("\n🔍 KaBuM (Selenium)")
    processed = 0
    bugs = 0
    seen = set()
    driver = setup_driver(headless=True)

    try:
        driver.get(url)
        time.sleep(REQUEST_DELAY)

        for page in range(1, max_pages + 1):
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href,'/produto/')]"))
            )

            cards = driver.find_elements(By.XPATH, "//a[contains(@href,'/produto/')]")
            page_new = 0

            for a in cards:
                try:
                    link = normalize_url(a.get_attribute("href") or "")
                    raw = clean_text(a.text or "")
                    if not link or not raw:
                        continue

                    try:
                        name = a.find_element(By.XPATH, ".//h3|.//h2").text.strip()
                    except Exception:
                        name = raw.split("R$")[0].strip(" -|•\t")[:160]
                    if not name:
                        continue

                    pid = make_product_id(name, link)
                    if pid in seen:
                        continue
                    seen.add(pid)

                    price_cents = extract_price_by_site("kabum", raw, link)
                    if not price_cents:
                        continue

                    last_prices = db_get_last_prices(conn, "kabum", pid, 3)
                    is_bug, disc, avg = detect_bug(price_cents, last_prices)

                    ts = time.time()
                    db_insert_price(conn, "kabum", pid, name, link, price_cents, ts)
                    save_to_csv_audit([ts, "kabum", pid, name, price_cents, link])
                    processed += 1
                    page_new += 1

                    if is_bug and avg > 0:
                        bugs += 1
                        print(f"🐛 BUG: {name[:60]} - R$ {price_cents/100:.2f} ({disc:.1f}% abaixo)")
                        send_bug_alert("kabum", name, price_cents, avg, disc, link)

                except Exception:
                    continue

            print(f"  📄 página {page}: +{page_new} | total={processed}")

            moved = safe_click(driver, [
                "//a[contains(@aria-label,'Próxima')]",
                "//a[normalize-space(.)='>']",
                "//a[normalize-space(.)='>>']",
                "//li[a[normalize-space(.)='>']]/a",
            ])
            if not moved:
                break

            time.sleep(REQUEST_DELAY)

    except Exception:
        debug_dump("kabum", driver, "error")
    finally:
        driver.quit()

    return processed, bugs

# =========================
# Mercado Livre (Selenium)
# =========================
def scan_mercadolivre_selenium(conn, url: str, max_pages: int):
    print("\n🔍 Mercado Livre (Selenium)")
    processed = 0
    bugs = 0
    seen = set()
    driver = setup_driver(headless=True)

    try:
        driver.get(url)
        time.sleep(REQUEST_DELAY)

        for page in range(1, max_pages + 1):
            cards = WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_all_elements_located((By.XPATH, "//li[contains(@class,'ui-search-layout__item')]"))
            )

            page_new = 0
            for li in cards:
                try:
                    a = li.find_element(By.XPATH, ".//a[contains(@class,'ui-search-link') or @href]")
                    link = normalize_url(a.get_attribute("href") or "")
                    if not link:
                        continue

                    name = li.find_element(By.XPATH, ".//h2").text.strip()
                    raw = clean_text(li.text)

                    # Mercado Livre: tenta extrair preço pelo DOM (fraction + cents). Se falhar, cai no texto.
                    price_cents = None
                    try:
                        frac = li.find_element(By.XPATH, ".//span[contains(@class,'andes-money-amount__fraction')]").text
                        cents = "00"
                        try:
                            cents = li.find_element(By.XPATH, ".//span[contains(@class,'andes-money-amount__cents')]").text
                        except Exception:
                            pass

                        frac_num = re.sub(r"[^\\d]", "", frac)
                        cents_num = re.sub(r"[^\\d]", "", cents)[:2].ljust(2, "0")
                        if frac_num:
                            price_cents = int(frac_num) * 100 + int(cents_num)
                    except Exception:
                        price_cents = None

                    if not price_cents:
                        price_cents = extract_price_by_site("mercadolivre", raw, link)
                    if not price_cents:
                        continue

                    pid = make_product_id(name, link)
                    if pid in seen:
                        continue
                    seen.add(pid)

                    last_prices = db_get_last_prices(conn, "mercadolivre", pid, 3)
                    is_bug, disc, avg = detect_bug(price_cents, last_prices)

                    ts = time.time()
                    db_insert_price(conn, "mercadolivre", pid, name, link, price_cents, ts)
                    save_to_csv_audit([ts, "mercadolivre", pid, name, price_cents, link])
                    processed += 1
                    page_new += 1

                    if is_bug and avg > 0:
                        bugs += 1
                        print(f"🐛 BUG: {name[:60]} - R$ {price_cents/100:.2f} ({disc:.1f}% abaixo)")
                        send_bug_alert("mercadolivre", name, price_cents, avg, disc, link)

                except Exception:
                    continue

            print(f"  📄 página {page}: +{page_new} | total={processed}")

            moved = safe_click(driver, [
                "//a[contains(@title,'Seguinte')]",
                "//a[contains(.,'Seguinte')]",
                "//a[@rel='next']",
            ])
            if not moved:
                break
            time.sleep(REQUEST_DELAY)

    except Exception:
        debug_dump("mercadolivre", driver, "error_or_block")
        print("  ⚠️ Mercado Livre bloqueou/variou DOM. Veja debug/*.html e debug/*.png")
    finally:
        driver.quit()

    return processed, bugs

# =========================
# Runner
# =========================
def run_bot_once():
    if not telegram_enabled():
        print("⚠️ Telegram não configurado. Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID.")

    conn = db_connect()
    results = {}
    bugs_total = 0
    start = time.time()

    try:
        for site, cfg in SITES.items():
            # Permite controlar a Pichau por variável de ambiente: PICHAU_STRATEGY=auto|requests|selenium
            if site == "pichau":
                strategy = (PICHAU_STRATEGY or cfg.get("type", "pichau_auto")).lower().replace("pichau_", "")
                if strategy == "requests":
                    c, b = scan_pichau_jsonld_requests(conn, cfg["url"], cfg["max_pages"])
                elif strategy == "selenium":
                    c, b = scan_pichau_selenium(conn, cfg["url"], cfg["max_pages"])
                else:
                    c, b = scan_pichau_auto(conn, cfg["url"], cfg["max_pages"])

            elif site == "kabum":
                c, b = scan_kabum_selenium(conn, cfg["url"], cfg["max_pages"])
            elif site == "terabyte":
                c, b = scan_terabyte_selenium(conn, cfg["url"], cfg["max_pages"], cfg.get("validate_if_suspicious", True))
            elif site == "mercadolivre":
                c, b = scan_mercadolivre_selenium(conn, cfg["url"], cfg["max_pages"])
            else:
                c, b = 0, 0

            results[site] = c
            bugs_total += b
            time.sleep(REQUEST_DELAY)

        elapsed = time.time() - start
        print("\n📊 Resumo:")
        for s, c in results.items():
            print(f"  • {s}: {c} produtos")
        print(f"  🐛 Bugs/alertas: {bugs_total}")
        print(f"⏱️ Tempo: {elapsed:.1f}s")

        # Telegram: enviar apenas quando houver BUG (alertas individuais)
        # (Relatório desativado a pedido do usuário)

    finally:
        conn.close()

if __name__ == "__main__":
    print("🤖 Bug Sentinel (KaBuM + Pichau + Terabyte + MercadoLivre)")
    run_bot_once()
    schedule.every(SCAN_INTERVAL_MINUTES).minutes.do(run_bot_once)
    while True:
        schedule.run_pending()
        time.sleep(15)