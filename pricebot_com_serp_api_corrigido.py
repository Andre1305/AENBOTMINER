import re
import time
import sqlite3
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from urllib.parse import urlsplit, urlunsplit
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from decouple import config


# Configurações
SERP_API_KEY = config("SERP_API_KEY", "")
TELEGRAM_BOT_TOKEN = config("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = config("TELEGRAM_CHAT_ID", "")
SCAN_INTERVAL_SECONDS = int(config("SCAN_INTERVAL_SECONDS", "1800"))
MIN_HISTORY_FOR_ALERT = int(config("MIN_HISTORY_FOR_ALERT", "5"))
BUG_DROP_ALERT = float(config("BUG_DROP_ALERT", "0.10"))
ALERT_COOLDOWN_HOURS = int(config("ALERT_COOLDOWN_HOURS", "12"))


def normalize_drop_threshold(value: float) -> float:
    """Aceita 0.10 (10%) ou 10 (10%) e normaliza para fração [0, 1]."""
    if value > 1:
        value = value / 100
    return min(max(value, 0.0), 1.0)


BUG_DROP_THRESHOLD = normalize_drop_threshold(BUG_DROP_ALERT)


def env_bool(name: str, default: bool = False) -> bool:
    value = str(config(name, default=str(default))).strip().lower()
    return value in {"1", "true", "yes", "on"}


SKIP_SERP_API = env_bool("SKIP_SERP_API", True)


# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pricebot.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# Funções auxiliares

def get_md5_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:12]


def sanitize_product_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", name.lower())


def normalize_product_url(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    # remove querystring/fragmento para manter identidade estável entre ciclos
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def extract_price_from_text(text: str) -> Optional[float]:
    if not text:
        return None

    match = re.search(r"R\$\s*([0-9]+(?:\.[0-9]+)*,[0-9]+)", text)
    if not match:
        return None

    return float(match.group(1).replace(".", "").replace(",", "."))


# Banco de dados

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            site TEXT,
            name TEXT,
            url TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            price REAL,
            timestamp TEXT,
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            old_price REAL,
            new_price REAL,
            percentage_drop REAL,
            timestamp TEXT,
            sent_telegram BOOLEAN DEFAULT FALSE,
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_product_timestamp ON prices(product_id, timestamp DESC)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            status TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cursor.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn



def close_db(conn: Optional[sqlite3.Connection]) -> None:
    if conn:
        conn.close()


# Scrapers

def scrape_with_serp(query: str) -> List[Dict]:
    if not SERP_API_KEY or SKIP_SERP_API:
        return []

    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERP_API_KEY,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        products = []
        for result in data.get("organic_results", []):
            title = result.get("title", "")
            link = result.get("link", "")
            snippet = result.get("snippet", "")
            price = extract_price_from_text(snippet)
            if title and link and price:
                products.append({"name": title, "url": link, "price": price})
        return products
    except Exception as e:
        logger.error(f"Erro na SerpAPI: {e}")
        return []



def scrape_with_requests(site: str, product_type: str) -> List[Dict]:
    scrape_pichau_via_store_api = None

    try:
        from scraper_requests_final_corrigido import direct_scrape_site, extract_products_from_html, scrape_pichau_via_store_api
    except ImportError:
        try:
            from scraper_requests_final import direct_scrape_site, extract_products_from_html
        except ImportError:
            logger.warning("Módulo scraper_requests_final não encontrado")
            return []

    root_url = {
        "kabum": "https://www.kabum.com.br/",
        "pichau": "https://www.pichau.com.br/",
        "terabyte": "https://www.terabyte.com.br/",
        "mercadolivre": "https://www.mercadolivre.com.br/",
    }.get(site, "")

    if not root_url:
        return []

    try:
        html_content = direct_scrape_site(root_url)
        if html_content:
            products = extract_products_from_html(html_content, product_type, base_url=root_url)
            if products:
                return products

        logger.warning(f"HTML vazio, bloqueado ou sem produtos em {root_url}")

        if site == "pichau" and scrape_pichau_via_store_api is not None:
            fallback_products = scrape_pichau_via_store_api(product_type)
            if fallback_products:
                logger.info(f"Fallback Store API da Pichau retornou {len(fallback_products)} produtos")
                return fallback_products

        return []
    except Exception as e:
        logger.error(f"Erro no scraping requests ({site}/{product_type}): {e}")
        return []



def scrape_with_selenium(site: str, product_type: str) -> List[Dict]:
    driver = None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        logger.warning("Selenium não está instalado")
        return []

    try:
        try:
            from scraper_requests_final_corrigido import extract_products_from_html
        except ImportError:
            from scraper_requests_final import extract_products_from_html
    except ImportError:
        logger.warning("Módulo scraper_requests_final não encontrado")
        return []

    root_url = {
        "kabum": "https://www.kabum.com.br/hardware",
        "pichau": "https://www.pichau.com.br/hardware",
        "terabyte": "https://www.terabyteshop.com.br/hardware",
        "mercadolivre": "https://lista.mercadolivre.com.br/hardware",
    }.get(site, "")

    if not root_url:
        return []

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        driver.get(root_url)
        html_content = driver.page_source

        if not html_content:
            logger.warning(f"HTML vazio no Selenium: {root_url}")
            return []

        return extract_products_from_html(html_content, product_type, base_url=root_url)
    except Exception as e:
        logger.error(f"Erro no Selenium ({site}/{product_type}): {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# Regras de preço / alerta

def get_product_price(conn: sqlite3.Connection, product_id: str) -> Optional[float]:
    cursor = conn.cursor()
    cursor.execute("SELECT price FROM prices WHERE product_id=? ORDER BY timestamp DESC LIMIT 1", (product_id,))
    row = cursor.fetchone()
    return row[0] if row else None



def get_average_price(conn: sqlite3.Connection, product_id: str, limit: int = MIN_HISTORY_FOR_ALERT) -> Optional[float]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT price FROM prices WHERE product_id=? ORDER BY timestamp DESC LIMIT ?",
        (product_id, limit),
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return sum(row[0] for row in rows) / len(rows)



def add_price_snapshot(conn: sqlite3.Connection, product_id: str, price: float) -> Optional[float]:
    previous_price = get_product_price(conn, product_id)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO prices (product_id, price, timestamp) VALUES (?, ?, ?)",
        (product_id, price, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    return previous_price



def check_for_alert(conn: sqlite3.Connection, product_id: str, current_price: float) -> bool:
    cursor = conn.cursor()

    # Usa histórico ANTERIOR ao preço atual (OFFSET 1),
    # evitando diluir a média com o próprio preço de promoção.
    cursor.execute(
        "SELECT price FROM prices WHERE product_id=? ORDER BY timestamp DESC LIMIT ? OFFSET 1",
        (product_id, MIN_HISTORY_FOR_ALERT),
    )
    history_prices = cursor.fetchall()

    if len(history_prices) < MIN_HISTORY_FOR_ALERT:
        return False

    avg_price = sum(p[0] for p in history_prices) / len(history_prices)
    if avg_price <= 0:
        return False

    drop_ratio = (avg_price - current_price) / avg_price


    if len(history_prices) < MIN_HISTORY_FOR_ALERT:
        return False

    avg_price = sum(p[0] for p in history_prices) / len(history_prices)
    if avg_price <= 0:
        return False

    drop_ratio = (avg_price - current_price) / avg_price

    # BUG_DROP_ALERT aceita 0.10 ou 10 para 10%
    if drop_ratio < BUG_DROP_THRESHOLD:
        return False

    cursor.execute(
        "SELECT timestamp FROM alerts WHERE product_id=? ORDER BY timestamp DESC LIMIT 1",
        (product_id,),
    )
    last_alert = cursor.fetchone()

    if last_alert:
        last_alert_dt = datetime.fromisoformat(last_alert[0])
        elapsed = (datetime.now(timezone.utc) - last_alert_dt).total_seconds()
        if elapsed <= ALERT_COOLDOWN_HOURS * 3600:
            return False

    cursor.execute(
        """
        INSERT INTO alerts (product_id, old_price, new_price, percentage_drop, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            product_id,
            avg_price,
            current_price,
            (avg_price - current_price) / avg_price * 100,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    return True



def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Erro ao enviar mensagem Telegram: {e}")
        return False



def process_product(conn: sqlite3.Connection, product: Dict[str, str]) -> None:
    stable_url = normalize_product_url(product.get("url", ""))
    unique_key = f"{product.get('site', 'unknown')}|{sanitize_product_name(product['name'])}|{stable_url}"
    unique_key = f"{product.get('site', 'unknown')}|{sanitize_product_name(product['name'])}|{product.get('url', '')}"
    product_id = get_md5_hash(unique_key)
    cursor = conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()

    cursor.execute("SELECT 1 FROM products WHERE product_id=?", (product_id,))
    existing = cursor.fetchone()

    if not existing:
        cursor.execute(
            """
            INSERT INTO products (product_id, site, name, url, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (product_id, product["site"], product["name"], stable_url or product.get("url", ""), now_iso, now_iso),
        )
    else:
        cursor.execute(
            "UPDATE products SET updated_at=? WHERE product_id=?",
            (now_iso, product_id),
        )
    conn.commit()

    previous_price = add_price_snapshot(conn, product_id, product["price"])

    # Só avalia alerta quando houver queda de fato contra o preço imediatamente anterior.
    if previous_price is None:
        return
    if product["price"] >= previous_price or abs(product["price"] - previous_price) <= 0.01:
        return

    if not check_for_alert(conn, product_id, product["price"]):
        return

    avg_price = get_average_price(conn, product_id)
    if avg_price is None or avg_price <= 0:
        return

    drop_pct = (1 - product["price"] / avg_price) * 100
    message = (
        "🚨 BUG DE PREÇO DETECTADO!\n\n"
        f"Site: {product['site']}\n"
        f"Produto: {product['name']}\n"
        f"Preço atual: R$ {product['price']:.2f}\n"
        f"Média histórica: R$ {avg_price:.2f}\n"
        f"Queda: {drop_pct:.1f}%\n"
        f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M')}"
    )

    if product.get("url"):
        message += f"\n🔗 <a href='{product['url']}'>🛒 LINK PARA COMPRA</a>"

    send_telegram_message(message)



def process_site(site: str, product_type: str, db_path: str) -> None:
    conn = init_db(db_path)
    try:
        domain = "pichau.com" if site == "pichau" else f"{site}.com.br"
        query = f"{product_type} site:{domain}"

        products = scrape_with_serp(query)
        if not products:
            products = scrape_with_requests(site, product_type)
        if not products:
            products = scrape_with_selenium(site, product_type)

        for product in products:
            product["site"] = site
            process_product(conn, product)
    except Exception as e:
        logger.exception(f"Erro processando {site}/{product_type}: {e}")
    finally:
        close_db(conn)



def cleanup_old_prices(conn: sqlite3.Connection, days: int = 30) -> None:
    cursor = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cursor.execute("DELETE FROM prices WHERE timestamp < ?", (cutoff,))
    conn.commit()



def run_scan_cycle(db_path: str) -> None:
    cycle_id = register_cycle_start(db_path)
    cycle_failed = False

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for site in ["kabum", "pichau", "terabyte", "mercadolivre"]:
                for product_type in ["processador", "placa-mae", "memoria", "ssd", "hd"]:
                    futures.append(executor.submit(process_site, site, product_type, db_path))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    cycle_failed = True
                    logger.exception(f"Erro em uma thread: {e}")
    finally:
        register_cycle_end(db_path, cycle_id, status="failed" if cycle_failed else "completed")

def register_cycle_start(db_path: str) -> int:
    conn = init_db(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO scan_cycles (started_at, status) VALUES (?, ?)",
            (datetime.now(timezone.utc).isoformat(), "running"),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        close_db(conn)


def register_cycle_end(db_path: str, cycle_id: int, status: str = "completed") -> None:
    conn = init_db(db_path)
    try:
        conn.execute(
            "UPDATE scan_cycles SET finished_at=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), status, cycle_id),
        )
        conn.commit()
    finally:
        close_db(conn)


def send_startup_notification_once(db_path: str) -> None:
    conn = init_db(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM bot_state WHERE key='startup_notified_at'"
        ).fetchone()
        if row:
            return

        message = (
            "🤖 Bot de monitoramento iniciado com sucesso!\n"
            f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S UTC')}"
        )

        if send_telegram_message(message):
            conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                ("startup_notified_at", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            logger.info("Mensagem de início enviada no Telegram.")
        else:
            logger.warning("Não foi possível enviar mensagem de início no Telegram.")
    finally:
        close_db(conn)

def register_cycle_end(db_path: str, cycle_id: int, status: str = "completed") -> None:
    conn = init_db(db_path)
    try:
        conn.execute(
            "UPDATE scan_cycles SET finished_at=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), status, cycle_id),
        )
        conn.commit()
    finally:
        close_db(conn)


def register_cycle_end(db_path: str, cycle_id: int, status: str = "completed") -> None:
    conn = init_db(db_path)
    try:
        conn.execute(
            "UPDATE scan_cycles SET finished_at=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), status, cycle_id),
        )
        conn.commit()
    finally:
        close_db(conn)


def send_startup_notification_once(db_path: str) -> None:
    conn = init_db(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM bot_state WHERE key='startup_notified_at'"
        ).fetchone()
        if row:
            return

        message = (
            "🤖 Bot de monitoramento iniciado com sucesso!\n"
            f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S UTC')}"
        )
 
        if send_telegram_message(message):
            conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                ("startup_notified_at", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            logger.info("Mensagem de início enviada no Telegram.")
        else:
            logger.warning("Não foi possível enviar mensagem de início no Telegram.")
    finally:
        close_db(conn)

def send_startup_notification_once(db_path: str) -> None:
    conn = init_db(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM bot_state WHERE key='startup_notified_at'"
        ).fetchone()
        if row:
            return

        message = (
            "🤖 Bot de monitoramento iniciado com sucesso!\n"
            f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S UTC')}"
        )

        if send_telegram_message(message):
            conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                ("startup_notified_at", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            logger.info("Mensagem de início enviada no Telegram.")
        else:
            logger.warning("Não foi possível enviar mensagem de início no Telegram.")
    finally:
        close_db(conn)

def main() -> None:
    db_path = "pricebot.db"

    # Garante criação das tabelas
    conn = init_db(db_path)
    close_db(conn)

    logger.info("🚀 Iniciando monitoramento 24/7")
    send_startup_notification_once(db_path)

    try:
        while True:
            logger.info("=" * 60)
            logger.info(f"🔍 Novo ciclo iniciado em {time.strftime('%d/%m/%Y %H:%M:%S')}")
            logger.info("=" * 60)

            run_scan_cycle(db_path)

            conn = init_db(db_path)
            try:
                cleanup_old_prices(conn)
            finally:
                close_db(conn)

            logger.info(f"✅ Ciclo finalizado. Aguardando {SCAN_INTERVAL_SECONDS} segundos...")
            time.sleep(SCAN_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.warning("⛔ Monitoramento interrompido pelo usuário.")


if __name__ == "__main__":
    main()
