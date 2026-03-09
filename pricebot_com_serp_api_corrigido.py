import hashlib
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import requests
from decouple import config

from scraper_requests_final_corrigido import scrape_site_catalog

# Configurações
SERP_API_KEY = config("SERP_API_KEY", "caaec3c97fc463d1fa94c8bd641c9139ab61ed4693ea98ac188fe43c64213e41")
TELEGRAM_BOT_TOKEN = config("TELEGRAM_BOT_TOKEN", "7957463898:AAF7OAujnKjeRxYrY6eY4sH6X_X2zq2-Nzw")
TELEGRAM_CHAT_ID = config("TELEGRAM_CHAT_ID", "6834775938")
SCAN_INTERVAL_SECONDS = int(config("SCAN_INTERVAL_SECONDS", "600"))
MIN_HISTORY_FOR_ALERT = int(config("MIN_HISTORY_FOR_ALERT", "5"))
BUG_DROP_ALERT = float(config("BUG_DROP_ALERT", "0.50"))
ALERT_COOLDOWN_HOURS = int(config("ALERT_COOLDOWN_HOURS", "12"))
MAX_PAGES_PER_SITE = int(config("MAX_PAGES_PER_SITE", "30"))

# API desativada por padrão (pedido do usuário)
SKIP_SERP_API = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pricebot.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def normalize_drop_threshold(value: float) -> float:
    if value > 1:
        value = value / 100
    return min(max(value, 0.0), 1.0)


BUG_DROP_THRESHOLD = normalize_drop_threshold(BUG_DROP_ALERT)


def get_md5_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:12]


def sanitize_product_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", name.lower())


def normalize_product_url(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
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
    conn.execute(
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            old_price REAL,
            new_price REAL,
            percentage_drop REAL,
            reason TEXT,
            timestamp TEXT,
            sent_telegram BOOLEAN DEFAULT FALSE,
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_product_timestamp ON prices(product_id, timestamp DESC)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()
    return conn


def close_db(conn: Optional[sqlite3.Connection]) -> None:
    if conn:
        conn.close()


def add_price_snapshot(conn: sqlite3.Connection, product_id: str, price: float) -> None:
    conn.execute(
        "INSERT INTO prices (product_id, price, timestamp) VALUES (?, ?, ?)",
        (product_id, price, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def get_price_history(conn: sqlite3.Connection, product_id: str, limit: int) -> List[float]:
    rows = conn.execute(
        "SELECT price FROM prices WHERE product_id=? ORDER BY timestamp DESC LIMIT ?",
        (product_id, limit),
    ).fetchall()
    return [r[0] for r in rows]


def can_send_alert(conn: sqlite3.Connection, product_id: str) -> bool:
    row = conn.execute(
        "SELECT timestamp FROM alerts WHERE product_id=? ORDER BY timestamp DESC LIMIT 1",
        (product_id,),
    ).fetchone()
    if not row:
        return True
    elapsed = datetime.now(timezone.utc) - datetime.fromisoformat(row[0])
    return elapsed.total_seconds() > ALERT_COOLDOWN_HOURS * 3600


def register_alert(conn: sqlite3.Connection, product_id: str, old_price: float, new_price: float, reason: str) -> None:
    drop_pct = ((old_price - new_price) / old_price) * 100 if old_price > 0 else 0
    conn.execute(
        """
        INSERT INTO alerts (product_id, old_price, new_price, percentage_drop, reason, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (product_id, old_price, new_price, drop_pct, reason, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=30,
        )
        return response.status_code == 200
    except Exception as exc:
        logger.error("Erro ao enviar Telegram: %s", exc)
        return False


def process_product(conn: sqlite3.Connection, product: Dict[str, object]) -> None:
    stable_url = normalize_product_url(str(product.get("url", "")))
    unique_key = f"{product.get('site', 'unknown')}|{sanitize_product_name(str(product.get('name', '')))}|{stable_url}"
    product_id = get_md5_hash(unique_key)
    now_iso = datetime.now(timezone.utc).isoformat()

    existing = conn.execute("SELECT 1 FROM products WHERE product_id=?", (product_id,)).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO products (product_id, site, name, url, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (product_id, product["site"], product["name"], stable_url, now_iso, now_iso),
        )
    else:
        conn.execute("UPDATE products SET updated_at=? WHERE product_id=?", (now_iso, product_id))
    conn.commit()

    current_price = float(product["price"])
    add_price_snapshot(conn, product_id, current_price)

    if not can_send_alert(conn, product_id):
        return

    # Regra 1: queda com base no preço de referência no próprio site (de/por)
    old_site_price = product.get("old_price")
    if isinstance(old_site_price, (int, float)) and old_site_price > current_price:
        drop_ratio = (old_site_price - current_price) / old_site_price
        if drop_ratio >= BUG_DROP_THRESHOLD:
            register_alert(conn, product_id, float(old_site_price), current_price, "desconto_no_site")
            send_alert_message(product, float(old_site_price), current_price, "Preço anterior no próprio site")
            return

    # Regra 2: queda com base em histórico local
    history = get_price_history(conn, product_id, MIN_HISTORY_FOR_ALERT + 1)
    historical_only = history[1:]
    if len(historical_only) < MIN_HISTORY_FOR_ALERT:
        return

    avg_price = sum(historical_only) / len(historical_only)
    if avg_price <= 0:
        return

    drop_ratio = (avg_price - current_price) / avg_price
    if drop_ratio >= BUG_DROP_THRESHOLD:
        register_alert(conn, product_id, avg_price, current_price, "historico")
        send_alert_message(product, avg_price, current_price, "Média histórica")


def send_alert_message(product: Dict[str, object], reference_price: float, current_price: float, reason_label: str) -> None:
    drop_pct = ((reference_price - current_price) / reference_price) * 100 if reference_price > 0 else 0
    message = (
        "🚨 BUG DE PREÇO DETECTADO!\n\n"
        f"Site: {product['site']}\n"
        f"Produto: {product['name']}\n"
        f"Preço atual: R$ {current_price:.2f}\n"
        f"Preço de referência: R$ {reference_price:.2f}\n"
        f"Queda: {drop_pct:.1f}%\n"
        f"Critério: {reason_label}\n"
        f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S UTC')}"
    )
    if product.get("url"):
        message += f"\n🔗 <a href='{product['url']}'>Link do produto</a>"
    send_telegram_message(message)


def process_site(site: str, product_type: str, db_path: str) -> None:
    conn = init_db(db_path)
    try:
        products = scrape_site_catalog(site=site, product_type=product_type, max_pages=MAX_PAGES_PER_SITE)
        logger.info("%s/%s -> %s produtos", site, product_type, len(products))
        for product in products:
            product["site"] = site
            process_product(conn, product)
    except Exception as exc:
        logger.exception("Erro processando %s/%s: %s", site, product_type, exc)
    finally:
        close_db(conn)


def register_cycle_start(db_path: str) -> int:
    conn = init_db(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO scan_cycles (started_at, status) VALUES (?, ?)",
            (datetime.now(timezone.utc).isoformat(), "running"),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        close_db(conn)


def register_cycle_end(db_path: str, cycle_id: int, status: str) -> None:
    conn = init_db(db_path)
    try:
        conn.execute(
            "UPDATE scan_cycles SET finished_at=?, status=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), status, cycle_id),
        )
        conn.commit()
    finally:
        close_db(conn)


def cleanup_old_prices(conn: sqlite3.Connection, days: int = 30) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn.execute("DELETE FROM prices WHERE timestamp < ?", (cutoff,))
    conn.commit()


def send_startup_notification_once(db_path: str) -> None:
    conn = init_db(db_path)
    try:
        exists = conn.execute("SELECT value FROM bot_state WHERE key='startup_notified_at'").fetchone()
        if exists:
            return
        if send_telegram_message(
            "🤖 Bot iniciado com sucesso!\n"
            f"Hora: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S UTC')}\n"
            "Modo: monitoramento 24/7"
        ):
            conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                ("startup_notified_at", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
    finally:
        close_db(conn)


def run_scan_cycle(db_path: str) -> None:
    cycle_id = register_cycle_start(db_path)
    cycle_failed = False
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_site, site, product_type, db_path)
                for site in ["kabum", "pichau", "terabyte", "mercadolivre"]
                for product_type in ["processador", "placa-mae", "memoria", "ssd", "hd"]
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    cycle_failed = True
                    logger.exception("Erro em thread: %s", exc)
    finally:
        register_cycle_end(db_path, cycle_id, "failed" if cycle_failed else "completed")


def main() -> None:
    db_path = "pricebot.db"
    close_db(init_db(db_path))
    logger.info("🚀 Iniciando monitoramento 24/7")
    send_startup_notification_once(db_path)

    while True:
        logger.info("=" * 60)
        logger.info("🔍 Novo ciclo iniciado em %s", time.strftime("%d/%m/%Y %H:%M:%S"))
        logger.info("=" * 60)

        run_scan_cycle(db_path)

        conn = init_db(db_path)
        try:
            cleanup_old_prices(conn)
        finally:
            close_db(conn)

        logger.info("✅ Ciclo finalizado. Aguardando %s segundos...", SCAN_INTERVAL_SECONDS)
        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
