import json
import logging
import random
import re
import threading
import time
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "DNT": "1",
}

FALLBACK_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

_thread_local = threading.local()
_request_lock = threading.Lock()
_next_allowed_request_ts = 0.0
_host_cooldowns: Dict[str, float] = {}

REQUEST_DELAY_BASE = 4.0
REQUEST_DELAY_JITTER = 1.75
BLOCK_COOLDOWN_SECONDS = 15 * 60

SEARCH_URLS = {
    "kabum": "https://www.kabum.com.br/busca/{query}?page_number={page}",
    "pichau": "https://www.pichau.com.br/search?q={query}&page={page}",
    "terabyte": "https://www.terabyteshop.com.br/busca?str={query}&pagina={page}",
    "mercadolivre": "https://lista.mercadolivre.com.br/{query}_Desde_{offset}",
}

SITE_SELECTORS = {
    "kabum": ["div.productCard", "article.productCard", "div.product-card"],
    "pichau": ["article.product", "div.product-item", "div.product-card"],
    "terabyte": ["div.pbox", "div.product-item", "div.product-card"],
    "mercadolivre": ["li.ui-search-layout__item"],
}


def direct_scrape_site(url: str) -> Optional[str]:
    host = urlsplit(url).netloc
    now = time.time()
    cooldown_until = _host_cooldowns.get(host, 0.0)
    if cooldown_until > now:
        logger.info("Host %s em cooldown anti-bloqueio por %.0fs", host, cooldown_until - now)
        return None

    delay = max(0.0, REQUEST_DELAY_BASE + random.uniform(-REQUEST_DELAY_JITTER, REQUEST_DELAY_JITTER))
    _respect_rate_limit(delay)

    session = _get_session()
    try:
        response = session.get(url, timeout=30)
        if response.status_code == 200 and response.text:
            return response.text
        if response.status_code in (403, 429):
            _host_cooldowns[host] = time.time() + BLOCK_COOLDOWN_SECONDS
            logger.warning("Host %s bloqueou (%s). Cooldown aplicado.", host, response.status_code)
        logger.debug("Falha em %s: status=%s", url, response.status_code)
        return None
    except Exception as exc:
        logger.warning("Erro ao acessar %s: %s", url, exc)
        return None


def _get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        _thread_local.session = session

    # Pequena rotação de UA entre requests para evitar fingerprinting estático
    session.headers["User-Agent"] = random.choice(FALLBACK_USER_AGENTS)
    return session


def _respect_rate_limit(delay_seconds: float) -> None:
    global _next_allowed_request_ts
    with _request_lock:
        now = time.time()
        wait_for = _next_allowed_request_ts - now
        if wait_for > 0:
            time.sleep(wait_for)
        _next_allowed_request_ts = max(now, _next_allowed_request_ts) + delay_seconds


def extract_price_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d,\.]", "", text)
    match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:\.\d{2})?)", cleaned)
    if not match:
        return None
    raw = match.group(1)
    try:
        return float(raw.replace(".", "").replace(",", ".")) if "," in raw else float(raw)
    except ValueError:
        return None


def extract_product_info(element, product_type: str, base_url: str = "") -> Optional[Dict]:
    name_elem = element.select_one("h3, h2, h1, .name, .product-name, .product-title, .ui-search-item__title")
    current_price_elem = element.select_one(
        ".price, .product-price, .sale-price, .current-price, .ui-search-price__second-line"
    )
    old_price_elem = element.select_one(".old-price, .price-old, .original-price, s")
    url_elem = element.select_one("a")

    if not name_elem or not current_price_elem or not url_elem:
        return None

    name = name_elem.get_text(" ", strip=True)
    url = url_elem.get("href", "").strip()
    if not name or not url:
        return None
    if base_url:
        url = urljoin(base_url, url)

    current_price = extract_price_from_text(current_price_elem.get_text(" ", strip=True))
    if current_price is None:
        return None

    old_price = None
    if old_price_elem:
        old_price = extract_price_from_text(old_price_elem.get_text(" ", strip=True))

    return {
        "name": name,
        "url": url,
        "price": current_price,
        "old_price": old_price,
        "product_type": product_type,
    }


def extract_products_from_json_ld(html: str, product_type: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    products: List[Dict] = []

    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue

            entries = []
            if node.get("@type") == "ItemList":
                entries = node.get("itemListElement") or []
            elif node.get("@type") == "Product":
                entries = [node]

            for entry in entries:
                item = entry.get("item") if isinstance(entry, dict) else None
                obj = item if isinstance(item, dict) else entry
                if not isinstance(obj, dict):
                    continue

                if obj.get("@type") != "Product":
                    continue

                name = (obj.get("name") or "").strip()

                offers = obj.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}

                url = (obj.get("url") or (offers.get("url") if isinstance(offers, dict) else "") or "").strip()

                price_raw = None
                old_raw = None
                if isinstance(offers, dict):
                    price_raw = offers.get("price")
                    old_raw = offers.get("highPrice") or offers.get("priceSpecification", {}).get("price")

                price = extract_price_from_text(str(price_raw)) if price_raw is not None else None
                if price is None and isinstance(price_raw, (int, float)):
                    price = float(price_raw)

                old_price = extract_price_from_text(str(old_raw)) if old_raw is not None else None
                if old_price is None and isinstance(old_raw, (int, float)):
                    old_price = float(old_raw)

                if not name or not url or price is None or price <= 0:
                    continue

                products.append(
                    {
                        "name": name,
                        "url": url,
                        "price": price,
                        "old_price": old_price,
                        "product_type": product_type,
                    }
                )

    unique = []
    seen = set()
    for product in products:
        if product["url"] in seen:
            continue
        seen.add(product["url"])
        unique.append(product)
    return unique


def extract_products_from_html(html: str, product_type: str, site: str, base_url: str = "") -> List[Dict]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    selectors = SITE_SELECTORS.get(site, [])
    products: List[Dict] = []

    for selector in selectors:
        elements = soup.select(selector)
        for elem in elements:
            product = extract_product_info(elem, product_type, base_url=base_url)
            if product:
                products.append(product)
        if products:
            break

    if not products:
        products = extract_products_from_json_ld(html, product_type)

    unique: List[Dict] = []
    seen_urls = set()
    for product in products:
        if product["url"] in seen_urls:
            continue
        seen_urls.add(product["url"])
        unique.append(product)
    return unique


def parse_woocommerce_minor_units(value: Optional[str], minor_unit: int = 2) -> Optional[float]:
    if value is None:
        return None

    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None

    try:
        amount = int(digits)
    except ValueError:
        return None

    return amount / (10 ** max(minor_unit, 0))


def scrape_pichau_via_store_api(product_type: str, max_pages: int = 20) -> List[Dict]:
    api_url = "https://www.pichau.com/wp-json/wc/store/v1/products"
    search_term = product_type.replace("-", " ")
    products: List[Dict] = []

    for page in range(1, max_pages + 1):
        try:
            response = requests.get(
                api_url,
                headers=DEFAULT_HEADERS,
                params={"search": search_term, "per_page": 30, "page": page},
                timeout=30,
            )
            if response.status_code != 200:
                break
            data = response.json()
        except Exception as exc:
            logger.warning("Falha no fallback da Store API da Pichau: %s", exc)
            break

        if not isinstance(data, list) or not data:
            break

        for item in data:
            name = (item.get("name") or "").strip()
            url = (item.get("permalink") or "").strip()
            prices = item.get("prices") or {}
            minor_unit = int(prices.get("currency_minor_unit", 2) or 2)

            price = parse_woocommerce_minor_units(prices.get("sale_price"), minor_unit)
            if price is None:
                price = parse_woocommerce_minor_units(prices.get("price"), minor_unit)
            if price is None:
                price = parse_woocommerce_minor_units(prices.get("regular_price"), minor_unit)

            old_price = parse_woocommerce_minor_units(prices.get("regular_price"), minor_unit)
            if old_price is not None and price is not None and old_price <= price:
                old_price = None

            if not name or not url or price is None or price <= 0:
                continue

            products.append(
                {
                    "name": name,
                    "url": url,
                    "price": price,
                    "old_price": old_price,
                    "product_type": product_type,
                }
            )

    unique = []
    seen = set()
    for product in products:
        if product["url"] in seen:
            continue
        seen.add(product["url"])
        unique.append(product)
    return unique


def build_search_url(site: str, query: str, page: int) -> Optional[str]:
    template = SEARCH_URLS.get(site)
    if not template:
        return None

    query_encoded = quote_plus(query)
    if site == "mercadolivre":
        offset = (page - 1) * 50 + 1
        return template.format(query=query_encoded, offset=offset)
    return template.format(query=query_encoded, page=page)


def scrape_site_catalog(site: str, product_type: str, max_pages: int = 20) -> List[Dict]:
    if site == "pichau":
        api_products = scrape_pichau_via_store_api(product_type, max_pages=max_pages)
        if api_products:
            return api_products

    query = product_type.replace("-", " ")
    all_products: List[Dict] = []
    seen_urls = set()
    empty_streak = 0

    for page in range(1, max_pages + 1):
        url = build_search_url(site, query, page)
        if not url:
            break

        html = direct_scrape_site(url)
        if not html:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        page_products = extract_products_from_html(html, product_type, site=site, base_url=url)
        if not page_products:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        empty_streak = 0
        for product in page_products:
            if product["url"] in seen_urls:
                continue
            seen_urls.add(product["url"])
            all_products.append(product)

    if not all_products:
        logger.warning("Nenhum produto coletado para %s/%s (possível bloqueio anti-bot)", site, product_type)

    return all_products
