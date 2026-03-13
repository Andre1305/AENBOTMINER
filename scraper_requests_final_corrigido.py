import json
import logging
import re
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

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
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
        if response.status_code == 200 and response.text:
            return response.text
        logger.debug("Falha em %s: status=%s", url, response.status_code)
        return None
    except Exception as exc:
        logger.warning("Erro ao acessar %s: %s", url, exc)
        return None


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


def build_search_url(site: str, query: str, page: int) -> Optional[str]:
    template = SEARCH_URLS.get(site)
    if not template:
        return None
    if site == "mercadolivre":
        offset = (page - 1) * 50 + 1
        return template.format(query=query, offset=offset)
    return template.format(query=query, page=page)

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

        empty_streak = 0
        for product in page_products:
            if product["url"] in seen_urls:
                continue
            seen_urls.add(product["url"])
            all_products.append(product)

    if not all_products:
        logger.warning("Nenhum produto coletado para %s/%s (possível bloqueio anti-bot)", site, product_type)

def scrape_site_catalog(site: str, product_type: str, max_pages: int = 20) -> List[Dict]:
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

    return all_products
