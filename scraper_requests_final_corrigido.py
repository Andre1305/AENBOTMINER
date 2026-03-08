import re
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}



def direct_scrape_site(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
        logger.debug("%s -> status %s", url, response.status_code)

        if response.status_code == 200 and response.text:
            return response.text

        logger.debug("resposta vazia ou bloqueada para %s", url)
        return None
    except Exception as e:
        logger.warning("Erro ao acessar %s: %s", url, e)
        return None



def extract_price_from_text(text: str) -> Optional[float]:
    if not text:
        return None

    cleaned = re.sub(r"[^\d,\.]", "", text)
    match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:\.\d{2})?)", cleaned)
    if not match:
        return None

    try:
        raw_price = match.group(1)
        if "," in raw_price:
            normalized = raw_price.replace(".", "").replace(",", ".")
        else:
            normalized = raw_price
        return float(normalized)
    except ValueError:
        return None



def extract_product_info(element, product_type: str, base_url: str = "") -> Optional[Dict]:
    name_elem = element.select_one("h3, h2, h1, .product-title, .title")
    price_elem = element.select_one(".price, .product-price, .sale-price, .current-price")
    url_elem = element.select_one("a, .product-link")

    if not name_elem or not price_elem or not url_elem:
        return None

    name = name_elem.get_text(strip=True)
    url = url_elem.get("href", "")
    if not name or not url:
        return None

    if base_url:
        url = urljoin(base_url, url)

    price_text = price_elem.get_text(strip=True)
    price = extract_price_from_text(price_text)
    if price is None:
        return None

    return {
        "name": name,
        "url": url,
        "price": price,
        "product_type": product_type,
    }



def extract_products_from_html(html: str, product_type: str, base_url: str = "") -> List[Dict]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    products: List[Dict] = []

    product_selectors = [
        "div.product-item",          # KaBuM
        "div.product-card",          # Pichau
        "div.product-list-item",     # Terabyte
        "li.ui-search-layout__item", # Mercado Livre
    ]

    for selector in product_selectors:
        elements = soup.select(selector)
        if not elements:
            continue

        for elem in elements:
            product = extract_product_info(elem, product_type, base_url=base_url)
            if product:
                products.append(product)

    unique_products: List[Dict] = []
    seen_urls = set()
    for product in products:
        if product["url"] not in seen_urls:
            unique_products.append(product)
            seen_urls.add(product["url"])

    return unique_products



def scrape_product_page(url: str) -> Optional[Dict]:
    html = direct_scrape_site(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    name_elem = soup.select_one("h1, .product-name, .title")
    price_elem = soup.select_one(".price, .product-price, .sale-price, .current-price")

    if not name_elem or not price_elem:
        return None

    name = name_elem.get_text(strip=True)
    price_text = price_elem.get_text(strip=True)
    price = extract_price_from_text(price_text)

    if not name or price is None:
        return None

    return {
        "name": name,
        "url": url,
        "price": price,
        "product_type": "unknown",
    }


if __name__ == "__main__":
    html = direct_scrape_site("https://www.kabum.com.br/hardware")
    if html:
        products = extract_products_from_html(html, "hardware", base_url="https://www.kabum.com.br")
        print(f"Encontrados {len(products)} produtos")
        for product in products[:5]:
            print(f"- {product['name']}: R$ {product['price']:.2f}")
