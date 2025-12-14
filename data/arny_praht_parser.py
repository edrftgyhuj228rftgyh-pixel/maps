# Если чего-то не хватает, сначала в терминале внутри venv:
# pip install requests beautifulsoup4 pandas

import re
import time
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ---------------- НАСТРОЙКИ ---------------- #

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15"
    )
}

CATEGORY_URLS = {
    "backpacks": "https://arnypraht.com/category/backpacks/",
    "bags": "https://arnypraht.com/category/bags/",
    "accessories": "https://arnypraht.com/category/aksessuary/",
}

clean_price = lambda s: int(re.sub(r"\D+", "", s)) if s else None  # lambda по требованию


# ---------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------- #


def download_page(url: str) -> Optional[BeautifulSoup]:
    """Скачиваем страницу и возвращаем BeautifulSoup, либо None при ошибке."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ОШИБКА] Не удалось получить {url}: {e}")
        return None

    return BeautifulSoup(resp.text, "html.parser")


def find_product_elements(soup: BeautifulSoup) -> List:
    """
    Находит элементы-карточки товаров.
    1) Пробуем несколько CSS-селекторов.
    2) Если не сработало — берём <li>, где есть и 'Купить', и '₽'.
    3) Если и это не сработало — берём предков текста 'Купить'.
    """
    if soup is None:
        return []

    selectors = [
        "li.product",
        "li.product-item",
        "div.product",
        "div.product-item",
        "div.catalog__item",
        "div.product-card",
    ]

    for sel in selectors:
        elems = soup.select(sel)
        if elems:
            print(f"Нашли {len(elems)} карточек по селектору {sel}")
            return elems

    # Fallback 1: <li>, в которых есть и 'Купить', и '₽'
    li_cards = []
    for li in soup.find_all("li"):
        text = li.get_text(" ", strip=True)
        if "Купить" in text and "₽" in text:
            li_cards.append(li)
    if li_cards:
        print(f"Нашли {len(li_cards)} карточек как <li> с ценой и 'Купить'")
        return li_cards

    # Fallback 2: поднимаемся от текста 'Купить' вверх максимум на 5 уровней
    cards = []
    seen = set()
    for buy in soup.find_all(string=re.compile("Купить", re.I)):
        container = buy.parent
        for _ in range(5):
            if container is None:
                break
            text = container.get_text(" ", strip=True)
            if "₽" in text:
                key = id(container)
                if key not in seen:
                    seen.add(key)
                    cards.append(container)
                break
            container = container.parent

    if cards:
        print(f"Нашли {len(cards)} карточек как предков 'Купить'")

    return cards


def parse_product_card(card, category: str) -> Dict:
    """
    Разбираем одну карточку товара.
    Качественные переменные:
        - category
        - name
        - product_url
    Количественные переменные:
        - current_price_rub
        - old_price_rub
        - discount_percent
        - installment_price_rub
        - installments_count
    """

    # Все тексты внутри карточки
    texts = [t.strip() for t in card.stripped_strings if t.strip()]

    # --- скидка ---
    discount_percent = None
    for t in texts:
        m = re.search(r"(\d+)%", t)
        if m and "-" in t:
            discount_percent = int(m.group(1))
            break

    # --- цены ---
    price_texts = [t for t in texts if "₽" in t]

    # выделяем текст рассрочки вида "3725 ₽ × 4"
    installment_text = next(
        (t for t in price_texts if re.search(r"₽\s*[×x]\s*\d+", t)), None
    )
    if installment_text:
        price_texts = [t for t in price_texts if t is not installment_text]

    prices: List[int] = []
    for t in price_texts:
        for num in re.findall(r"(\d[\d\s]*)\s*₽", t):
            prices.append(clean_price(num))

    current_price = prices[0] if prices else None
    old_price = prices[1] if len(prices) > 1 else None

    # --- рассрочка ---
    installment_price = None
    installments_count = None
    if installment_text:
        m = re.search(r"(\d[\d\s]*)\s*₽\s*[×x]\s*(\d+)", installment_text)
        if m:
            installment_price = clean_price(m.group(1))
            installments_count = int(m.group(2))

    # --- название ---
    name = None

    # сперва пробуем по тегам
    name_tag = (
        card.select_one(".woocommerce-loop-product__title")
        or card.select_one(".product-title")
        or card.select_one(".product-item__title")
        or card.find(["h2", "h3"])
    )
    if name_tag:
        name = name_tag.get_text(strip=True)

    # если не нашли — выдёргиваем из текстов первую строку без ₽/процентов/Купить
    if not name:
        for t in texts:
            if "₽" in t:
                continue
            if "Купить" in t:
                continue
            if re.search(r"\d+%", t):
                continue
            if t.lower().startswith("фильтры"):
                continue
            name = t
            break

    # --- ссылка на товар ---
    product_url = None
    for a in card.find_all("a", href=True):
        href = a["href"]
        if any(part in href for part in ("/category/", "/tovar/")):
            product_url = href
            break

    return {
        "category": category,
        "name": name,
        "current_price_rub": current_price,
        "old_price_rub": old_price,
        "discount_percent": discount_percent,
        "installment_price_rub": installment_price,
        "installments_count": installments_count,
        "product_url": product_url,
    }


def scrape_category(category_name: str, base_url: str) -> List[Dict]:
    """
    Обходит страницы одной категории и собирает товары.
    page=1 — базовый URL, дальше пытаемся /page/2, /page/3 ...
    При 404 выходим.
    """
    results: List[Dict] = []
    page = 1

    while True:
        if page == 1:
            url = base_url
        else:
            url = base_url.rstrip("/") + f"/page/{page}/"

        print(f"\nСобираю {category_name}, страница {page}: {url}")
        soup = download_page(url)

        if soup is None:
            # если не смогли скачать страницу (404 и т.п.) — считаем, что страниц больше нет
            if page > 1:
                print("Больше страниц в категории нет, выходим.")
            break

        cards = find_product_elements(soup)
        if not cards:
            print("Карточки товаров на этой странице не найдены, выходим из категории.")
            break

        print(f"Карточек на странице: {len(cards)}")

        for card in cards:
            try:
                item = parse_product_card(card, category_name)
                # не слишком строгий фильтр: хотя бы название или цена
                if item.get("name") or item.get("current_price_rub"):
                    results.append(item)
            except Exception as e:
                print(f"[ПРЕДУПРЕЖДЕНИЕ] Ошибка при разборе товара: {e}")

        page += 1
        time.sleep(1)  # немного притормаживаем, чтобы не долбить сайт

    return results


# ---------------- ЗАПУСК ПАРСИНГА ---------------- #

all_products: List[Dict] = [
    product
    for cat_name, cat_url in CATEGORY_URLS.items()
    for product in scrape_category(cat_name, cat_url)
]

df = pd.DataFrame(all_products)

print(f"\nВсего товаров собрано: {len(df)}")
print(df.head())

output_file = "arny_praht_products.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"\nДанные сохранены в файл {output_file}")
