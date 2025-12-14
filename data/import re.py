import re
import time
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# --- Настройки ----------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15"
    )
}

# категории, которые будем собирать
CATEGORY_URLS = {
    "backpacks": "https://arnypraht.com/category/backpacks/",
    "bags": "https://arnypraht.com/category/bags/",
    "accessories": "https://arnypraht.com/category/aksessuary/",
}

# lambda-функция для очистки цены
clean_price = lambda s: int(re.sub(r"\D+", "", s)) if s else None


# --- Функции для парсинга -----------------------------------------------------


def download_page(url: str) -> Optional[BeautifulSoup]:
    """
    Загружает HTML-страницу и возвращает объект BeautifulSoup.
    Если что-то пошло не так — аккуратно ловим исключение.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ОШИБКА] Не удалось получить {url}: {e}")
        return None

    return BeautifulSoup(response.text, "html.parser")


def find_product_elements(soup: BeautifulSoup) -> List:
    """
    Пытается найти карточки товаров разными CSS-селекторами.
    Если по классам ничего не нашли — используем запасной вариант:
    берём родителей кнопок «Купить».
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
            return elems

    # fallback: карточки как родители текста «Купить»
    buy_texts = soup.find_all(string=re.compile("Купить", re.I))
    cards = [txt.parent for txt in buy_texts if txt.parent]
    return cards


def parse_product_card(card, category: str) -> Dict:
    """
    Извлекает данные из одной карточки товара.
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

    # --- название товара ---
    name_tag = (
        card.select_one(".woocommerce-loop-product__title")
        or card.select_one(".product-title")
        or card.select_one(".product-item__title")
        or card.find(["h2", "h3", "a"])
    )
    name = name_tag.get_text(strip=True) if name_tag else None

    # --- цены и рассрочка ---
    price_strings = card.find_all(string=re.compile("₽"))

    prices: List[int] = []
    installment_text = None

    for s in price_strings:
        text = str(s)

        # если это строка вида "875 ₽ × 4" — считаем её рассрочкой и не добавляем в цены
        if re.search(r"₽\s*[×x]\s*\d+", text):
            installment_text = text
            continue

        # вытащить все суммы вида "12345 ₽"
        for num in re.findall(r"(\d[\d\s]*)\s*₽", text):
            prices.append(clean_price(num))

    current_price = prices[0] if prices else None
    old_price = prices[1] if len(prices) > 1 else None

    # --- скидка в процентах ---
    discount_tag = card.find(string=re.compile(r"-\s*\d+%"))
    discount_percent = None
    if discount_tag:
        m = re.search(r"(\d+)%", discount_tag)
        if m:
            discount_percent = int(m.group(1))

    # --- рассрочка "XXXX ₽ × N" ---
    installment_price = None
    installments_count = None
    if installment_text:
        m = re.search(r"(\d[\d\s]*)\s*₽\s*[×x]\s*(\d+)", installment_text)
        if m:
            installment_price = clean_price(m.group(1))
            installments_count = int(m.group(2))

    # --- ссылка на товар ---
    product_url = None
    for a in card.find_all("a", href=True):
        href = a["href"]
        if any(part in href for part in ("/product/", "/collection/", "/shop-")):
            product_url = href
            break
    if not product_url:
        a = card.find("a", href=True)
        if a:
            product_url = a["href"]

    return {
        "category": category,                    # качественная переменная
        "name": name,                            # качественная переменная
        "current_price_rub": current_price,      # количественная
        "old_price_rub": old_price,              # количественная
        "discount_percent": discount_percent,    # количественная
        "installment_price_rub": installment_price,  # количественная
        "installments_count": installments_count,    # количественная
        "product_url": product_url,              # качественная
    }


def scrape_category(category_name: str, base_url: str) -> List[Dict]:
    """
    Обходит все страницы одной категории и собирает товары.
    page=1 — это базовый URL,
    page>1 — URL формата /page/<n>/.
    """
    results: List[Dict] = []
    page = 1

    while True:
        if page == 1:
            url = base_url
        else:
            url = base_url.rstrip("/") + f"/page/{page}/"

        print(f"Собираю {category_name}, страница {page}: {url}")

        soup = download_page(url)
        if soup is None:
            break

        cards = find_product_elements(soup)
        if not cards:
            # если мы уже были хотя бы на одной странице и товаров нет — выходим из цикла
            if page > 1:
                print("Больше страниц в категории нет, останавливаюсь.")
            else:
                print("Не удалось найти карточки товаров на первой странице.")
            break

        for card in cards:
            try:
                item = parse_product_card(card, category_name)
                # отбрасываем совсем пустые строки
                if item.get("name") and item.get("current_price_rub"):
                    results.append(item)
            except Exception as e:
                # try/except, чтобы одна сломанная карточка не роняла весь парсер
                print(f"[ПРЕДУПРЕЖДЕНИЕ] Ошибка при разборе товара: {e}")

        page += 1
        # чтобы не спамить сайт запросами
        time.sleep(1)

    return results


# --- Запуск парсинга ----------------------------------------------------------

all_products: List[Dict] = [
    product
    for cat_name, cat_url in CATEGORY_URLS.items()    # list comprehension
    for product in scrape_category(cat_name, cat_url)
]

df = pd.DataFrame(all_products)

print(f"\nВсего товаров собрано: {len(df)}\n")
print(df.head())

# сохраняем данные в csv-файл
output_file = "arny_praht_products.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"\nДанные сохранены в файл {output_file}")