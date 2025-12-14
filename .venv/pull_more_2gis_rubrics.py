import os
import json
from pathlib import Path

import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# ==========================
#  НАСТРОЙКИ
# ==========================

# Ключ 2ГИС: берем из переменной окружения DGIS_API_KEY,
# если её нет — можно временно подставить строкой (но лучше через env)
API_KEY = "a07319d9-8b64-45fd-bd13-5424148ad318"

BASE_URL = "https://catalog.api.2gis.com/3.0/items"
REGION_ID = "38"  # Санкт-Петербург

DATA_DIR = Path("data")
PARQUET_PATH = DATA_DIR / "kirovsky_all.parquet"
GEOJSON_PATH = DATA_DIR / "kirovsky.geojson"

PAGE_SIZE = 10  # 2ГИС требует 1..10


# Словарь категорий: доменная категория -> список rubric_id (int)

CATEGORIES = [
    # --- Авто-сервис и мойки ---
    {"code": "auto_infra", "rubric_id": 42903},   # Автосервис / автотовары
    {"code": "auto_infra", "rubric_id": 9041},    # Легковой автосервис
    {"code": "auto_infra", "rubric_id": 20989},   # Грузовой автосервис
    {"code": "auto_infra", "rubric_id": 110822},  # Автосервис самообслуживания
    {"code": "auto_infra", "rubric_id": 405},     # Автомойки

    # --- Парковки / стоянки ---
    {"code": "parking", "rubric_id": 60340},      # Паркинги
    {"code": "parking", "rubric_id": 409},        # Автостоянки
    {"code": "parking", "rubric_id": 18305},      # Штрафстоянки

    # --- Нефть / топливо (мобильные АЗС) ---
    {"code": "gas_mobile", "rubric_id": 112463},  # Мобильные АЗС

    # --- Бары / ночная жизнь ---
    {"code": "nightlife", "rubric_id": 159},      # Бары
    {"code": "nightlife", "rubric_id": 15791},    # Суши-бары
    {"code": "nightlife", "rubric_id": 173},      # Ночные клубы

    # --- Парки / рекреация ---
    {"code": "parks", "rubric_id": 168},          # Парки
    {"code": "parks", "rubric_id": 24169},        # Ботанический сад
    {"code": "parks", "rubric_id": 110304},       # Верёвочные парки
    {"code": "parks", "rubric_id": 110388},       # Парки для водных видов спорта
    {"code": "parks", "rubric_id": 110745},       # Скейт-парки
    {"code": "parks", "rubric_id": 112668},       # Природные достопримечательности

    # --- Склады / логистика ---
    {"code": "warehouses", "rubric_id": 5279},    # Складское хранение
    {"code": "warehouses", "rubric_id": 110406},  # Склады индивидуального хранения

    # --- Госуслуги ---
    {"code": "public_services", "rubric_id": 53505},  # МФЦ

    # --- Медицина ---
    {"code": "medicine", "rubric_id": 207},       # Аптеки
    {"code": "medicine", "rubric_id": 201},       # Больницы
    {"code": "medicine", "rubric_id": 204},       # Ветеринарные аптеки
    {"code": "medicine", "rubric_id": 205},       # Ветеринарные клиники
    {"code": "medicine", "rubric_id": 224},       # Взрослые поликлиники
    {"code": "medicine", "rubric_id": 225},       # Детские поликлиники
    {"code": "medicine", "rubric_id": 112853},    # Детские стоматологические поликлиники
    {"code": "medicine", "rubric_id": 226},       # Стоматологические поликлиники
    {"code": "medicine", "rubric_id": 110653},    # Челюстно-лицевой хирург
    {"code": "medicine", "rubric_id": 10135},     # Школы для будущих мам

    # --- Образование ---
    {"code": "education", "rubric_id": 233},      # Автошколы
    {"code": "education", "rubric_id": 18574},    # Воскресные школы
    {"code": "education", "rubric_id": 683},      # Гимназии
    {"code": "education", "rubric_id": 243},      # Детские музыкальные школы
    {"code": "education", "rubric_id": 237},      # Детские сады
    {"code": "education", "rubric_id": 244},      # Детские художественные школы
    {"code": "education", "rubric_id": 49787},    # Кадетские школы
    {"code": "education", "rubric_id": 55666},    # Киношколы
    {"code": "education", "rubric_id": 246},      # Колледжи
    {"code": "education", "rubric_id": 15287},    # Лицеи
    {"code": "education", "rubric_id": 21040},    # Лицеи-интернаты
    {"code": "education", "rubric_id": 110999},   # Медиашколы
    {"code": "education", "rubric_id": 1058},     # Межшкольные учебные комбинаты
    {"code": "education", "rubric_id": 110402},   # Мотошколы
    {"code": "education", "rubric_id": 15761},    # Начальные школы-детские сады и прогимназии
    {"code": "education", "rubric_id": 15285},    # Профессиональные лицеи
    {"code": "education", "rubric_id": 110311},   # Специальные школы
    {"code": "education", "rubric_id": 633},      # Спортивные школы
    {"code": "education", "rubric_id": 232},      # Университеты
    {"code": "education", "rubric_id": 19080},    # Фотошколы
    {"code": "education", "rubric_id": 67495},    # Цирковые школы
    {"code": "education", "rubric_id": 110405},   # Частные детские сады
    {"code": "education", "rubric_id": 245},      # Школы
    {"code": "education", "rubric_id": 518},      # Школы искусств
    {"code": "education", "rubric_id": 112634},   # Школы приёмных родителей
    {"code": "education", "rubric_id": 687},      # Школы-интернаты
    {"code": "education", "rubric_id": 110404},   # Школьная форма
    {"code": "education", "rubric_id": 675},      # Языковые школы

    # --- Зелёные зоны / сады / усадьбы ---
    {"code": "green_spaces", "rubric_id": 24169}, # Ботанический сад (дублируется в parks — норм)
    {"code": "green_spaces", "rubric_id": 53981}, # Ремонт садового инвентаря
    {"code": "green_spaces", "rubric_id": 4497},  # Садово-парковая мебель
    {"code": "green_spaces", "rubric_id": 10923}, # Садоводческие товарищества
    {"code": "green_spaces", "rubric_id": 397},   # Садовый инвентарь
    {"code": "green_spaces", "rubric_id": 112912},# Сады / цветники
    {"code": "green_spaces", "rubric_id": 385},   # Семена и посадочный материал
    {"code": "green_spaces", "rubric_id": 112911} # Усадьбы
]





# ==========================
#  УТИЛИТЫ
# ==========================

def load_kirovsky_polygon():
    """Читает data/kirovsky.geojson и возвращает объединённый полигон Кировского района."""
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"Не найден файл полигона: {GEOJSON_PATH}")

    gdf = gpd.read_file(GEOJSON_PATH)

    if len(gdf) == 0:
        raise ValueError("В kirovsky.geojson нет геометрий")

    if len(gdf) == 1:
        geom = gdf.geometry.iloc[0]
    else:
        geom = gdf.unary_union

    return geom


def point_in_kirovsky(point_dict, polygon):
    """Проверяет, лежит ли точка внутри полигона Кировского.
       point_dict = {'lat': ..., 'lon': ...} (формат 2ГИС).
    """
    if not isinstance(point_dict, dict):
        return False

    lat = point_dict.get("lat")
    lon = point_dict.get("lon")
    if lat is None or lon is None:
        return False

    # shapely ожидает (x=lon, y=lat)
    pt = Point(lon, lat)
    return polygon.contains(pt)


def load_existing_df_and_ids():
    """Загружает существующий Parquet (если есть) и возвращает (DataFrame, set(ids))."""
    if PARQUET_PATH.exists():
        df = pd.read_parquet(PARQUET_PATH)
        if "id" in df.columns:
            existing_ids = set(df["id"].astype(str).tolist())
        else:
            existing_ids = set()
        print(f"Уже было объектов в Parquet: {len(df)}")
    else:
        df = pd.DataFrame()
        existing_ids = set()
        print("Parquet ещё нет — создаём новый.")
    return df, existing_ids


# ==========================
#  ОСНОВНАЯ ЛОГИКА СБОРА
# ==========================

def fetch_category(cat, existing_ids, polygon):
    """Собирает новые объекты по одной рубрике, фильтрует по СПб и Кировскому району."""
    code = cat["code"]
    rubric_id = cat["rubric_id"]

    print(f"\n=== Сбор категории '{code}' (rubric_id={rubric_id}) ===")

    page = 1
    total_new = 0
    rows = []

    while True:
        params = {
            "key": API_KEY,
            "page": page,
            "page_size": PAGE_SIZE,
            "rubric_id": rubric_id,
            "fields": "items.point,items.rubrics,items.address_name,items.region_id",
            "type": "branch",
        }

        resp = requests.get(BASE_URL, params=params)
        try:
            data = resp.json()
        except Exception as e:
            print("⚠ Не получилось распарсить JSON:", e)
            print("Тело ответа:", resp.text[:500])
            break

        if "result" not in data or "items" not in data["result"]:
            print("⚠ Неожиданный формат ответа от API 2ГИС:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            break

        items = data["result"]["items"]
        if not items:
            print(f"Страницы закончились на page={page}")
            break

        for it in items:
            # фильтруем по региону СПб
            region_id = it.get("region_id")
            if region_id is not None and str(region_id) != REGION_ID:
                continue

            bid = str(it.get("id"))
            if bid in existing_ids:
                continue

            point = it.get("point")
            if not point_in_kirovsky(point, polygon):
                continue

            name = it.get("name")
            address = it.get("address_name")

            rows.append(
                {
                    "id": bid,
                    "name": name,
                    "address": address,
                    "point": json.dumps(point, ensure_ascii=False),
                    "rubrics": json.dumps(it.get("rubrics", []), ensure_ascii=False),
                    "region_id": region_id,
                    "category_code": code,
                }
            )
            existing_ids.add(bid)
            total_new += 1

        print(f"  Страница {page}: найдено объектов (после фильтров) — {total_new}")
        page += 1

    print(f"Итого по категории {code}: новых объектов {total_new}")
    return rows


def main():
    if not API_KEY:
        raise RuntimeError("Не задан API_KEY (переменная окружения DGIS_API_KEY или строка в коде).")

    polygon = load_kirovsky_polygon()

    # Загружаем уже собранные данные
    df_existing, existing_ids = load_existing_df_and_ids()

    all_new_rows = []

    for cat in CATEGORIES:
        new_rows = fetch_category(cat, existing_ids, polygon)
        all_new_rows.extend(new_rows)

    if not all_new_rows:
        print("Новых объектов не найдено — Parquet не меняем.")
        return

    df_new = pd.DataFrame(all_new_rows)
    print(f"\nВсего новых объектов по всем категориям: {len(df_new)}")

    if df_existing.empty:
        df_final = df_new
    else:
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
        # финальная защита от дублей
        df_final = df_final.drop_duplicates(subset=["id"])

    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_parquet(PARQUET_PATH, index=False)
    print(f"Parquet обновлён: {PARQUET_PATH} (всего объектов: {len(df_final)})")


if __name__ == "__main__":
    main()
