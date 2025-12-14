import os
import time
from pathlib import Path
from typing import List, Dict, Any, Set

import requests
import pandas as pd

DATA_DIR = Path("data")
PARQUET_PATH = DATA_DIR / "kirovsky_all.parquet"

API_KEY = "a07319d9-8b64-45fd-bd13-5424148ad318"  # положи ключ в переменную окружения
BASE_URL = "https://catalog.api.2gis.com/3.0/items"  # или твой текущий endpoint


# Сюда вписываешь свои рубрики / rubric_id / q, как ты делаешь сейчас
CATEGORIES = [
    # --- Авто-сервис и мойки ---
    {"code": "car_service_all", "rubric_id": "42903"},   # Автосервис / автотовары (зонт)
    {"code": "car_service_light", "rubric_id": "9041"},  # Легковой автосервис
    {"code": "car_service_truck", "rubric_id": "20989"}, # Грузовой автосервис
    {"code": "car_service_mobile", "rubric_id": "110822"}, # Самообслуживание
    {"code": "car_wash", "rubric_id": "405"},            # Автомойки

    # --- Парковки / стоянки ---
    {"code": "parking", "rubric_id": "60340"},           # Паркинги
    {"code": "parking_open", "rubric_id": "409"},        # Автостоянки
    {"code": "impound_parking", "rubric_id": "18305"},   # Штрафстоянки

    # --- Нефть / топливо ---
    {"code": "gas_station_mobile", "rubric_id": "112463"}, # Мобильные АЗС
    # (обычных АЗС отдельной рубрикой не видно – возможно, они размечены как-то иначе)

    # --- Бары / ночная жизнь ---
    {"code": "bars", "rubric_id": "159"},                # Бары
    {"code": "sushi_bars", "rubric_id": "15791"},        # Суши-бары
    {"code": "night_clubs", "rubric_id": "173"},         # Ночные клубы

    # --- Парки / зеленые зоны ---
    {"code": "parks", "rubric_id": "168"},               # Парки
    {"code": "botanical_garden", "rubric_id": "24169"},  # Ботанический сад
    {"code": "rope_parks", "rubric_id": "110304"},       # Веревочные парки
    {"code": "water_parks_sport", "rubric_id": "110388"},# Водные парки
    {"code": "skate_parks", "rubric_id": "110745"},      # Скейт-парки
    {"code": "gardens_flowers", "rubric_id": "112912"},  # Сады / цветники

    # --- Склады / логистика ---
    {"code": "warehouses", "rubric_id": "5279"},         # Складское хранение
    {"code": "self_storage", "rubric_id": "110406"},     # Склады индив. хранения

    # --- Госуслуги ---
    {"code": "mfc", "rubric_id": "53505"},               # МФЦ
]



def load_existing_ids() -> Set[str]:
    """Грузим уже собранные id из Parquet, чтобы не собирать дубли."""
    if not PARQUET_PATH.exists():
        return set()
    df = pd.read_parquet(PARQUET_PATH)
    if "id" not in df.columns:
        return set()
    return set(df["id"].astype(str))


def fetch_category(cat, existing_ids):
    code = cat["code"]
    rubric_id = cat.get("rubric_id")

    page = 1
    page_size = 10

    total_new = 0
    rows = []

    while True:
        params = {
            "key": API_KEY,
            "page": page,
            "page_size": page_size,
            "fields": "items.point,items.rubrics,items.address",
            # твой фильтр по полигону района (region_id/polygon/...) — как было
        }
        if rubric_id:
            params["rubric_id"] = rubric_id

        resp = requests.get(BASE_URL, params=params)
        data = resp.json()

        if "result" not in data or "items" not in data["result"]:
            print("⚠ Неожиданный формат ответа от API 2ГИС:")
            print(data)
            break

        items = data["result"]["items"]
        if not items:
            print(f"Страниц закончились на page={page}")
            break

        for it in items:
            bid = it.get("id")
            if bid in existing_ids:
                continue
            existing_ids.add(bid)
            # собираешь нужные поля в rows (name, address, rubrics, point...)
            rows.append({...})
            total_new += 1

        page += 1

    print(f"Итого по категории {code}: новых объектов {total_new}")
    # здесь сохраняешь CSV/обновляешь Parquet, как уже делал
    return rows


    print(f"\n=== Сбор категории {code!r} (q={q!r}) ===")

    page = 1
    page_size = 10
    collected: List[Dict[str, Any]] = []

    while True:
        params = {
            "q": q,
            "page": page,
            "page_size": page_size,
            "key": API_KEY,
            "fields": "items.point,items.rubrics,items.address",
            # здесь можно добавить фильтрацию по региону/границам, как у тебя
        }
        print("API_KEY длина:", len(API_KEY))

        resp = requests.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        # Отладка: если в ответе вообще нет result или items — покажем весь ответ
        if "result" not in data or "items" not in data.get("result", {}):
            print("⚠ Неожиданный формат ответа от API 2ГИС:")
            print(data)
            break

        

        new_on_page = 0

        for it in items:
            obj_id = str(it.get("id"))
            if obj_id in existing_ids:
                continue

            # Собираем минимально нужные поля
            row = {
                "id": obj_id,
                "name": it.get("name"),
                "point": it.get("point"),
                "address": it.get("address"),
                "rubrics": it.get("rubrics"),
                "source_category": code,
            }
            collected.append(row)
            existing_ids.add(obj_id)
            new_on_page += 1

        print(f"Страница {page}: всего {len(items)}, новых {new_on_page}")
        page += 1
        time.sleep(0.3)  # немного бережём API

    print(f"Итого по категории {code}: новых объектов {len(collected)}")
    return collected


def main():
    DATA_DIR.mkdir(exist_ok=True)

    existing_ids = load_existing_ids()
    print(f"Уже было объектов в Parquet: {len(existing_ids)}")

    all_new_rows: List[Dict[str, Any]] = []

    for cat in CATEGORIES:
        new_rows = fetch_category(cat, existing_ids)
        if not new_rows:
            continue

        # Сохраняем отдельный CSV по категории
        code = cat["code"]
        csv_path = DATA_DIR / f"kirovsky_extra_{code}.csv"
        pd.DataFrame(new_rows).to_csv(csv_path, index=False)
        print(f"CSV по категории {code} записан: {csv_path}")

        all_new_rows.extend(new_rows)

    if not all_new_rows:
        print("Новых объектов не найдено — Parquet не меняем.")
        return

    df_new = pd.DataFrame(all_new_rows)

    # Обновляем Parquet: склеиваем с существующим, убираем дубли
    if PARQUET_PATH.exists():
        df_old = pd.read_parquet(PARQUET_PATH)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    if "id" in df_all.columns:
        df_all["id"] = df_all["id"].astype(str)
        df_all = df_all.drop_duplicates(subset=["id"])

    df_all.to_parquet(PARQUET_PATH, index=False)
    print(f"Parquet обновлён: {PARQUET_PATH} (строк: {len(df_all)})")


if __name__ == "__main__":
    main()
