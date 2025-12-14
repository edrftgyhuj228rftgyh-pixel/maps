import os
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import requests
from dotenv import load_dotenv
from shapely.geometry import shape, Point

# =========================
# 0) Настройки и ключ
# =========================
load_dotenv(dotenv_path="data/.env")
DGIS_KEY = os.getenv("DGIS_KEY", "a07319d9-8b64-45fd-bd13-5424148ad318")
if not DGIS_KEY:
    raise RuntimeError("Нет ключа DGIS_KEY в data/.env")
print("DGIS_KEY:", DGIS_KEY[:8] + "...")

CATALOG_API = "https://catalog.api.2gis.com/3.0/items"
POLYGON_PATH = os.getenv("POLYGON_PATH", "data/kirovsky.geojson")
NX = int(os.getenv("NX", "3"))  # Оптимальный баланс покрытия и скорости
NY = int(os.getenv("NY", "3"))

# Пути к файлам
MASTER_PARQUET = "data/kirovsky_all.parquet"

# =========================
# 1) Управление Parquet файлом
# =========================
def load_existing_ids() -> set:
    """Загружает существующие ID из мастер-файла для дедупликации."""
    if not os.path.exists(MASTER_PARQUET):
        return set()
    
    try:
        table = pq.read_table(MASTER_PARQUET, columns=['id'])
        return set(table.column('id').to_pylist())
    except Exception as e:
        print(f"⚠️ Ошибка чтения {MASTER_PARQUET}: {e}")
        return set()

def append_to_parquet(new_items: list, existing_ids: set) -> set:
    """
    Добавляет новые items в Parquet файл, избегая дубликатов.
    Возвращает обновленный набор ID.
    """
    if not new_items:
        return existing_ids
    
    # Фильтрация дубликатов
    unique_new_items = []
    for item in new_items:
        item_id = item.get('id')
        if item_id and item_id not in existing_ids:
            unique_new_items.append(item)
            existing_ids.add(item_id)
    
    if not unique_new_items:
        print("🤷 Все объекты уже существуют, пропускаем")
        return existing_ids
    
    # Создаем DataFrame
    df = pd.DataFrame(unique_new_items)
    
    # Добавляем timestamp сбора
    df['_collected_at'] = pd.Timestamp.now()
    
    # Сохраняем
    if os.path.exists(MASTER_PARQUET):
        # Append к существующему файлу
        existing_table = pq.read_table(MASTER_PARQUET)
        new_table = pa.Table.from_pandas(df, schema=existing_table.schema)
        combined_table = pa.concat_tables([existing_table, new_table])
        pq.write_table(combined_table, MASTER_PARQUET)
    else:
        # Создаем новый файл
        os.makedirs(os.path.dirname(MASTER_PARQUET), exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df), MASTER_PARQUET)
    
    print(f"💾 Добавлено {len(unique_new_items)} записей в {MASTER_PARQUET}")
    return existing_ids

# =========================
# 2) Гео-утилиты
# =========================
def geojson_geom(geojson_path: str):
    """Читает Polygon/MultiPolygon из GeoJSON."""
    with open(geojson_path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    if gj.get("type") == "FeatureCollection":
        geom = shape(gj["features"][0]["geometry"])
    elif gj.get("type") == "Feature":
        geom = shape(gj["geometry"])
    else:
        geom = shape(gj)
    return geom

def make_tiles(minx: float, miny: float, maxx: float, maxy: float, nx: int, ny: int):
    """Разбивает bbox на nx×ny тайлов."""
    tiles = []
    dx = (maxx - minx) / nx
    dy = (maxy - miny) / ny
    for j in range(ny):
        lat_top = maxy - j * dy
        lat_bottom = maxy - (j + 1) * dy
        for i in range(nx):
            lon_left = minx + i * dx
            lon_right = minx + (i + 1) * dx
            point1 = f"{lon_left},{lat_top}"
            point2 = f"{lon_right},{lat_bottom}"
            tiles.append((point1, point2))
    return tiles

# =========================
# 3) Работа с API
# =========================
def fetch_page(params: dict, page: int = 1, page_size: int = 10) -> dict:
    """
    Запрос одной страницы; лимиты 2ГИС: page_size 1..10, page 1..5.
    Возвращает dict с ключами items/total. 404 трактуем как пустую страницу.
    """
    if not (1 <= page_size <= 10):
        raise ValueError("page_size должен быть 1..10")
    if not (1 <= page <= 5):
        raise ValueError("page должен быть 1..5")

    p = dict(params)
    p.update({"page": page, "page_size": page_size, "key": DGIS_KEY})
    
    try:
        r = requests.get(CATALOG_API, params=p, timeout=30)
    except requests.exceptions.Timeout:
        print(f"       ⏱️ Таймаут запроса, страница {page}")
        return {"items": [], "total": 0}
    except requests.exceptions.ConnectionError:
        print(f"       🔌 Ошибка соединения, страница {page}")
        return {"items": [], "total": 0}
    except Exception as e:
        print(f"       ❌ Ошибка запроса: {e}")
        return {"items": [], "total": 0}

    # HTTP 404 — пусто
    if r.status_code == 404:
        return {"items": [], "total": 0}
    if r.status_code != 200:
        print(f"       ❌ HTTP {r.status_code} для страницы {page}")
        return {"items": [], "total": 0}

    data = r.json()
    meta = data.get("meta", {})
    code = meta.get("code")

    # Код 404 в meta — тоже пусто
    if code == 404:
        return {"items": [], "total": 0}

    if code != 200:
        error_msg = meta.get('error', {}).get('message') or 'Unknown error'
        print(f"       ❌ 2GIS error {code}: {error_msg}")
        return {"items": [], "total": 0}

    result = data.get("result") or {}
    result.setdefault("items", [])
    result.setdefault("total", 0)
    return result

def fetch_all(params: dict, sleep: float = 0.3, page_size: int = 10) -> list:
    """Перебор страниц в рамках лимита API: page 1..5. Пустые страницы (404) пропускаем."""
    items_all = []
    for page in range(1, 6):
        res = fetch_page(params, page=page, page_size=page_size)
        items = res.get("items", []) or []
        total = res.get("total", 0) or 0

        if not items:
            break

        items_all.extend(items)
        print(f"       📄 Страница {page}: {len(items)} объектов")

        if total and page * page_size >= total:
            break

        time.sleep(sleep)

    return items_all

# =========================
# 4) Сохранение
# =========================
def save_category_files(items: list, out_prefix: str):
    """Сохраняет данные категории в CSV и GeoJSON."""
    
    if not items:
        print("     📭 Нет данных для сохранения")
        return
    
    csv_path = f"data/{out_prefix}.csv"
    gj_path = f"data/{out_prefix}.geojson"
    
    # CSV
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    csv_data = []
    for it in items:
        pt = it.get("point") or {}
        rubrics = ",".join([r.get("name", "") for r in it.get("rubrics", [])])
        csv_data.append({
            "id": it.get("id"),
            "name": it.get("name"),
            "address_name": it.get("address_name") or (it.get("address") or {}).get("name"),
            "lon": pt.get("lon"),
            "lat": pt.get("lat"),
            "rubrics": rubrics,
        })
    
    df = pd.DataFrame(csv_data)
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    # GeoJSON
    feats = []
    for it in items:
        pt = it.get("point")
        if not pt:
            continue
        feats.append({
            "type": "Feature",
            "properties": {
                "id": it.get("id"),
                "name": it.get("name"),
                "address_name": it.get("address_name") or (it.get("address") or {}).get("name"),
                "rubrics": ",".join([r.get("name", "") for r in it.get("rubrics", [])]),
            },
            "geometry": {"type": "Point", "coordinates": [pt["lon"], pt["lat"]]},
        })
    
    fc = {"type": "FeatureCollection", "features": feats}
    with open(gj_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
    
    print(f"     💾 Файлы: {os.path.basename(csv_path)}, {os.path.basename(gj_path)}")

# =========================
# 5) Основной процесс: тайлинг + дедуп + фильтр по полигону
# =========================
def pull_for_polygon(geojson_path: str, query_text: str, out_prefix: str, 
                    existing_ids: set, nx: int = NX, ny: int = NY) -> set:
    """
    Сбор данных для полигона с немедленным добавлением в Parquet.
    Возвращает обновленный набор existing_ids.
    """
    poly = geojson_geom(geojson_path)
    minx, miny, maxx, maxy = poly.bounds

    tiles = make_tiles(minx, miny, maxx, maxy, nx=nx, ny=ny)
    print(f"   🗺️  Тайлов: {len(tiles)} ({nx}×{ny})")

    # собираем со всех тайлов, дедуп по id
    all_items = []
    successful_tiles = 0
    
    for idx, (point1, point2) in enumerate(tiles, start=1):
        params = {
            "q": query_text,
            "type": "branch",
            "point1": point1,   # левый-верхний
            "point2": point2,   # правый-нижний
            "fields": "items.point,items.address,items.rubrics",
        }
        print(f"     📍 Тайл {idx}/{len(tiles)}")
        batch = fetch_all(params, page_size=10)
        
        if batch:
            successful_tiles += 1
            print(f"       ✅ Найдено: {len(batch)} объектов")
        else:
            print(f"       ❌ Не найдено объектов")
        
        # Фильтрация по полигону
        filtered_batch = []
        for it in batch:
            p = it.get("point")
            if p and poly.contains(Point(p["lon"], p["lat"])):
                filtered_batch.append(it)
        
        all_items.extend(filtered_batch)
        time.sleep(0.2)  # уменьшили паузу между тайлами

    print(f"   ✅ Успешных тайлов: {successful_tiles}/{len(tiles)}")

    # Дедупликация на уровне категории
    unique_items = []
    seen_ids = set()
    for item in all_items:
        item_id = item.get('id')
        if item_id and item_id not in seen_ids:
            unique_items.append(item)
            seen_ids.add(item_id)
    
    print(f"   🔍 Уникальных в категории: {len(unique_items)}")

    # Сохраняем файлы категории
    save_category_files(unique_items, out_prefix)
    
    # Добавляем в основной Parquet
    updated_ids = append_to_parquet(unique_items, existing_ids)
    
    return updated_ids

# =========================
# 6) Запуск
# =========================
def get_parquet_stats():
    """Возвращает статистику по основному Parquet файлу."""
    if not os.path.exists(MASTER_PARQUET):
        return 0, 0.0
    
    try:
        table = pq.read_table(MASTER_PARQUET)
        count = table.num_rows
        size_mb = os.path.getsize(MASTER_PARQUET) / (1024 * 1024)
        return count, size_mb
    except Exception as e:
        print(f"⚠️ Ошибка чтения статистики: {e}")
        return 0, 0.0

if __name__ == "__main__":
    # Проверяем существование полигона
    if not os.path.exists(POLYGON_PATH):
        print(f"❌ Файл полигона не найден: {POLYGON_PATH}")
        print("Убедитесь, что файл kirovsky.geojson находится в папке data/")
        exit(1)
    
    print("=" * 60)
    print("🗺️  2ГИС Сборщик данных для Кировского района")
    print("=" * 60)
    
    # Загружаем существующие ID для дедупликации
    existing_ids = load_existing_ids()
    initial_count, initial_size = get_parquet_stats()
    print(f"📊 Начальная статистика:")
    print(f"   • Уникальных объектов: {initial_count}")
    print(f"   • Размер Parquet: {initial_size:.2f} MB")
    
    # СОКРАЩЕННЫЙ список ключевых категорий (15 вместо 50+)
    rubrics = [
        # Основные категории еды
        ("кафе", "food"),
        ("ресторан", "food"),
        ("столовая", "food"),
        
        # Здоровье и медицина
        ("аптека", "health"),
        ("поликлиника", "health"),
        
        # Образование
        ("школа", "education"),
        ("детский сад", "education"),
        
        # Транспорт
        ("остановка", "transport"),
        ("парковка", "transport"),
        ("метро", "transport"),
        
        # Магазины
        ("супермаркет", "retail"),
        ("магазин", "retail"),
        
        # Услуги
        ("банк", "services"),
        ("банкомат", "services"),
        
        # Развлечения
        ("кинотеатр", "entertainment"),
    ]

    # Обрабатываем рубрики
    total_categories = len(rubrics)
    print(f"\n🔍 Будет собрано {total_categories} ключевых категорий")
    print("⏰ Ориентировочное время: 20-40 минут")
    
    start_time = time.time()
    
    for i, (query_text, cls) in enumerate(rubrics, 1):
        safe_name = f"kirovsky_{cls}_{query_text.replace(' ', '_')}"
        print(f"\n[{i}/{total_categories}] 🔍 Сбор: '{query_text}'")
        print("-" * 40)
        
        try:
            existing_ids = pull_for_polygon(POLYGON_PATH, query_text, safe_name, existing_ids, nx=NX, ny=NY)
        except Exception as e:
            print(f"❌ Критическая ошибка в категории '{query_text}': {e}")
            print("Продолжаем со следующей категорией...")
            continue
        
        # Уменьшенная пауза между категориями
        if i < total_categories:
            print("     ⏳ Пауза 1 секунда...")
            time.sleep(1)
    
    # Финальная статистика
    end_time = time.time()
    elapsed_time = end_time - start_time
    final_count, final_size = get_parquet_stats()
    
    print(f"\n{'='*60}")
    print(f"🎉 БЫСТРЫЙ СБОР ДАННЫХ ЗАВЕРШЕН!")
    print(f"{'='*60}")
    print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   • Всего уникальных объектов: {final_count}")
    print(f"   • Добавлено за сессию: {final_count - initial_count}")
    print(f"   • Размер Parquet файла: {final_size:.2f} MB")
    print(f"   • Затраченное время: {elapsed_time/60:.1f} минут")
    print(f"   • Категорий собрано: {total_categories}")
    print(f"\n💾 Основной файл: {MASTER_PARQUET}")
    
    # Если нужно добавить больше категорий позже
    if final_count > initial_count:
        print(f"\n✅ Успешно собраны ключевые POI!")
        print("   Чтобы добавить больше категорий, просто запустите скрипт снова")
        print("   с расширенным списком rubrics")