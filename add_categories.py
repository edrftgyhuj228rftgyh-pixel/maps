import ast
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent

INPUT_CSV = BASE_DIR / "data" / "kirovsky_data_with_rubrics_parsed.csv"
OUTPUT_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"

print(f"Читаю данные из: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

print("Столбцы:", df.columns.tolist())
print("Всего строк:", len(df))


# === 1. Разбираем rubric_ids обратно в список int ===

def parse_ids(x):
    """
    В файле rubric_ids хранится как строка вида "[161, 10803]".
    Превращаем её в список [161, 10803].
    """
    if isinstance(x, list):
        return x
    try:
        parsed = ast.literal_eval(str(x))
        if isinstance(parsed, list):
            return [int(i) for i in parsed]
    except Exception:
        pass
    return []


df["rubric_ids_list"] = df["rubric_ids"].apply(parse_ids)


# === 2. Мэппинги rubric_id -> subcategory и subcategory -> category ===

RUBRIC_TO_SUBCATEGORY = {
    # --- ЕДА / НОЧНАЯ ЖИЗНЬ / ОБЩЕПИТ ---
    161: "cafe",                 # Кафе
    159: "bar",                  # Бары
    164: "restaurant",           # Рестораны
    165: "fast_food",            # Быстрое питание
    166: "canteen",              # Столовые
    10803: "banquet_hall",       # Банкетные залы
    16322: "food_delivery",      # Доставка еды
    188: "food_delivery",        # (ещё одна доставка еды)
    13726: "grocery_delivery",   # Доставка продуктов (по смыслу)

    # --- ПРОДУКТЫ / МАГАЗИНЫ ---
    350: "supermarket",          # Супермаркеты
    373: "grocery_store",        # Продуктовые магазины
    360: "alcohol_shop",         # Алкогольные напитки

    # --- ОБРАЗОВАНИЕ ---
    237: "kindergarten",         # Детские сады
    110405: "kindergarten_private",  # Частные детские сады
    895: "child_development_center", # Центры раннего развития детей
    245: "school",               # Школы
    15287: "lyceum",             # Лицеи
    232: "university",           # Университеты
    246: "college",              # Колледжи
    49787: "cadet_school",       # Кадетские школы
    55666: "cinema_school",      # Киношколы
    243: "music_school",         # Детские музыкальные школы
    244: "art_school",           # Детские худ. школы
    518: "art_school",           # Школы искусств
    633: "sport_school",         # Спортивные школы
    67495: "circus_school",      # Цирковые школы
    675: "language_school",      # Языковые школы
    18574: "sunday_school",      # Воскресные школы
    1058: "training_center",     # Межшкольные учебные комбинаты
    110402: "moto_school",       # Мотошколы
    110311: "special_school",    # Специальные школы
    112634: "foster_parent_school",  # Школы приёмных родителей
    687: "boarding_school",      # Школы-интернаты
    19080: "photo_school",       # Фотошколы
    15761: "primary_school_kindergarten",  # Начальные школы + сады

    # --- МЕДИЦИНА ---
    201: "hospital",             # Больницы
    207: "pharmacy",             # Аптеки
    204: "vet_pharmacy",         # Вет. аптеки
    205: "vet_clinic",           # Вет. клиники
    224: "clinic_adult",         # Взрослые поликлиники
    225: "clinic_child",         # Детские поликлиники
    112853: "clinic_child_dental",   # Детские стоматологические поликлиники
    226: "clinic_dental",        # Стоматологические поликлиники
    110653: "maxillofacial_surgery", # Челюстно-лицевой хирург
    537: "diagnostics",          # Медицинская диагностика
    530: "speech_therapist",     # Логопед
    534: "massage",              # Массажист
    10135: "moms_school",        # Школы для будущих мам

    # --- АВТО / ПАРКОВКИ / СКЛАДЫ ---
    42903: "auto_service_complex",   # Автосервис / автотовары (parent)
    9041: "auto_service_light",      # Легковой автосервис
    20989: "auto_service_truck",     # Грузовой автосервис
    110822: "auto_service_self",     # Автосервис самообслуживания
    405: "car_wash",                 # Автомойки
    409: "car_parking",              # Автостоянки
    60340: "parking_multilevel",     # Паркинги
    18305: "impound_parking",        # Штрафстоянки
    112463: "mobile_gas",            # Мобильные АЗС

    5279: "warehouse",               # Складское хранение
    110406: "self_storage",          # Склады инд. хранения

    # --- ГОСУСЛУГИ ---
    53505: "mfc",                    # МФЦ

    # --- ЗЕЛЁНЫЕ ЗОНЫ / ПРИРОДА ---
    168: "park",                     # Парки
    24169: "botanical_garden",       # Ботанический сад
    110304: "rope_park",             # Верёвочные парки
    110388: "water_sport_park",      # Парки для водных видов спорта
    110745: "skate_park",            # Скейт-парки
    112668: "natural_sight",         # Природные достопримечательности
    53981: "garden_tool_repair",     # Ремонт садового инвентаря
    4497: "garden_furniture",        # Садово-парковая мебель
    10923: "garden_association",     # Садоводческие товарищества
    397: "garden_tools",             # Садовый инвентарь
    112912: "gardens_flowers",       # Сады / цветники
    385: "seeds",                    # Семена и посадочный материал
    112911: "manor",                 # Усадьбы

    # --- ФИНАНСЫ / B2B / УСЛУГИ ---
    492: "bank",                     # Банки
    498: "atm",                      # Банкоматы (по смыслу)
    522: "atm_or_payment",           # Банкоматы / терминалы
    50372: "currency_exchange",      # Обмен валюты
    661: "currency_exchange",        # Обмен валюты (другая рубрика)
    5683: "payment_terminal",        # Платёжные терминалы
    495: "stock_operations",         # Операции на фондовом рынке
    1577: "mortgage",                # Ипотечные кредиты
    47633: "finance_other",          # Финуслуги / убедимся по parent_id 969
    10446: "finance_other",          # Финуслуги (по parent_id)
    307: "e_signature",              # Электронные подписи
    1613: "business_registration",   # Регистрация/ликвидация предприятий
}


SUBCATEGORY_TO_CATEGORY = {
    # Еда / общепит
    "cafe": "food_drink",
    "bar": "food_drink",
    "restaurant": "food_drink",
    "fast_food": "food_drink",
    "canteen": "food_drink",
    "banquet_hall": "food_drink",
    "food_delivery": "food_drink",

    # Продуктовый ритейл
    "supermarket": "retail_food",
    "grocery_store": "retail_food",
    "grocery_delivery": "retail_food",
    "alcohol_shop": "retail_food",

    # Образование
    "kindergarten": "education",
    "kindergarten_private": "education",
    "child_development_center": "education",
    "school": "education",
    "lyceum": "education",
    "university": "education",
    "college": "education",
    "cadet_school": "education",
    "cinema_school": "education",
    "music_school": "education",
    "art_school": "education",
    "sport_school": "education",
    "circus_school": "education",
    "language_school": "education",
    "sunday_school": "education",
    "training_center": "education",
    "moto_school": "education",
    "special_school": "education",
    "foster_parent_school": "education",
    "boarding_school": "education",
    "photo_school": "education",
    "primary_school_kindergarten": "education",

    # Медицина
    "hospital": "healthcare",
    "pharmacy": "healthcare",
    "vet_pharmacy": "healthcare",
    "vet_clinic": "healthcare",
    "clinic_adult": "healthcare",
    "clinic_child": "healthcare",
    "clinic_child_dental": "healthcare",
    "clinic_dental": "healthcare",
    "maxillofacial_surgery": "healthcare",
    "diagnostics": "healthcare",
    "speech_therapist": "healthcare",
    "massage": "healthcare",
    "moms_school": "healthcare",

    # Авто / парковки / склады
    "auto_service_complex": "auto_mobility",
    "auto_service_light": "auto_mobility",
    "auto_service_truck": "auto_mobility",
    "auto_service_self": "auto_mobility",
    "car_wash": "auto_mobility",
    "car_parking": "auto_mobility",
    "parking_multilevel": "auto_mobility",
    "impound_parking": "auto_mobility",
    "mobile_gas": "auto_mobility",

    "warehouse": "warehouses",
    "self_storage": "warehouses",

    # Госуслуги
    "mfc": "public_services",

    # Зелёные зоны
    "park": "green_spaces",
    "botanical_garden": "green_spaces",
    "rope_park": "green_spaces",
    "water_sport_park": "green_spaces",
    "skate_park": "green_spaces",
    "natural_sight": "green_spaces",
    "garden_tool_repair": "green_spaces",
    "garden_furniture": "green_spaces",
    "garden_association": "green_spaces",
    "garden_tools": "green_spaces",
    "gardens_flowers": "green_spaces",
    "seeds": "green_spaces",
    "manor": "green_spaces",

    # Финансы / услуги
    "bank": "finance",
    "atm": "finance",
    "atm_or_payment": "finance",
    "currency_exchange": "finance",
    "payment_terminal": "finance",
    "stock_operations": "finance",
    "mortgage": "finance",
    "finance_other": "finance",
    "e_signature": "business_services",
    "business_registration": "business_services",
}


# === 3. Функции мэппинга ===

def map_subcategory(rubric_ids):
    """
    Берём список rubric_ids и пытаемся найти первый,
    который есть в словаре RUBRIC_TO_SUBCATEGORY.
    """
    for rid in rubric_ids:
        if rid in RUBRIC_TO_SUBCATEGORY:
            return RUBRIC_TO_SUBCATEGORY[rid]
    return None


def map_category(subcat):
    if subcat is None:
        return None
    return SUBCATEGORY_TO_CATEGORY.get(subcat)


df["subcategory"] = df["rubric_ids_list"].apply(map_subcategory)
df["category"] = df["subcategory"].apply(map_category)

print("\nПример категорий для первых 15 объектов:")
print(df[["name", "rubric_ids_list", "subcategory", "category"]].head(15).to_string(index=False))

print("\nРаспределение по категориям:")
print(df["category"].value_counts(dropna=False))


# === 4. Сохраняем результат ===

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"\nГотово! Сохранено в: {OUTPUT_CSV}")
