import re
from pathlib import Path

import pandas as pd


# === 1. Пути к файлам ===

BASE_DIR = Path(__file__).resolve().parent

# если файл в data/
SOURCE_CSV = BASE_DIR / "data" / "kirovsky_data_with_clusters.csv"
OUTPUT_CSV = BASE_DIR / "data" / "kirovsky_data_with_rubrics_parsed.csv"

print(f"Читаю данные из: {SOURCE_CSV}")

df = pd.read_csv(SOURCE_CSV)

print("Столбцы в датафрейме:")
print(df.columns.tolist())
print("Всего строк:", len(df))


# === 2. Функция извлечения rubric_id из сырой строки rubrics ===

def extract_rubric_ids_from_str(r: str):
    """
    В rubrics лежит строка вида:

    "[{'alias': 'kafe', 'id': '161', 'kind': 'additional', ...}
      {'alias': 'banketnye_zaly', 'id': '10803', 'kind': 'primary', ...}]"

    Мы не парсим это как Python-объект, а просто вытаскиваем все 'id': '...'
    с помощью регулярного выражения.
    """
    if not isinstance(r, str):
        return []

    raw_ids = re.findall(r"'id': '(\d+)'", r)
    return [int(x) for x in raw_ids]


print("\nИзвлекаю rubric_ids из rubrics...")
df["rubric_ids"] = df["rubrics"].apply(extract_rubric_ids_from_str)

print("Пример rubric_ids для первых 5 объектов:")
print(df[["name", "rubric_ids"]].head())


def first_or_none(lst):
    return lst[0] if lst else None


df["first_rubric_id"] = df["rubric_ids"].apply(first_or_none)

print("\nРаспределение по наличию хотя бы одного rubric_id:")
has_any = df["rubric_ids"].apply(bool).sum()
print(f"Объектов с хотя бы одним rubric_id: {has_any} из {len(df)}")


# === 3. Сохраняем результат ===

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"\nГотово! Сохранено в: {OUTPUT_CSV}")
