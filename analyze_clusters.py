from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent

INPUT_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
OUT_CLUSTER_COUNTS = BASE_DIR / "data" / "cluster_counts.csv"
OUT_CATEGORY_COUNTS = BASE_DIR / "data" / "category_counts.csv"
OUT_CLUSTER_CATEGORY = BASE_DIR / "data" / "cluster_by_category.csv"
OUT_CLUSTER_SUBCAT = BASE_DIR / "data" / "cluster_subcategory_top.csv"

print(f"Читаю данные из: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

print("Столбцы:", df.columns.tolist())
print("Всего строк:", len(df))

if "cluster" not in df.columns:
    raise RuntimeError("В данных нет столбца 'cluster'")
if "category" not in df.columns:
    raise RuntimeError("В данных нет столбца 'category'")
if "subcategory" not in df.columns:
    raise RuntimeError("В данных нет столбца 'subcategory'")


# === 1. Распределение по кластерам ===

cluster_counts = (
    df["cluster"]
    .value_counts(dropna=False)
    .rename_axis("cluster")
    .reset_index(name="count")
    .sort_values("cluster")
)

print("\n=== Распределение по кластерам ===")
print(cluster_counts.to_string(index=False))

cluster_counts.to_csv(OUT_CLUSTER_COUNTS, index=False, encoding="utf-8-sig")
print(f"\nСохранено распределение по кластерам в: {OUT_CLUSTER_COUNTS}")


# === 2. Распределение по категориям ===

cat_counts = (
    df["category"]
    .fillna("unknown")
    .value_counts(dropna=False)
    .rename_axis("category")
    .reset_index(name="count")
)

print("\n=== Распределение по категориям ===")
print(cat_counts.to_string(index=False))

cat_counts.to_csv(OUT_CATEGORY_COUNTS, index=False, encoding="utf-8-sig")
print(f"\nСохранено распределение по категориям в: {OUT_CATEGORY_COUNTS}")


# === 3. Матрица cluster × category ===

cluster_cat = (
    df.assign(category=df["category"].fillna("unknown"))
      .groupby(["cluster", "category"])
      .size()
      .reset_index(name="count")
)

print("\n=== Кластер × категория (первые строки) ===")
print(cluster_cat.head(30).to_string(index=False))

cluster_cat_pivot = (
    cluster_cat
    .pivot_table(index="cluster", columns="category", values="count", fill_value=0)
    .reset_index()
)

cluster_cat.to_csv(OUT_CLUSTER_CATEGORY, index=False, encoding="utf-8-sig")
print(f"\nСохранена длинная форма cluster × category в: {OUT_CLUSTER_CATEGORY}")

# Можно ещё сохранить wide-форму:
OUT_CLUSTER_CATEGORY_WIDE = BASE_DIR / "data" / "cluster_by_category_wide.csv"
cluster_cat_pivot.to_csv(OUT_CLUSTER_CATEGORY_WIDE, index=False, encoding="utf-8-sig")
print(f"Сохранена wide-форма cluster × category в: {OUT_CLUSTER_CATEGORY_WIDE}")


# === 4. ТОП подкатегорий внутри каждого кластера ===

cluster_subcat = (
    df.assign(subcategory=df["subcategory"].fillna("unknown"))
      .groupby(["cluster", "subcategory"])
      .size()
      .reset_index(name="count")
)

# Для удобства — отсортируем внутри кластера по убыванию
cluster_subcat = cluster_subcat.sort_values(["cluster", "count"], ascending=[True, False])

print("\n=== ТОП подкатегорий по кластерам (первые 50 строк) ===")
print(cluster_subcat.head(50).to_string(index=False))

cluster_subcat.to_csv(OUT_CLUSTER_SUBCAT, index=False, encoding="utf-8-sig")
print(f"\nСохранены подкатегории по кластерам в: {OUT_CLUSTER_SUBCAT}")

print("\nГотово.")
