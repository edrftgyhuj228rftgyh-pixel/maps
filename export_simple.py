import pandas as pd
import os
print("Экспорт данных...")
df = pd.read_parquet("data/kirovsky_all.parquet")
print(f"Загружено {len(df)} записей")
csv_data = []
for _, row in df.iterrows():
    record = {"id": row.get("id", ""), "name": row.get("name", ""), "address": row.get("address_name", "")}
    point = row.get("point", {})
    if isinstance(point, dict):
        record["lon"] = point.get("lon", "")
        record["lat"] = point.get("lat", "")
    rubrics = row.get("rubrics", [])
    if isinstance(rubrics, list):
        record["rubrics"] = ", ".join([r.get("name", "") for r in rubrics if isinstance(r, dict)])
    csv_data.append(record)
pd.DataFrame(csv_data).to_csv("data/kirovsky_simple.csv", index=False, encoding="utf-8")
print("Готово! data/kirovsky_simple.csv")
