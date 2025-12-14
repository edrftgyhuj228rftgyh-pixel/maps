import json
import pandas as pd
import requests

API_KEY = "a07319d9-8b64-45fd-bd13-5424148ad318"
REGION_ID = "38"

url = "https://catalog.api.2gis.com/3.0/rubricate/list"

params = {
    "region_id": REGION_ID,
    "key": API_KEY,
}

resp = requests.get(url, params=params)
data = resp.json()

rubrics = data["result"]["rubrics"]
rows = []

for r in rubrics:
    rows.append({
        "id": r.get("id"),
        "name": r.get("name"),
        "alias": r.get("alias"),
        "parent_id": r.get("parent_id"),
        "section_id": r.get("section_id"),
        "group_id": r.get("group_id"),
    })

df = pd.DataFrame(rows)
df.to_csv("rubrics_full_spb.csv", index=False, encoding="utf-8")
print("Сохранено рубрик:", len(df))
