import re

import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import folium
from folium.plugins import MarkerCluster


# === 1. Загрузка данных ===
df = pd.read_csv("data/kirovsky_data.csv")

# Ожидаем, что есть колонка "point" формата "POINT (lon lat)"
def parse_point(value):
    if pd.isna(value):
        return None, None
    text = str(value)
    m = re.search(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)", text)
    if not m:
        return None, None
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lat, lon


# Если колонок lat/lon нет — создаём их из point
if "lat" not in df.columns or "lon" not in df.columns:
    lats = []
    lons = []
    for val in df["point"]:
        lat, lon = parse_point(val)
        lats.append(lat)
        lons.append(lon)
    df["lat"] = lats
    df["lon"] = lons

# Убираем строки без координат
df = df.dropna(subset=["lat", "lon"]).copy()

print("Строк с координатами:", len(df))

# === 2. Подготовка данных для DBSCAN ===
coords = df[["lat", "lon"]].to_numpy()

scaler = StandardScaler()
coords_scaled = scaler.fit_transform(coords)

# Подбор параметров можно потом улучшить
dbscan = DBSCAN(eps=0.2, min_samples=5)
labels = dbscan.fit_predict(coords_scaled)
df["cluster"] = labels

print("Уникальные кластеры:", sorted(df["cluster"].unique()))

# === 3. Создание карты ===
center_lat = df["lat"].mean()
center_lon = df["lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
marker_cluster = MarkerCluster().add_to(m)

colors = [
    "red", "blue", "green", "purple", "orange",
    "darkred", "lightred", "beige", "darkblue", "darkgreen",
    "cadetblue", "pink", "gray", "black"
]

for _, row in df.iterrows():
    cluster = int(row["cluster"])
    color = colors[cluster % len(colors)] if cluster != -1 else "gray"

    name = row["name"] if "name" in df.columns else ""
    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=4,
        color=color,
        fill=True,
        fill_opacity=0.7,
        popup=f"{name} (cluster {cluster})"
    ).add_to(marker_cluster)

# === 4. Сохранение карты ===
out_path = "kirovsky_clusters.html"
m.save(out_path)
print(f"Карта сохранена в {out_path}")
