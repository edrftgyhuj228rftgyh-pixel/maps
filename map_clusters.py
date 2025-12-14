import pandas as pd
import folium
from folium.plugins import MarkerCluster
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
OUTPUT_HTML = BASE_DIR / "maps" / "kirovsky_clusters_map.html"

df = pd.read_csv(DATA_CSV)

# Центр карты — среднее по координатам
center_lat = df["lat"].mean()
center_lon = df["lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")


# Цвета для кластеров
cluster_colors = {
    -1: "gray",
     0: "red",
     1: "blue",
     2: "green",
     3: "purple",
     4: "orange",
     5: "pink",
}

# Добавляем MarkerCluster (группировка точек)
marker_cluster = MarkerCluster().add_to(m)

for _, row in df.iterrows():
    cl = row["cluster"]
    color = cluster_colors.get(cl, "black")

    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=2,
        fill=True,
        fill_opacity=0.9,
        color=color,
        popup=folium.Popup(f"""
            <b>{row['name']}</b><br>
            Category: {row['category']}<br>
            Subcategory: {row['subcategory']}<br>
            Cluster: {cl}
        """, max_width=300)
    ).add_to(marker_cluster)

# Сохраняем карту
OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
m.save(str(OUTPUT_HTML))

print(f"Карта сохранена в: {OUTPUT_HTML}")
