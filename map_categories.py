import pandas as pd
import folium
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
OUTPUT_HTML = BASE_DIR / "maps" / "kirovsky_categories_map.html"

df = pd.read_csv(DATA_CSV)

categories = sorted(df["category"].dropna().unique())

center_lat = df["lat"].mean()
center_lon = df["lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")


# Цветовая палитра
palette = [
    "red", "blue", "green", "purple", "orange", "pink",
    "yellow", "black", "gray", "brown"
]

color_map = {cat: palette[i % len(palette)] for i, cat in enumerate(categories)}

# Слой для каждой категории
for cat in categories:
    layer = folium.FeatureGroup(name=f"{cat}", show=False)
    df_cat = df[df["category"] == cat]

    for _, row in df_cat.iterrows():
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=4,
            fill=True,
            fill_opacity=0.9,
            color=color_map[cat],
            popup=f"{row['name']} ({row['subcategory']})"
        ).add_to(layer)

    layer.add_to(m)

folium.LayerControl().add_to(m)

OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
m.save(str(OUTPUT_HTML))

print(f"Карта сохранена в: {OUTPUT_HTML}")
