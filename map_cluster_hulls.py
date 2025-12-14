from pathlib import Path
import pandas as pd
import geopandas as gpd
import folium


BASE_DIR = Path(__file__).resolve().parent
DATA_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
OUT_HTML = BASE_DIR / "maps" / "kirovsky_functional_typology.html"

print(f"Читаю данные из: {DATA_CSV}")
df = pd.read_csv(DATA_CSV)

required = {"lat", "lon", "cluster", "category"}
missing = required - set(df.columns)
if missing:
    raise RuntimeError(f"В данных не хватает столбцов: {missing}. Сначала запусти add_categories.py и кластеризацию.")

df = df.dropna(subset=["lat", "lon", "cluster"])
df["category"] = df["category"].fillna("unknown")

# на всякий случай
df["cluster"] = df["cluster"].astype(int)

print("Кластеры:", sorted(df["cluster"].unique()))

# GeoDataFrame с точками
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    crs="EPSG:4326",
)

gdf_3857 = gdf.to_crs(epsg=3857)

hulls = []

for cl in sorted(gdf_3857["cluster"].unique()):
    if cl == -1:
        # шум DBSCAN, можно не включать в типологию
        continue

    sub = gdf_3857[gdf_3857["cluster"] == cl]
    if len(sub) < 3:
        print(f"Кластер {cl}: мало точек ({len(sub)}), пропускаю hull.")
        continue

    # convex hull
    geom_union = sub.geometry.unary_union
    hull = geom_union.convex_hull

    # Топ категорий: здесь была ошибка — фиксируем
    cat_counts = sub["category"].value_counts().reset_index(name="count")
    cat_counts.columns = ["category", "count"]

    top_rows = cat_counts.head(3).to_dict("records")
    top_descr_parts = [f"{r['category']} ({r['count']})" for r in top_rows]
    top_descr = ", ".join(top_descr_parts) if top_descr_parts else "no data"

    hulls.append(
        {
            "cluster": cl,
            "geometry": hull,
            "n_points": int(len(sub)),
            "top_categories": top_descr,
        }
    )

if not hulls:
    raise RuntimeError("Не удалось построить ни одного hull — проверь наличие кластеров != -1.")

hulls_gdf = gpd.GeoDataFrame(hulls, crs="EPSG:3857").to_crs(epsg=4326)

center_lat = float(gdf["lat"].mean())
center_lon = float(gdf["lon"].mean())

m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")

palette = [
    "#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
    "#ff7f00", "#ffff33", "#a65628", "#f781bf",
    "#999999",
]
cluster_colors = {}
for i, cl in enumerate(sorted(hulls_gdf["cluster"].unique())):
    cluster_colors[cl] = palette[i % len(palette)]

for _, row in hulls_gdf.iterrows():
    cl = row["cluster"]
    color = cluster_colors.get(cl, "#000000")

    popup_html = f"""
    <b>Кластер {cl}</b><br>
    Объектов: {row['n_points']}<br>
    Топ категорий: {row['top_categories']}
    """

    folium.GeoJson(
        data=row["geometry"],
        style_function=lambda feature, col=color: {
            "fillColor": col,
            "color": col,
            "weight": 2,
            "fillOpacity": 0.25,
        },
        tooltip=f"Кластер {cl}",
        popup=folium.Popup(popup_html, max_width=300),
    ).add_to(m)

# точки поверх (по желанию)
show_points = True
if show_points:
    for _, row in gdf.iterrows():
        cl = row["cluster"]
        if cl == -1:
            continue
        color = cluster_colors.get(cl, "#000000")

        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=2,
            color=color,
            fill=True,
            fill_opacity=0.7,
        ).add_to(m)

OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
m.save(str(OUT_HTML))

print(f"Карта функциональной типологии сохранена в: {OUT_HTML}")
