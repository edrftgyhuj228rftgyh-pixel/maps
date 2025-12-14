import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from sklearn.neighbors import KernelDensity
import folium
from folium.plugins import HeatMap


BASE_DIR = Path(__file__).resolve().parent
DATA_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
MAPS_DIR = BASE_DIR / "maps_kde"

print(f"Читаю данные из: {DATA_CSV}")
df = pd.read_csv(DATA_CSV)

required_cols = {"lat", "lon", "category"}
missing = required_cols - set(df.columns)
if missing:
    raise RuntimeError(f"Не хватает столбцов в данных: {missing}. Сначала запусти add_categories.py")

# убираем строки без координат или категории
df = df.dropna(subset=["lat", "lon"])
df["category"] = df["category"].fillna("unknown")

# Можно ограничиться только интересными категориями
target_categories = [
    "food_drink",
    "retail_food",
    "education",
    "healthcare",
    "auto_mobility",
    "green_spaces",
    "finance",
]

# Если список пуст или категорий мало — возьмём все
if not target_categories:
    target_categories = sorted(df["category"].unique())

print("Категории, по которым строим KDE:", target_categories)

# Превращаем в GeoDataFrame
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    crs="EPSG:4326",
)

MAPS_DIR.mkdir(parents=True, exist_ok=True)


def build_kde_map_for_category(cat: str):
    sub = gdf[gdf["category"] == cat].copy()
    if len(sub) < 5:
        print(f"[{cat}] Слишком мало объектов ({len(sub)}), пропускаю.")
        return

    print(f"[{cat}] Объектов: {len(sub)}")

    # Проецируем в метры (WebMercator), чтобы bandwidth был в метрах
    sub_3857 = sub.to_crs(epsg=3857)
    X = np.vstack([sub_3857.geometry.x.values, sub_3857.geometry.y.values]).T

    # Границы с небольшим отступом
    xmin, ymin, xmax, ymax = sub_3857.total_bounds
    padding = 400  # метров
    xmin -= padding
    xmax += padding
    ymin -= padding
    ymax += padding

    # Сетка для оценки KDE
    grid_size = 70  # чем больше — тем детальнее, но дольше
    xs = np.linspace(xmin, xmax, grid_size)
    ys = np.linspace(ymin, ymax, grid_size)
    xx, yy = np.meshgrid(xs, ys)
    grid_coords = np.vstack([xx.ravel(), yy.ravel()]).T

    # KDE
    bandwidth = 300.0  # метров, можно подкрутить под масштаб района
    kde = KernelDensity(bandwidth=bandwidth, kernel="gaussian")
    kde.fit(X)
    z = np.exp(kde.score_samples(grid_coords))

    # Нормализация плотности в [0, 1]
    z_min, z_max = float(z.min()), float(z.max())
    z_norm = (z - z_min) / (z_max - z_min + 1e-9)

    # Обратно в широту/долготу
    grid_gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(grid_coords[:, 0], grid_coords[:, 1]),
        crs="EPSG:3857",
    ).to_crs(epsg=4326)

    lats = grid_gdf.geometry.y.values
    lons = grid_gdf.geometry.x.values

    # Данные для HeatMap: [lat, lon, weight]
    heat_data = [
        [float(lat), float(lon), float(w)]
        for lat, lon, w in zip(lats, lons, z_norm)
        if w > 0.05  # фильтруем совсем слабый фон
    ]

    # Центр карты по этой категории
    sub_4326 = sub_3857.to_crs(epsg=4326)
    center_lat = float(sub_4326.geometry.y.mean())
    center_lon = float(sub_4326.geometry.x.mean())

    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="cartodbpositron")

    # KDE heatmap
    HeatMap(
        heat_data,
        radius=18,
        blur=25,
        max_zoom=18,
        min_opacity=0.2,
        max_val=1.0,
    ).add_to(m)

    # Можно дополнительно добавить реальные точки (тонкие маркеры)
    for _, row in sub_4326.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=2,
            color="black",
            fill=True,
            fill_opacity=0.6,
            popup=row.get("name", ""),
        ).add_to(m)

    out_path = MAPS_DIR / f"kde_{cat}.html"
    m.save(str(out_path))
    print(f"[{cat}] Карта сохранена в: {out_path}")


for cat in target_categories:
    build_kde_map_for_category(cat)

print("Готово.")
