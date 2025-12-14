from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sklearn.neighbors import NearestNeighbors
import folium
from branca.colormap import StepColormap


BASE_DIR = Path(__file__).resolve().parent

DATA_CSV = BASE_DIR / "data" / "kirovsky_data_categorized.csv"
POSSIBLE_GEOJSON = [
    BASE_DIR / "kirovsky.geojson",
    BASE_DIR / "data" / "kirovsky.geojson",
]

OUT_DIR = BASE_DIR / "maps_accessibility"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- настройки визуализации ---
GRID_STEP_M = 250.0
POINT_RADIUS = 7
SERVICE_POINT_RADIUS = 3

# Классы пешей доступности (метры)
BINS = [0, 200, 400, 600, 800, 1200, 2000]
BIN_COLORS = ["#1a9850", "#66bd63", "#a6d96a", "#fee08b", "#fdae61", "#d73027"]


def load_area_polygon():
    geo_path = next((p for p in POSSIBLE_GEOJSON if p.exists()), None)
    if geo_path is None:
        raise RuntimeError("Не найден kirovsky.geojson (ни в корне, ни в data/).")

    print(f"Читаю геометрию района из: {geo_path}")
    gdf_poly = gpd.read_file(geo_path)
    area_geom = gdf_poly.unary_union

    gdf_area_4326 = gpd.GeoDataFrame(geometry=[area_geom], crs="EPSG:4326")
    gdf_area_3857 = gdf_area_4326.to_crs(epsg=3857)
    return gdf_area_4326, gdf_area_3857


def build_grid(area_geom_3857):
    xmin, ymin, xmax, ymax = area_geom_3857.bounds
    xs = np.arange(xmin, xmax, GRID_STEP_M)
    ys = np.arange(ymin, ymax, GRID_STEP_M)

    grid_points = []
    for x in xs:
        for y in ys:
            pt = Point(x, y)
            if area_geom_3857.contains(pt):
                grid_points.append(pt)

    print(f"Точек сетки внутри района: {len(grid_points)} (шаг {GRID_STEP_M} м)")
    return gpd.GeoDataFrame(geometry=grid_points, crs="EPSG:3857")


def classify_bin(d: float) -> int:
    for i in range(len(BINS) - 1):
        if BINS[i] <= d < BINS[i + 1]:
            return i
    return len(BIN_COLORS) - 1


def color_for_distance(d: float) -> str:
    return BIN_COLORS[classify_bin(d)]


def make_accessibility_map(df, area_4326, area_3857, target_categories, title_slug):
    # фильтруем сервис
    df_svc = df[df["category"].isin(target_categories)].copy()
    if df_svc.empty:
        raise RuntimeError(f"Нет объектов в категориях: {target_categories}")

    print(f"[{title_slug}] Объектов: {len(df_svc)}")

    gdf_svc = gpd.GeoDataFrame(
        df_svc,
        geometry=gpd.points_from_xy(df_svc["lon"], df_svc["lat"]),
        crs="EPSG:4326",
    ).to_crs(epsg=3857)

    # сетка
    area_geom_3857 = area_3857.geometry.iloc[0]
    grid_3857 = build_grid(area_geom_3857)

    # расстояния nearest neighbor
    svc_coords = np.vstack([gdf_svc.geometry.x.values, gdf_svc.geometry.y.values]).T
    grid_coords = np.vstack([grid_3857.geometry.x.values, grid_3857.geometry.y.values]).T

    nbrs = NearestNeighbors(n_neighbors=1, algorithm="ball_tree").fit(svc_coords)
    distances, _ = nbrs.kneighbors(grid_coords)
    distances = distances.reshape(-1)

    grid_3857["dist_m"] = distances

    print(f"[{title_slug}] Статистика расстояний (м):")
    print(pd.Series(distances).describe())

    # в WGS84
    grid_4326 = grid_3857.to_crs(epsg=4326)
    svc_4326 = gdf_svc.to_crs(epsg=4326)

    # карта
    center_lat = float(grid_4326.geometry.y.mean())
    center_lon = float(grid_4326.geometry.x.mean())

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")

    # граница района
    folium.GeoJson(
        area_4326.geometry.iloc[0],
        name="Граница района",
        style_function=lambda feature: {"color": "#222222", "weight": 2, "fillOpacity": 0.0},
    ).add_to(m)

    # сетка
    layer_access = folium.FeatureGroup(name=f"Доступность ({title_slug}) — сетка", show=True)
    for pt, d in zip(grid_4326.geometry, grid_4326["dist_m"].values):
        folium.CircleMarker(
            location=[pt.y, pt.x],
            radius=POINT_RADIUS,
            color=color_for_distance(float(d)),
            fill=True,
            fill_opacity=0.75,
            weight=0,
            popup=folium.Popup(f"Расстояние: {int(round(d))} м", max_width=250),
        ).add_to(layer_access)

    layer_access.add_to(m)

    # точки сервиса (выключены по умолчанию)
    layer_points = folium.FeatureGroup(name=f"Точки ({title_slug}) — объектов: {len(svc_4326)}", show=False)
    for _, row in svc_4326.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=SERVICE_POINT_RADIUS,
            color="#000000",
            fill=True,
            fill_opacity=0.9,
            weight=1,
            popup=folium.Popup(row.get("name", ""), max_width=250),
        ).add_to(layer_points)
    layer_points.add_to(m)

    # легенда
    colormap = StepColormap(
        colors=BIN_COLORS,
        index=BINS,
        vmin=BINS[0],
        vmax=BINS[-1],
        caption=f"Доступность: расстояние до ближайшего объекта ({title_slug}), м",
    )
    colormap.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # сохранить
    out_html = OUT_DIR / f"accessibility_{title_slug}.html"
    m.save(str(out_html))
    print(f"[{title_slug}] Карта сохранена в: {out_html}")

    out_csv = BASE_DIR / "data" / f"accessibility_{title_slug}_grid.csv"
    grid_out = grid_4326.copy()
    grid_out["lat"] = grid_out.geometry.y
    grid_out["lon"] = grid_out.geometry.x
    grid_out = grid_out[["lat", "lon", "dist_m"]].rename(columns={"dist_m": f"dist_to_{title_slug}_m"})
    grid_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[{title_slug}] CSV сетки сохранён в: {out_csv}")


def main():
    print(f"Читаю POI из: {DATA_CSV}")
    df = pd.read_csv(DATA_CSV)

    required = {"lat", "lon", "category"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"В данных не хватает столбцов: {missing}")

    df = df.dropna(subset=["lat", "lon"]).copy()
    df["category"] = df["category"].fillna("unknown")

    area_4326, area_3857 = load_area_polygon()

    # Образование
    make_accessibility_map(
        df=df,
        area_4326=area_4326,
        area_3857=area_3857,
        target_categories={"education"},
        title_slug="education",
    )

    # Медицина
    make_accessibility_map(
        df=df,
        area_4326=area_4326,
        area_3857=area_3857,
        target_categories={"healthcare"},
        title_slug="healthcare",
    )

    print("Готово.")


if __name__ == "__main__":
    main()
