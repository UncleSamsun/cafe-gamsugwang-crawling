import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import box, shape
from shapely.ops import unary_union, transform
import pyproj
import folium

# -------------------------------
# 1. 제주도 경계 로드 (Jeju + Seogwipo)
# -------------------------------
jeju_gdf = gpd.read_file("../data/geo/Jeju.geojson")
seogwipo_gdf = gpd.read_file("../data/geo/Seogwipo.geojson")

# CRS 통일 (위경도 기준)
jeju_gdf = jeju_gdf.to_crs("EPSG:4326")
seogwipo_gdf = seogwipo_gdf.to_crs("EPSG:4326")

# 하나로 합치기
merged_gdf = pd.concat([jeju_gdf, seogwipo_gdf], ignore_index=True)
jeju_union = merged_gdf.union_all()

# -------------------------------
# 2. 200m 격자 생성 (위경도 기준)
# -------------------------------
lat_start, lat_end = 33.10, 33.60
lng_start, lng_end = 126.15, 126.98
lat_step = 0.0018
lng_step = 0.00213

def frange(start, stop, step):
    while start < stop:
        yield round(start, 10)
        start += step

grid_cells = []
for lat in frange(lat_start, lat_end, lat_step):
    for lng in frange(lng_start, lng_end, lng_step):
        grid_cells.append({
            "min_lat": round(lat, 6),
            "min_lng": round(lng, 6),
            "max_lat": round(lat + lat_step, 6),
            "max_lng": round(lng + lng_step, 6),
        })

grid_df = pd.DataFrame(grid_cells)
grid_df.to_csv("jeju_grid_rects_200m.csv", index=False)

# -------------------------------
# 3. 제주 경계와 겹치는 셀만 필터링 (면적 기준)
# -------------------------------
# 사각형 셀 geometry 생성
grid_df["geometry"] = grid_df.apply(
    lambda row: box(row["min_lng"], row["min_lat"], row["max_lng"], row["max_lat"]),
    axis=1
)
grid_gdf = gpd.GeoDataFrame(grid_df, geometry="geometry", crs="EPSG:4326")

# 좌표계 변환: WGS84 → UTM-K (EPSG:5179)
project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True).transform
jeju_union_utm = transform(project, jeju_union)
grid_gdf_utm = grid_gdf.to_crs(epsg=5179)

# 면적 기준 필터링 (1만 m² 이상 겹칠 때만)
intersections = grid_gdf_utm.geometry.intersection(jeju_union_utm)
filtered = grid_gdf[intersections.area > 10000]

filtered[["min_lat", "min_lng", "max_lat", "max_lng"]].to_csv("filtered_jeju_grid_rects_200m.csv", index=False)

# -------------------------------
# 4. 지도 시각화
# -------------------------------

m = folium.Map(location=[33.38, 126.55], zoom_start=10)

for _, row in filtered.iterrows():
    bounds = [[row["min_lat"], row["min_lng"]], [row["max_lat"], row["max_lng"]]]
    folium.Rectangle(
        bounds=bounds,
        color="blue",
        fill=True,
        fill_opacity=0.2,
        weight=1
    ).add_to(m)

m.save("jeju_grid_map.html")