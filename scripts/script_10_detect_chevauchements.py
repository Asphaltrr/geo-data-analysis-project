#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 10 — Détection des chevauchements de parcelles (> 15 %)
Basé sur data_clean/parcelles_clean.geojson
"""

import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
import logging

# ---------------- CONFIGURATION ----------------

SCRIPT_NUM = "script_10"
LOG_DIR = f"logs/{SCRIPT_NUM}"
OUT_DIR = f"outputs/{SCRIPT_NUM}"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

log_path = f"{LOG_DIR}/overlaps.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(SCRIPT_NUM)

GEO_PATH = "data_clean/parcelles_clean.geojson"
OUTPUT_OVERLAP = f"{OUT_DIR}/parcelles_chevauchements.geojson"
OUTPUT_SUMMARY = f"{OUT_DIR}/resume_chevauchements.csv"

# ---------------- LECTURE ----------------

logger.info(f"Lecture du GeoJSON propre : {GEO_PATH}")
gdf = gpd.read_file(GEO_PATH)
logger.info(f"→ {len(gdf)} entités chargées, CRS={gdf.crs}")

# ---------------- DÉTECTION DES CHEVAUCHEMENTS ----------------

logger.info("Détection des chevauchements (>15%) en cours...")

# reprojection en mètre pour calculs de surface fiables
gdf_m = gdf.to_crs(32630)  # UTM zone 30N (Côte d’Ivoire)
overlaps = []

for i, geom1 in enumerate(gdf_m.geometry):
    for j, geom2 in enumerate(gdf_m.geometry[i+1:], start=i+1):
        if geom1.intersects(geom2):
            inter = geom1.intersection(geom2)
            if not inter.is_empty:
                area1 = geom1.area
                area2 = geom2.area
                inter_area = inter.area
                ratio = max(inter_area / area1, inter_area / area2)
                if ratio > 0.15:
                    overlaps.append({
                        "id_1": gdf.iloc[i]["Farms_ID"],
                        "id_2": gdf.iloc[j]["Farms_ID"],
                        "pourcentage_chevauchement": round(ratio * 100, 2)
                    })

logger.info(f"→ {len(overlaps)} chevauchements détectés >15%")

# ---------------- EXPORT ----------------

if overlaps:
    df_over = pd.DataFrame(overlaps)
    df_over.to_csv(OUTPUT_SUMMARY, index=False, encoding="utf-8")
    logger.info(f"📊 Résumé exporté → {OUTPUT_SUMMARY}")

    geometries = []
    for _, row in df_over.iterrows():
        g1 = gdf_m.loc[gdf_m["Farms_ID"] == row["id_1"], "geometry"]
        g2 = gdf_m.loc[gdf_m["Farms_ID"] == row["id_2"], "geometry"]

        if g1.empty or g2.empty:
            geometries.append(None)
            continue

        inter = g1.values[0].intersection(g2.values[0])

        # Nettoyage : ignorer géométries vides ou invalides
        if inter.is_empty or not inter.is_valid:
            geometries.append(None)
        else:
            geometries.append(inter)

    # Construction du GeoDataFrame
    gdf_over = gpd.GeoDataFrame(df_over, geometry=geometries, crs=gdf_m.crs)
    gdf_over = gdf_over[gdf_over.geometry.notna() & (~gdf_over.geometry.is_empty)]

    if len(gdf_over) > 0:
        gdf_over.to_crs(4326).to_file(OUTPUT_OVERLAP, driver="GeoJSON")
        logger.info(f"🗺️ GeoJSON exporté → {OUTPUT_OVERLAP} ({len(gdf_over)} entités)")
    else:
        logger.warning("⚠️ Aucun chevauchement valide exporté dans le GeoJSON.")
else:
    logger.info("✅ Aucun chevauchement significatif détecté.")
