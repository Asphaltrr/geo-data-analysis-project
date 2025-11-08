#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 7 — Nettoyage géométrique et préparation finale
------------------------------------------------------
Entrée  : data_raw/parcelles.geojson
Sorties :
 - data_clean/parcelles_clean.geojson
 - outputs/geojson_clean_log.csv
 - logs/script_7_clean_geojson.log
"""

import os
import logging
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon
from shapely.errors import TopologicalError

# ---------------- CONFIGURATION ----------------

INPUT = "data_raw/parcelles.geojson"
OUTPUT_GEOJSON = "data_clean/parcelles_clean.geojson"
SCRIPT_ID = "script_7"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"
OUTPUT_LOG = f"{OUT_DIR}/geojson_clean_log.csv"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs("data_clean", exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/script_7_clean_geojson.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("clean_geojson")

# ---------------- LECTURE ----------------

logger.info(f"Lecture du GeoJSON brut : {INPUT}")
gdf = gpd.read_file(INPUT)
logger.info(f"{len(gdf)} entités chargées.")

# ---------------- CRS ----------------

if gdf.crs is None:
    logger.warning("⚠️ Aucun CRS détecté — affectation forcée EPSG:4326.")
    gdf.set_crs(epsg=4326, inplace=True)
elif gdf.crs.to_string() != "EPSG:4326":
    logger.info(f"♻️ Reprojection → EPSG:4326 (ancien : {gdf.crs})")
    gdf = gdf.to_crs(epsg=4326)

# ---------------- JOURNAL DES CORRECTIONS ----------------

logs = []

def log_change(identifiant, action, details):
    logs.append({"identifiant": identifiant, "action": action, "details": details})

# ---------------- 1. Suppression géométries vides ----------------

n_empty = gdf.geometry.is_empty.sum()
if n_empty > 0:
    logger.warning(f"🗑️ {n_empty} géométries vides supprimées.")
    for idx in gdf[gdf.geometry.is_empty].index:
        fid = gdf.loc[idx, "Farms_ID"] if "Farms_ID" in gdf.columns else idx
        log_change(fid, "suppression", "géométrie vide")
    gdf = gdf[~gdf.geometry.is_empty]

# ---------------- 2. Correction géométries invalides ----------------

n_invalid = (~gdf.is_valid).sum()
if n_invalid > 0:
    logger.warning(f"🛠️ {n_invalid} géométries invalides détectées — tentative de correction buffer(0).")
    for idx in gdf[~gdf.is_valid].index:
        fid = gdf.loc[idx, "Farms_ID"] if "Farms_ID" in gdf.columns else idx
        try:
            gdf.at[idx, "geometry"] = gdf.at[idx, "geometry"].buffer(0)
            log_change(fid, "correction", "geometry invalid → validé buffer(0)")
        except TopologicalError:
            log_change(fid, "suppression", "geometry irréparable")
            gdf.drop(idx, inplace=True)
else:
    logger.info("✅ Aucune géométrie invalide.")

# ---------------- 3. Suppression doublons attributaires ----------------

if "Farms_ID" in gdf.columns:
    dupli_attr = gdf[gdf.duplicated(subset=["Farms_ID"], keep=False)]
    n_dupli_attr = len(dupli_attr)
    if n_dupli_attr > 0:
        logger.warning(f"🗑️ {n_dupli_attr} doublons d'attribut supprimés (1 seul exemplaire conservé).")
        for fid in dupli_attr["Farms_ID"].unique():
            log_change(fid, "doublon_attribut", "Farms_ID dupliqué")
        gdf = gdf.drop_duplicates(subset=["Farms_ID"], keep="first")
else:
    logger.warning("⚠️ Colonne Farms_ID absente — pas de déduplication possible.")

# ---------------- 4. Suppression doublons géométriques ----------------

logger.info("🔍 Détection des doublons géométriques exacts...")
geom_hash = gdf.geometry.apply(lambda g: None if g is None else g.wkb_hex)
dupli_geom_mask = geom_hash.duplicated(keep=False)

n_dupli_geom = dupli_geom_mask.sum()
if n_dupli_geom > 0:
    logger.warning(f"🗑️ {n_dupli_geom} doublons géométriques détectés.")

    # On ne garde que le premier exemplaire de chaque géométrie unique
    gdf = gdf.loc[~geom_hash.duplicated(keep="first")].copy()

    logger.info(f"✅ {n_dupli_geom} doublons géométriques supprimés (premier conservé).")

    # Journalisation (on refait le hash sur le gdf réduit)
    geom_hash = gdf.geometry.apply(lambda g: None if g is None else g.wkb_hex)
    geom_counts = geom_hash.value_counts()
    for h, cnt in geom_counts.items():
        if cnt > 1:
            # normalement aucun, mais garde la logique
            log_change(f"hash:{h[:16]}", "doublon_géométrique", f"{cnt} occurrences")
else:
    logger.info("✅ Aucun doublon géométrique détecté.")


# ---------------- 5. Recalcul des superficies ----------------

logger.info("📐 Recalcul des superficies réelles (ha)...")
gdf_area = gdf.to_crs(epsg=3857)  # projection métrique
gdf["surface_calculee_ha"] = gdf_area.geometry.area / 10_000

logger.info("✅ Superficies calculées et ajoutées sous 'surface_calculee_ha'.")

# ---------------- 6. EXPORT ----------------

logger.info(f"💾 Export du GeoJSON nettoyé → {OUTPUT_GEOJSON}")
gdf.to_file(OUTPUT_GEOJSON, driver="GeoJSON", encoding="utf-8")

pd.DataFrame(logs).to_csv(OUTPUT_LOG, index=False, encoding="utf-8")
logger.info(f"🧾 Journal des corrections → {OUTPUT_LOG}")

logger.info("✔️ Nettoyage géométrique terminé avec succès.")
