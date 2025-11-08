#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 8 — Comparaison surfaces déclarées (CSV) vs calculées (GeoJSON)
----------------------------------------------------------------------
Granularité : PARCELLE (une ligne = une plantation)

Entrées :
 - data_clean/coop_plantations_clean.csv      (colonnes attendues : code_plantation, superficie_cacao_ha)
 - data_clean/parcelles_clean.geojson         (colonnes attendues : Farms_ID, surface_calculee_ha)

Sorties :
 - outputs/surfaces_compare_parcelle.csv      (jointure + écart %)
 - outputs/anomalies_surfaces_parcelle.csv    (écart > ±10 %)
 - logs/script_8_compare_surfaces.log
"""

import os
import logging
import pandas as pd
import geopandas as gpd
import numpy as np

# ---------------- CONFIG ---------------- #

CSV_PLANT = "data_clean/coop_plantations_clean.csv"
GEOJSON   = "data_clean/parcelles_clean.geojson"

SCRIPT_ID = "script_8"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"

OUT_ALL   = f"{OUT_DIR}/surfaces_compare_parcelle.csv"
OUT_ANOM  = f"{OUT_DIR}/anomalies_surfaces_parcelle.csv"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/script_8_compare_surfaces.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("compare_surfaces")

# ---------------- UTILS ---------------- #

def to_float(x):
    try:
        if pd.isna(x): 
            return np.nan
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return np.nan

# ---------------- LOAD ---------------- #

logger.info(f"Lecture CSV plantations : {CSV_PLANT}")
dfp = pd.read_csv(CSV_PLANT, dtype=str)
logger.info(f"  → {dfp.shape[0]} lignes, {dfp.shape[1]} colonnes")

logger.info(f"Lecture GeoJSON propre : {GEOJSON}")
gdf = gpd.read_file(GEOJSON)
logger.info(f"  → {gdf.shape[0]} entités, CRS={gdf.crs}")

# ---------------- SÉLECTION COLONNES ---------------- #

# CSV plantations : on garde l’essentiel
keep_csv = ["code_plantation", "code_producteur", "cooperative", "superficie_cacao_ha"]
for c in keep_csv:
    if c not in dfp.columns:
        logger.error(f"Colonne manquante dans CSV : {c}")
        raise SystemExit(1)
dfp = dfp[keep_csv].copy()
dfp["superficie_cacao_ha"] = dfp["superficie_cacao_ha"].map(to_float)

# GeoJSON nettoyé : colonne surface_calculee_ha créée au script 7
keep_geo = ["Farms_ID", "surface_calculee_ha"]
for c in keep_geo:
    if c not in gdf.columns:
        logger.error(f"Colonne manquante dans GeoJSON : {c}")
        raise SystemExit(1)
gdf = gdf[keep_geo].copy()
gdf["surface_calculee_ha"] = gdf["surface_calculee_ha"].map(to_float)

# ---------------- JOINTURE (parcelle) ---------------- #
# Hypothèse de clé : code_plantation (CSV) <-> Farms_ID (GeoJSON)

logger.info("Jointure parcelle : CSV.code_plantation ↔ GEOJSON.Farms_ID (inner)")
merged = dfp.merge(gdf, left_on="code_plantation", right_on="Farms_ID", how="inner")
logger.info(f"Jointure OK → {merged.shape[0]} lignes correspondantes")

# ---------------- CALCUL ÉCART ---------------- #

# écart relatif (%) = (calc - déclaré) / déclaré * 100
merged["ecart_surface_pct"] = (
    (merged["surface_calculee_ha"] - merged["superficie_cacao_ha"])
    / merged["superficie_cacao_ha"].replace(0, np.nan)
) * 100

# Flag anomalies > ±10 %
merged["anomalie_surface"] = merged["ecart_surface_pct"].abs() > 10

# ---------------- EXPORTS ---------------- #

# Export complet
merged_out = merged[[
    "code_plantation", "code_producteur", "cooperative",
    "superficie_cacao_ha", "surface_calculee_ha", "ecart_surface_pct", "anomalie_surface"
]].copy()

merged_out.to_csv(OUT_ALL, index=False, encoding="utf-8")
logger.info(f"✅ Export comparatif (toutes lignes) → {OUT_ALL}")

# Export anomalies
anom = merged_out[merged_out["anomalie_surface"] == True].copy()
anom.to_csv(OUT_ANOM, index=False, encoding="utf-8")
logger.info(f"⚠️ Export anomalies (écart > ±10 %) → {OUT_ANOM} (n={len(anom)})")

# ---------------- RÉSUMÉ LOG ---------------- #

total = len(merged_out)
nb_anom = len(anom)
pct_anom = (nb_anom / total * 100) if total else 0
logger.info(f"Résumé : {nb_anom}/{total} parcelles en anomalie ({pct_anom:.1f} %)")
logger.info("✔️ Comparaison surfaces par parcelle terminée.")
