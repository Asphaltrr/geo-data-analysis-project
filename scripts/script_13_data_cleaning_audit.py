#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT 13 -- Rapport d'audit detaille du nettoyage de donnees.
Compare les fichiers bruts (data_raw/) et nettoyes (data_clean/) pour:
 - coop_producteurs
 - coop_plantations
 - parcelles (GeoJSON)

Produit:
 - display-data/data_cleaning_audit.json
"""

import json
import logging
import os
from datetime import datetime

import geopandas as gpd
import pandas as pd

SCRIPT_ID = "script_13"
LOG_DIR = f"logs/{SCRIPT_ID}"
DISPLAY_DIR = "display-data"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DISPLAY_DIR, exist_ok=True)
log_path = f"{LOG_DIR}/data_cleaning_audit.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("data_audit")

RAW_DIR = "data_raw"
CLEAN_DIR = "data_clean"

FILES = {
    "coop_producteurs": ("coop_producteurs.csv", "coop_producteurs_clean.csv"),
    "coop_plantations": ("coop_plantations.csv", "coop_plantations_clean.csv"),
    "parcelles": ("parcelles.geojson", "parcelles_clean.geojson"),
}


def compare_csv(raw_path: str, clean_path: str, name: str) -> dict:
    """Compare deux CSV tabulaires."""
    df_raw = pd.read_csv(raw_path)
    df_clean = pd.read_csv(clean_path)

    summary = {
        "dataset": name,
        "rows_raw": len(df_raw),
        "rows_clean": len(df_clean),
        "rows_removed": max(len(df_raw) - len(df_clean), 0),
        "percent_retained": round((len(df_clean) / len(df_raw)) * 100, 2)
        if len(df_raw) > 0
        else None,
        "columns_raw": list(df_raw.columns),
        "columns_clean": list(df_clean.columns),
    }

    summary["columns_added"] = [col for col in df_clean.columns if col not in df_raw.columns]
    summary["columns_removed"] = [col for col in df_raw.columns if col not in df_clean.columns]

    type_changes = {}
    for col in df_clean.columns:
        if col in df_raw.columns:
            t_raw, t_clean = str(df_raw[col].dtype), str(df_clean[col].dtype)
            if t_raw != t_clean:
                type_changes[col] = {"from": t_raw, "to": t_clean}
    summary["type_changes"] = type_changes

    na_raw = int(df_raw.isna().sum().sum())
    na_clean = int(df_clean.isna().sum().sum())
    summary["missing_values_raw"] = na_raw
    summary["missing_values_clean"] = na_clean
    summary["missing_reduction"] = na_raw - na_clean

    if "code_producteur" in df_raw.columns:
        dupli_raw = int(df_raw.duplicated(subset="code_producteur").sum())
        dupli_clean = int(df_clean.duplicated(subset="code_producteur").sum())
        summary["duplicates_removed"] = dupli_raw - dupli_clean

    logger.info("Compare: %s (%s -> %s lignes)", name, summary["rows_raw"], summary["rows_clean"])
    return summary


def compare_geojson(raw_path: str, clean_path: str, name: str) -> dict:
    """Compare deux fichiers GeoJSON geospatiaux."""
    gdf_raw = gpd.read_file(raw_path)
    gdf_clean = gpd.read_file(clean_path)

    summary = {
        "dataset": name,
        "rows_raw": len(gdf_raw),
        "rows_clean": len(gdf_clean),
        "rows_removed": max(len(gdf_raw) - len(gdf_clean), 0),
        "percent_retained": round((len(gdf_clean) / len(gdf_raw)) * 100, 2)
        if len(gdf_raw) > 0
        else None,
        "columns_raw": list(gdf_raw.columns),
        "columns_clean": list(gdf_clean.columns),
        "crs_raw": str(gdf_raw.crs),
        "crs_clean": str(gdf_clean.crs),
    }

    invalid_raw = int((~gdf_raw.is_valid).sum())
    invalid_clean = int((~gdf_clean.is_valid).sum())
    summary["invalid_geometries_fixed"] = invalid_raw - invalid_clean

    summary["columns_added"] = [col for col in gdf_clean.columns if col not in gdf_raw.columns]
    summary["columns_removed"] = [col for col in gdf_raw.columns if col not in gdf_clean.columns]

    na_raw = int(gdf_raw.drop(columns="geometry").isna().sum().sum())
    na_clean = int(gdf_clean.drop(columns="geometry").isna().sum().sum())
    summary["missing_reduction"] = na_raw - na_clean

    logger.info("Compare: %s (%s -> %s polygones)", name, summary["rows_raw"], summary["rows_clean"])
    return summary


report = {"timestamp": datetime.now().isoformat(), "datasets": []}

for name, (raw_file, clean_file) in FILES.items():
    raw_path = os.path.join(RAW_DIR, raw_file)
    clean_path = os.path.join(CLEAN_DIR, clean_file)

    if raw_file.endswith(".geojson"):
        result = compare_geojson(raw_path, clean_path, name)
    else:
        result = compare_csv(raw_path, clean_path, name)

    report["datasets"].append(result)

out_path = os.path.join(DISPLAY_DIR, "data_cleaning_audit.json")
with open(out_path, "w", encoding="utf-8") as file_obj:
    json.dump(report, file_obj, ensure_ascii=False, indent=2)

logger.info("Rapport d'audit exporte -> %s", out_path)
logger.info("Script termine avec succes.")
