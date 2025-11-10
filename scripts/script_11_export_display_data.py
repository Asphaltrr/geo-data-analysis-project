#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT 11 -- Export des fichiers analytiques pour le tableau de bord.
Rassemble les anomalies, doublons et chevauchements sous format JSON afin
d'alimenter le dossier display-data/ pour la visualisation et le rapport.

Sources:
 - outputs/script_3/doublons_producteurs.csv
 - outputs/script_7/doublons_parcelles.csv
 - outputs/script_8/anomalies_surfaces_parcelle.csv
 - outputs/script_10/chevauchements_parcelles.csv

Resultats:
 - display-data/anomalies_surface.json
 - display-data/doublons_producteurs.json
 - display-data/doublons_parcelles.json
 - display-data/chevauchements_parcelles.json
 - display-data/resume_anomalies.json
"""

import logging
import os
from datetime import datetime

import geopandas as gpd
import pandas as pd

SCRIPT_ID = "script_11"
LOG_DIR = f"logs/{SCRIPT_ID}"
DISPLAY_DIR = "display-data"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DISPLAY_DIR, exist_ok=True)
log_path = f"{LOG_DIR}/export_display_data.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("export_display")


def safe_load_table(path: str) -> pd.DataFrame:
    """Charge un CSV/GeoJSON s'il existe, sinon renvoie un DataFrame vide."""
    if not os.path.exists(path):
        logger.warning("Fichier manquant: %s", path)
        return pd.DataFrame()

    if path.endswith(".csv"):
        return pd.read_csv(path)

    if path.endswith(".geojson"):
        gdf = gpd.read_file(path)
        dataframe = gdf.copy()
        if "geometry" in dataframe.columns:
            dataframe["geometry_wkt"] = dataframe["geometry"].apply(
                lambda geom: geom.wkt if geom is not None else None
            )
            dataframe = dataframe.drop(columns="geometry")
        return pd.DataFrame(dataframe)

    if path.endswith(".json"):
        return pd.read_json(path)

    logger.warning("Format non supporte: %s", path)
    return pd.DataFrame()


paths = {
    "anomalies_surface": "outputs/script_8/anomalies_surfaces_parcelle.csv",
    "doublons_producteurs": "outputs/script_3/doublons_producteurs.csv",
    "doublons_parcelles": "outputs/script_7/doublons_parcelles.csv",
    "chevauchements_parcelles": "outputs/script_10/parcelles_chevauchements.geojson",
}

dfs = {name: safe_load_table(path) for name, path in paths.items()}

for name, dataframe in dfs.items():
    out_path = os.path.join(DISPLAY_DIR, f"{name}.json")
    dataframe.to_json(out_path, orient="records", indent=2, force_ascii=False)
    logger.info("Export JSON -> %s (%s lignes)", out_path, len(dataframe))


summary = {
    "timestamp": datetime.now().isoformat(),
    "nb_anomalies_surface": len(dfs["anomalies_surface"]),
    "nb_doublons_producteurs": len(dfs["doublons_producteurs"]),
    "nb_doublons_parcelles": len(dfs["doublons_parcelles"]),
    "nb_chevauchements": len(dfs["chevauchements_parcelles"]),
}

resume = pd.DataFrame([summary])
resume_path = os.path.join(DISPLAY_DIR, "resume_anomalies.json")
resume.to_json(resume_path, orient="records", indent=2, force_ascii=False)

logger.info("Resume exporte -> %s", resume_path)
logger.info("Export des donnees analytiques termine avec succes.")
