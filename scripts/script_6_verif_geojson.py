#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 6 — Vérification du GeoJSON (sans modification)
------------------------------------------------------
Fichier d'entrée : data_raw/parcelles.geojson

Contrôles effectués :
  1) Présence et validité du CRS (cible : EPSG:4326 / CRS84)
  2) Types de géométries (Polygon / MultiPolygon uniquement)
  3) Géométries invalides / vides
  4) Enveloppe (bbox) dans les bornes attendues pour la Côte d'Ivoire
     (lat ~ [4, 11], lon ~ [-9, -2]) — bornes larges pour tolérance
  5) Doublons d'attributs (Farms_ID)
  6) Doublons géométriques exacts (hash WKB)
  7) Colonnes attendues (Farms_ID, Farmer_ID, Superficie) — signalement si manquantes

Sorties :
  - logs/script_6_verif_geojson.log               (journal détaillé)
  - outputs/geojson_verif_anomalies.csv           (liste des anomalies par entité)
  - outputs/geojson_verif_resume.csv              (tableau de synthèse des contrôles)
"""

import os
import logging
import pandas as pd
import geopandas as gpd
import numpy as np

from shapely.geometry import Polygon, MultiPolygon

# ------------------------- CONFIG -------------------------

INPUT = "data_raw/parcelles.geojson"
SCRIPT_ID = "script_6"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"
LOG_PATH = f"{LOG_DIR}/script_6_verif_geojson.log"
OUT_ANOM = f"{OUT_DIR}/geojson_verif_anomalies.csv"
OUT_RESUME = f"{OUT_DIR}/geojson_verif_resume.csv"

# bornes larges pour CI
LAT_MIN, LAT_MAX = 4.0, 11.0
LON_MIN, LON_MAX = -9.5, -2.0

COL_ID_PREF = "Farms_ID"     # id de parcelle attendu
COLS_REQUISES = ["Farms_ID", "Farmer_ID", "Superficie", "geometry"]

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("verif_geojson")

# ------------------------- UTILS -------------------------

def id_or_index(row):
    """retourne l'id préféré s'il existe, sinon l'index sous forme 'idx_<n>'"""
    val = row.get(COL_ID_PREF, None)
    if pd.isna(val) or val is None:
        return f"idx_{row.name}"
    return str(val)

def resume_to_df(items_dict):
    df = pd.DataFrame(
        [{"controle": k, "valeur": v} for k, v in items_dict.items()]
    )
    return df[["controle", "valeur"]]

# ------------------------- MAIN -------------------------

def main():
    logger.info(f"Lecture du GeoJSON : {INPUT}")
    try:
        gdf = gpd.read_file(INPUT)
    except Exception as e:
        logger.error(f"❌ Impossible de lire le fichier : {e}")
        raise SystemExit(1)

    logger.info(f"Nombre d'entités : {len(gdf)}")
    logger.info(f"Colonnes : {list(gdf.columns)}")

    # 0) Colonnes requises
    manquantes = [c for c in COLS_REQUISES if c not in gdf.columns]
    if manquantes:
        logger.warning(f"⚠️ Colonnes manquantes : {manquantes}")
    else:
        logger.info("✅ Colonnes requises présentes.")

    # 1) CRS
    crs_txt = str(gdf.crs) if gdf.crs is not None else None
    if gdf.crs is None:
        crs_status = "CRS manquant"
        logger.warning("⚠️ CRS manquant (attendu EPSG:4326 / CRS84).")
    else:
        crs_status = crs_txt
        logger.info(f"✅ CRS détecté : {crs_txt}")

    # 2) Types géométriques
    geom_types = gdf.geometry.geom_type.fillna("None").value_counts().to_dict()
    bad_types = [t for t in geom_types.keys() if t not in {"Polygon", "MultiPolygon"}]
    if bad_types:
        logger.warning(f"⚠️ Types géométriques non attendus : {bad_types}")
    else:
        logger.info("✅ Types géométriques conformes (Polygon/MultiPolygon).")

    # 3) Invalidités / vides
    invalid_mask = ~gdf.is_valid
    empty_mask = gdf.geometry.is_empty | gdf.geometry.isna()
    nb_invalid = int(invalid_mask.sum())
    nb_empty = int(empty_mask.sum())
    if nb_invalid > 0:
        logger.warning(f"⚠️ Géométries invalides : {nb_invalid}")
    else:
        logger.info("✅ Aucune géométrie invalide.")
    if nb_empty > 0:
        logger.warning(f"⚠️ Géométries vides : {nb_empty}")
    else:
        logger.info("✅ Aucune géométrie vide.")

    # 4) Bornes géographiques (en EPSG:4326)
    # si CRS manquant mais 'CRS84' dans metadata d'origine, GeoPandas le traite généralement comme 4326
    try:
        gdf_ll = gdf.to_crs(4326) if gdf.crs is not None else gdf  # si None, on suppose dégénéré en degrés
    except Exception as e:
        logger.warning(f"⚠️ Impossible de reprojeter en EPSG:4326 pour contrôle des bornes : {e}")
        gdf_ll = gdf

    # centroïdes pour test d'emprise simple
    with np.errstate(all='ignore'):
        cent = gdf_ll.geometry.centroid
    lon_ok = cent.x.between(LON_MIN, LON_MAX, inclusive="both")
    lat_ok = cent.y.between(LAT_MIN, LAT_MAX, inclusive="both")
    bbox_bad_mask = ~(lon_ok & lat_ok)
    nb_bbox_bad = int(bbox_bad_mask.sum())
    if nb_bbox_bad > 0:
        logger.warning(f"⚠️ Entités hors emprise attendue (centroïde) : {nb_bbox_bad}")
    else:
        logger.info("✅ Tous les centroïdes dans les bornes attendues (CI).")

    # 5) Doublons d'attribut (Farms_ID)
    if COL_ID_PREF in gdf.columns:
        dupli_attr_mask = gdf.duplicated(subset=[COL_ID_PREF], keep=False)
        nb_dupli_attr = int(dupli_attr_mask.sum())
        if nb_dupli_attr > 0:
            logger.warning(f"⚠️ Doublons d'attribut {COL_ID_PREF} : {nb_dupli_attr}")
        else:
            logger.info("✅ Aucun doublon d'attribut sur Farms_ID.")
    else:
        dupli_attr_mask = pd.Series([False]*len(gdf), index=gdf.index)
        nb_dupli_attr = 0

    # 6) Doublons géométriques exacts (hash WKB)
    try:
        geom_hash = gdf.geometry.apply(lambda g: None if g is None else g.wkb_hex)
        dupli_geom_mask = geom_hash.duplicated(keep=False) & geom_hash.notna()
        nb_dupli_geom = int(dupli_geom_mask.sum())
    except Exception as e:
        logger.warning(f"⚠️ Erreur calcul hash géométrique : {e}")
        dupli_geom_mask = pd.Series([False]*len(gdf), index=gdf.index)
        nb_dupli_geom = 0

    if nb_dupli_geom > 0:
        logger.warning(f"⚠️ Doublons géométriques exacts : {nb_dupli_geom}")
    else:
        logger.info("✅ Aucun doublon géométrique exact détecté.")

    # ---------------- LIGNE PAR LIGNE : ANOMALIES ----------------
    anomalies = []

    def push_anom(idx, typ, col, val):
        rid = id_or_index(gdf.loc[idx])
        anomalies.append({"identifiant": rid, "type_anomalie": typ, "colonne_concernee": col, "valeur": val})

    # invalid/empty
    for idx in gdf.index[invalid_mask]:
        push_anom(idx, "Geometrie invalide", "geometry", "invalid")
    for idx in gdf.index[empty_mask]:
        push_anom(idx, "Geometrie vide", "geometry", "empty")

    # bbox out
    for idx in gdf.index[bbox_bad_mask]:
        geom = gdf_ll.geometry.iloc[idx]
        if geom.is_empty:
            push_anom(idx, "Centroide non calculable (geometry vide)", "centroid", "")
            continue
        c = geom.centroid
        push_anom(idx, "Centroide hors bornes CI", "centroid", f"{c.y:.6f},{c.x:.6f}")


    # attributs manquants
    for c in COLS_REQUISES:
        if c not in gdf.columns:
            anomalies.append({"identifiant": "GLOBAL", "type_anomalie": "Colonne manquante", "colonne_concernee": c, "valeur": ""})

    # doublons attributaires
    if COL_ID_PREF in gdf.columns and nb_dupli_attr > 0:
        for rid, cnt in gdf[COL_ID_PREF].value_counts().items():
            if cnt > 1:
                anomalies.append({"identifiant": str(rid), "type_anomalie": "Doublon attributaire Farms_ID", "colonne_concernee": COL_ID_PREF, "valeur": cnt})

    # doublons géométriques exacts
    if nb_dupli_geom > 0:
        grouped = pd.DataFrame({"hash": geom_hash}).groupby("hash").size().sort_values(ascending=False)
        for h, cnt in grouped.items():
            if pd.isna(h) or cnt <= 1:
                continue
            anomalies.append({"identifiant": f"hash:{h[:16]}", "type_anomalie": "Doublon geometrique exact", "colonne_concernee": "geometry", "valeur": cnt})

    # ---------------- RÉSUMÉ & EXPORT ----------------
    resume = {
        "nb_entites": len(gdf),
        "crs": crs_status,
        "geom_types": geom_types,
        "nb_invalid": nb_invalid,
        "nb_empty": nb_empty,
        "nb_centroid_hors_bornes": nb_bbox_bad,
        "nb_doublons_attribut_Farms_ID": nb_dupli_attr,
        "nb_doublons_geom_exacts": nb_dupli_geom
    }

    # anomalies
    anom_df = pd.DataFrame(anomalies, columns=["identifiant", "type_anomalie", "colonne_concernee", "valeur"]).drop_duplicates()
    anom_df.to_csv(OUT_ANOM, index=False, encoding="utf-8")
    logger.info(f"🧾 Anomalies exportées → {OUT_ANOM} (lignes : {len(anom_df)})")

    # résumé
    # pour les types de géométries (dict), on sérialise en texte
    res_df = resume_to_df({
        "nb_entites": resume["nb_entites"],
        "crs": resume["crs"],
        "geom_types": str(resume["geom_types"]),
        "nb_invalid": resume["nb_invalid"],
        "nb_empty": resume["nb_empty"],
        "nb_centroid_hors_bornes": resume["nb_centroid_hors_bornes"],
        "nb_doublons_attribut_Farms_ID": resume["nb_doublons_attribut_Farms_ID"],
        "nb_doublons_geom_exacts": resume["nb_doublons_geom_exacts"]
    })
    res_df.to_csv(OUT_RESUME, index=False, encoding="utf-8")
    logger.info(f"📊 Résumé exporté → {OUT_RESUME}")

    logger.info("✔️ Vérification GeoJSON terminée (aucune correction appliquée).")

if __name__ == "__main__":
    main()
