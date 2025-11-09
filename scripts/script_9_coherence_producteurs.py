#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 9 — Analyse de cohérence au niveau producteur
----------------------------------------------------
Utilise :
 - data_clean/coop_plantations_clean.csv
 - data_clean/parcelles_clean.geojson
 - outputs/script_8/surfaces_compare_parcelle.csv
 - outputs/script_8/anomalies_surfaces_parcelle.csv

Produit :
 - outputs/script_9/synthese_coherence_producteurs.csv
 - outputs/script_9/synthese_coherence_coop.csv
 - display-data/synthese_coherence_producteurs.json
 - display-data/synthese_coherence_coop.json
"""

import os
import pandas as pd
import geopandas as gpd
import logging
import json
from datetime import datetime

# ---------------- CONFIGURATION ---------------- #

SCRIPT_ID = "script_9"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"
DISPLAY_DIR = "display-data"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(DISPLAY_DIR, exist_ok=True)

log_path = f"{LOG_DIR}/script_9_coherence_producteurs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("coherence_producteurs")

# ---------------- PATHS ---------------- #

CSV_PLANT = "data_clean/coop_plantations_clean.csv"
GEOJSON = "data_clean/parcelles_clean.geojson"
COMPARE = "outputs/script_8/surfaces_compare_parcelle.csv"
ANOM = "outputs/script_8/anomalies_surfaces_parcelle.csv"

# ---------------- LECTURES ---------------- #

logger.info("Lecture des fichiers sources...")
dfp = pd.read_csv(CSV_PLANT, dtype=str)
gdf = gpd.read_file(GEOJSON)
df_compare = pd.read_csv(COMPARE)
df_anom = pd.read_csv(ANOM)

logger.info(f"→ Plantations CSV : {len(dfp)}")
logger.info(f"→ Parcelles GeoJSON : {len(gdf)}")
logger.info(f"→ Lignes jointes : {len(df_compare)}")
logger.info(f"→ Anomalies de surface : {len(df_anom)}")

# ---------------- TRAITEMENT ---------------- #

# Harmonisation
for df in [dfp, df_compare, df_anom]:
    if "code_producteur" in df.columns:
        df["code_producteur"] = df["code_producteur"].astype(str).str.strip()

# Agrégations
plant_par_prod = dfp.groupby("code_producteur").agg(
    nb_plantations_total=("code_plantation", "count"),
    superficie_decl_totale=("superficie_cacao_ha", lambda x: pd.to_numeric(x, errors="coerce").sum())
).reset_index()

jointes_par_prod = df_compare.groupby("code_producteur").agg(
    nb_jointes=("code_plantation", "count"),
    superficie_calc_totale=("surface_calculee_ha", lambda x: pd.to_numeric(x, errors="coerce").sum())
).reset_index()

anom_par_prod = df_anom.groupby("code_producteur").agg(
    nb_anomalies=("code_plantation", "count")
).reset_index()

# Fusion
df_agg = (
    plant_par_prod
    .merge(jointes_par_prod, on="code_producteur", how="left")
    .merge(anom_par_prod, on="code_producteur", how="left")
)

# Conversion numérique
for col in [
    "nb_plantations_total", "nb_jointes", "nb_anomalies",
    "superficie_decl_totale", "superficie_calc_totale"
]:
    df_agg[col] = pd.to_numeric(df_agg[col], errors="coerce").fillna(0)

# Fonctions robustes
def safe_divide(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    result = (a / b.replace(0, pd.NA)) * 100
    result = result.replace([float("inf"), -float("inf")], pd.NA)
    return result.fillna(0).astype(float).round(2)

# Calculs
df_agg["taux_couverture_geo"] = safe_divide(df_agg["nb_jointes"], df_agg["nb_plantations_total"])
df_agg["taux_anomalies"] = safe_divide(df_agg["nb_anomalies"], df_agg["nb_jointes"])
df_agg["ecart_surface_total_ha"] = (
    df_agg["superficie_decl_totale"] - df_agg["superficie_calc_totale"]
).fillna(0).astype(float).round(2)

# Coopératives
if "cooperative" in dfp.columns:
    df_agg = df_agg.merge(
        dfp[["code_producteur", "cooperative"]].drop_duplicates(),
        on="code_producteur",
        how="left"
    )

# ---------------- EXPORT CSV ---------------- #

csv_prod = f"{OUT_DIR}/synthese_coherence_producteurs.csv"
csv_coop = f"{OUT_DIR}/synthese_coherence_coop.csv"

df_agg.to_csv(csv_prod, index=False, encoding="utf-8")
logger.info(f"✅ Export producteurs → {csv_prod}")

# --- Vérification existence colonne avant agrégation ---
if "ecart_surface_total_ha" not in df_agg.columns:
    logger.warning("⚠️ Colonne 'ecart_surface_total_ha' manquante, ajoutée à 0.")
    df_agg["ecart_surface_total_ha"] = 0.0

# Synthèse par coopérative
df_coop = (
    df_agg.groupby("cooperative", dropna=False)
    .agg(
        nb_producteurs=("code_producteur", "count"),
        couverture_moyenne=("taux_couverture_geo", "mean"),
        taux_anomalies_moyen=("taux_anomalies", "mean"),
        ecart_surface_moyen_ha=("ecart_surface_total_ha", "mean"),
    )
    .reset_index()
    .sort_values("couverture_moyenne", ascending=False)
)

df_coop.to_csv(csv_coop, index=False, encoding="utf-8")
logger.info(f"✅ Export coopératives → {csv_coop}")

# ---------------- EXPORT JSON ---------------- #

display_prod = os.path.join(DISPLAY_DIR, "synthese_coherence_producteurs.json")
display_coop = os.path.join(DISPLAY_DIR, "synthese_coherence_coop.json")

df_agg.to_json(display_prod, orient="records", indent=2, force_ascii=False)
df_coop.to_json(display_coop, orient="records", indent=2, force_ascii=False)

logger.info(f"📊 Export JSON producteurs → {display_prod}")
logger.info(f"📊 Export JSON coopératives → {display_coop}")

# ---------------- META ---------------- #

summary = {
    "script": SCRIPT_ID,
    "timestamp": datetime.now().isoformat(),
    "nb_producteurs": len(df_agg),
    "nb_cooperatives": df_coop.shape[0],
    "taux_anomalies_moyen_global": round(df_coop["taux_anomalies_moyen"].mean(), 2)
}

meta_path = os.path.join(DISPLAY_DIR, "meta_script_9.json")
with open(meta_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

logger.info(f"🧾 Métadonnées exportées → {meta_path}")
logger.info("✔️ Analyse de cohérence au niveau producteur terminée avec succès.")
