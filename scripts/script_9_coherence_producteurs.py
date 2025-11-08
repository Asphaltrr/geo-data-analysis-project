#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 9 — Analyse de cohérence au niveau producteur
Utilise :
 - data_clean/coop_plantations_clean.csv
 - data_clean/parcelles_clean.geojson
 - outputs/surfaces_compare_parcelle.csv (jointure réussie CSV↔GeoJSON)
 - outputs/anomalies_surfaces_parcelle.csv (écarts > ±10%)

Produit :
 - outputs/synthese_coherence_producteurs.csv
 - outputs/synthese_coherence_coop.csv
"""

import os
import pandas as pd
import geopandas as gpd
import logging

# ---------------- CONFIGURATION ----------------

SCRIPT_ID = "script_9"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
log_path = f"{LOG_DIR}/script_9_coherence_producteurs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("coherence_producteurs")

CSV_PLANT = "data_clean/coop_plantations_clean.csv"
GEOJSON = "data_clean/parcelles_clean.geojson"
# Ces fichiers sont produits par le script 8 (comparaison surfaces) → les lire depuis outputs/script_8
COMPARE = "outputs/script_8/surfaces_compare_parcelle.csv"
ANOM = "outputs/script_8/anomalies_surfaces_parcelle.csv"

# ---------------- LECTURES ----------------

logger.info("Lecture des fichiers sources...")
dfp = pd.read_csv(CSV_PLANT, dtype=str)
gdf = gpd.read_file(GEOJSON)
df_compare = pd.read_csv(COMPARE)
df_anom = pd.read_csv(ANOM)

logger.info(f"→ Plantations CSV : {len(dfp)}")
logger.info(f"→ Parcelles GeoJSON : {len(gdf)}")
logger.info(f"→ Lignes jointes : {len(df_compare)}")
logger.info(f"→ Anomalies de surface : {len(df_anom)}")

# ---------------- TRAITEMENT ----------------

# Harmonisation des types pour éviter erreur object/int64
for df in [dfp, df_compare, df_anom]:
    if "code_producteur" in df.columns:
        df["code_producteur"] = df["code_producteur"].astype(str).str.strip()

# Total plantations par producteur
plant_par_prod = dfp.groupby("code_producteur").agg(
    nb_plantations_total=("code_plantation", "count"),
    superficie_decl_totale=("superficie_cacao_ha", lambda x: pd.to_numeric(x, errors="coerce").sum())
).reset_index()

# Total plantations par producteur
plant_par_prod = dfp.groupby("code_producteur").agg(
    nb_plantations_total=("code_plantation", "count"),
    superficie_decl_totale=("superficie_cacao_ha", lambda x: pd.to_numeric(x, errors="coerce").sum())
).reset_index()

# Plantations jointes (présentes dans la comparaison)
jointes_par_prod = df_compare.groupby("code_producteur").agg(
    nb_jointes=("code_plantation", "count"),
    superficie_calc_totale=("surface_calculee_ha", lambda x: pd.to_numeric(x, errors="coerce").sum())
).reset_index()

# Anomalies par producteur
anom_par_prod = df_anom.groupby("code_producteur").agg(
    nb_anomalies=("code_plantation", "count")
).reset_index()

# Jointure des trois
df_agg = plant_par_prod.merge(jointes_par_prod, on="code_producteur", how="left") \
                       .merge(anom_par_prod, on="code_producteur", how="left")

# Calculs dérivés
df_agg["nb_jointes"] = df_agg["nb_jointes"].fillna(0).astype(int)
df_agg["nb_anomalies"] = df_agg["nb_anomalies"].fillna(0).astype(int)
df_agg["taux_couverture_geo"] = (df_agg["nb_jointes"] / df_agg["nb_plantations_total"] * 100).round(2)
df_agg["taux_anomalies"] = (df_agg["nb_anomalies"] / df_agg["nb_jointes"].replace(0, pd.NA) * 100).round(2)
df_agg["ecart_surface_total_ha"] = (
    df_agg["superficie_decl_totale"] - df_agg["superficie_calc_totale"]
).round(2)

# Ajout des infos coop si dispo
if "cooperative" in dfp.columns:
    df_agg = df_agg.merge(dfp[["code_producteur", "cooperative"]].drop_duplicates(),
                          on="code_producteur", how="left")

# ---------------- EXPORT ----------------

df_agg.to_csv(f"{OUT_DIR}/synthese_coherence_producteurs.csv", index=False, encoding="utf-8")
logger.info(f"✅ Export producteur → {OUT_DIR}/synthese_coherence_producteurs.csv")

# Synthèse par coopérative
df_coop = df_agg.groupby("cooperative").agg(
    nb_producteurs=("code_producteur", "count"),
    couverture_moyenne=("taux_couverture_geo", "mean"),
    taux_anomalies_moyen=("taux_anomalies", "mean"),
    ecart_surface_moyen_ha=("ecart_surface_total_ha", "mean")
).reset_index().sort_values("couverture_moyenne", ascending=False)

df_coop.to_csv(f"{OUT_DIR}/synthese_coherence_coop.csv", index=False, encoding="utf-8")
logger.info(f"✅ Export coopérative → {OUT_DIR}/synthese_coherence_coop.csv")

logger.info("✔️ Analyse de cohérence au niveau producteur terminée.")
