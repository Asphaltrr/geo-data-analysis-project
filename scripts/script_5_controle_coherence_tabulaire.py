#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 5 — Contrôle de cohérence tabulaire entre Producteurs et Plantations
---------------------------------------------------------------------------
Objectif :
    Vérifier la cohérence logique entre les fichiers :
        - coop_producteurs_clean.csv
        - coop_plantations_clean.csv

Vérifications effectuées :
    1️⃣ Plantations sans producteur associé
    2️⃣ Producteurs sans plantation associée
    3️⃣ Écart de surfaces totales déclarées vs sommées
    4️⃣ Écart d’estimations totales (kg)
    5️⃣ Incohérence de coopérative (optionnelle)
"""

import os
import pandas as pd
import numpy as np
import logging

# ---------------- CONFIGURATION ---------------- #

SCRIPT_ID = "script_5"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/script_5_coherence.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("coherence")

PATH_PROD = "data_clean/coop_producteurs_clean.csv"
PATH_PLANT = "data_clean/coop_plantations_clean.csv"
OUT_PATH = f"{OUT_DIR}/anomalies_coherence_tabulaire.csv"

# ---------------- LECTURE DES FICHIERS ---------------- #

df_prod = pd.read_csv(PATH_PROD)
df_plant = pd.read_csv(PATH_PLANT)

logger.info(f"📄 Producteurs : {df_prod.shape[0]} lignes, {df_prod.shape[1]} colonnes")
logger.info(f"📄 Plantations : {df_plant.shape[0]} lignes, {df_plant.shape[1]} colonnes")

# ---------------- VÉRIF 1️⃣ : PLANTATIONS SANS PRODUCTEUR ---------------- #

plant_sans_prod = df_plant[~df_plant["code_producteur"].isin(df_prod["code_producteur"])].copy()
plant_sans_prod["identifiant"] = plant_sans_prod["code_plantation"]
plant_sans_prod["type_anomalie"] = "Plantation sans producteur associé"
plant_sans_prod["colonne_concernee"] = "code_producteur"
plant_sans_prod["valeur"] = plant_sans_prod["code_producteur"]

logger.info(f"⚠️ {len(plant_sans_prod)} plantations sans producteur associé")

# ---------------- VÉRIF 2️⃣ : PRODUCTEURS SANS PLANTATION ---------------- #

prod_sans_plant = df_prod[~df_prod["code_producteur"].isin(df_plant["code_producteur"])].copy()
prod_sans_plant["identifiant"] = prod_sans_plant["code_producteur"]
prod_sans_plant["type_anomalie"] = "Producteur sans plantation associée"
prod_sans_plant["colonne_concernee"] = "code_producteur"
prod_sans_plant["valeur"] = prod_sans_plant["code_producteur"]

logger.info(f"⚠️ {len(prod_sans_plant)} producteurs sans plantation associée")

# ---------------- VÉRIF 3️⃣ et 4️⃣ : ÉCARTS SURFACE & ESTIMATION ---------------- #

# Regrouper les plantations par producteur
agg_plant = df_plant.groupby("code_producteur", as_index=False).agg({
    "superficie_cacao_ha": "sum",
    "estimation_kg": "sum",
    "cooperative": "first"
}).rename(columns={
    "superficie_cacao_ha": "superficie_calculee_ha",
    "estimation_kg": "estimation_calculee_kg"
})

# Fusion avec les producteurs
merged = df_prod.merge(agg_plant, on="code_producteur", how="left", suffixes=("_prod", "_plant"))

# Écarts de surface
merged["ecart_surface_pct"] = (
    (merged["superficie_calculee_ha"] - merged["superficie_totale_cacao_ha"]) /
    merged["superficie_totale_cacao_ha"].replace(0, np.nan)
) * 100

# Écarts d’estimation
merged["ecart_estimation_pct"] = (
    (merged["estimation_calculee_kg"] - merged["estimation_totale_kg"]) /
    merged["estimation_totale_kg"].replace(0, np.nan)
) * 100

# Anomalies de surface > 10 %
anom_surface = merged[merged["ecart_surface_pct"].abs() > 10].copy()
anom_surface["identifiant"] = anom_surface["code_producteur"]
anom_surface["type_anomalie"] = "Écart surface > 10 %"
anom_surface["colonne_concernee"] = "superficie_totale_cacao_ha"
anom_surface["valeur"] = anom_surface["ecart_surface_pct"]

logger.info(f"⚠️ {len(anom_surface)} anomalies d’écart de surface > 10 %")

# Anomalies d’estimation > 10 %
anom_estim = merged[merged["ecart_estimation_pct"].abs() > 10].copy()
anom_estim["identifiant"] = anom_estim["code_producteur"]
anom_estim["type_anomalie"] = "Écart estimation > 10 %"
anom_estim["colonne_concernee"] = "estimation_totale_kg"
anom_estim["valeur"] = anom_estim["ecart_estimation_pct"]

logger.info(f"⚠️ {len(anom_estim)} anomalies d’écart d’estimation > 10 %")

# ---------------- VÉRIF 5️⃣ : COHÉRENCE COOPÉRATIVE ---------------- #

mask_coop = merged["cooperative_prod"] != merged["cooperative_plant"]
anom_coop = merged[mask_coop & merged["cooperative_plant"].notna()].copy()
anom_coop["identifiant"] = anom_coop["code_producteur"]
anom_coop["type_anomalie"] = "Incohérence de coopérative entre plantation et producteur"
anom_coop["colonne_concernee"] = "cooperative"
anom_coop["valeur"] = (
    anom_coop["cooperative_prod"].astype(str) + " ≠ " + anom_coop["cooperative_plant"].astype(str)
)

logger.info(f"⚠️ {len(anom_coop)} incohérences de coopérative")

# ---------------- AGGRÉGATION & EXPORT ---------------- #

anomalies_all = pd.concat([
    plant_sans_prod,
    prod_sans_plant,
    anom_surface,
    anom_estim,
    anom_coop
], ignore_index=True)

anomalies_all = anomalies_all[[
    "identifiant", "type_anomalie", "colonne_concernee", "valeur"
]].drop_duplicates()

anomalies_all.to_csv(OUT_PATH, index=False, encoding="utf-8")

logger.info(f"🧾 Rapport exporté → {OUT_PATH}")
logger.info(f"🔢 Total anomalies détectées : {len(anomalies_all)}")
logger.info("✔️ Contrôle de cohérence tabulaire terminé.")
