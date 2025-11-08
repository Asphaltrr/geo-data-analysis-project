#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 4 — Détection d’anomalies internes (individuelles)
---------------------------------------------------------
Analyse séparée de :
 - coop_producteurs_clean.csv
 - coop_plantations_clean.csv

Détection :
 - Doublons sur identifiants
 - Valeurs aberrantes par seuils métier
 - Sauvegarde des rapports individuels
 - Journalisation détaillée (log)
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# ---------------- CONFIG ---------------- #

SCRIPT_ID = "script_4"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

log_path = f"{LOG_DIR}/script_4_outliers_individuels.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("anomalies")

# --- Fichiers d'entrée --- #
PROD_PATH = "data_clean/coop_producteurs_clean.csv"
PLANT_PATH = "data_clean/coop_plantations_clean.csv"

# --- Fichiers de sortie --- #
OUT_PROD = f"{OUT_DIR}/anomalies_producteurs.csv"
OUT_PLANT = f"{OUT_DIR}/anomalies_plantations.csv"

# --- Seuils métier --- #
SEUILS = {
    "producteurs": {
        "annee_naissance": (1930, 2005),
        "superficie_totale_exploitation_ha": (0.1, 100),
        "superficie_totale_cacao_ha": (0.1, 50),
        "nb_plantations_cacao": (1, 20),
        "estimation_totale_kg": (50, 20000),
        "taille_menage": (1, 20)
    },
    "plantations": {
        "superficie_cacao_ha": (0.1, 50),
        "rendement_kg_ha": (100, 2000),
        "estimation_kg": (10, 20000),
        "latitude": (5, 10),
        "longitude": (-9, -2)
    }
}

# ---------------- OUTLIER DETECTION ---------------- #

def detect_outliers(df, seuils: dict, id_col: str):
    """Retourne un DF listant toutes les anomalies internes (valeurs hors bornes)."""
    anomalies = []
    for col, (min_v, max_v) in seuils.items():
        if col not in df.columns:
            logger.warning(f"Colonne manquante : {col}")
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        mask = series.notna() & ((series < min_v) | (series > max_v))
        if mask.any():
            tmp = df.loc[mask, [id_col, col]].copy()
            tmp["type_anomalie"] = f"{col} hors bornes [{min_v}; {max_v}]"
            tmp["valeur"] = series[mask]
            tmp = tmp.rename(columns={id_col: "identifiant", col: "colonne_concernee"})
            anomalies.append(tmp)
            logger.warning(f"⚠️ {mask.sum()} anomalies sur {col}")
    if not anomalies:
        return pd.DataFrame(columns=["identifiant", "colonne_concernee", "type_anomalie", "valeur"])
    return pd.concat(anomalies, ignore_index=True)

# ---------------- DUPLICATE DETECTION ---------------- #

def detect_duplicates(df, id_col: str, dataset_name: str):
    """Détecte les doublons d'identifiants et exporte un rapport dédié + log détaillé."""
    dupli = df[df.duplicated(subset=[id_col], keep=False)].copy()

    if dupli.empty:
        logger.info(f"✅ Aucun doublon détecté dans {dataset_name}.")
        return pd.DataFrame(columns=["identifiant", "colonne_concernee", "type_anomalie", "valeur"])

    # Ajouter les colonnes de description
    dupli["identifiant"] = dupli[id_col]
    dupli["colonne_concernee"] = id_col
    dupli["type_anomalie"] = f"Doublon sur {dataset_name}"
    dupli["valeur"] = dupli[id_col]

    # Export CSV
    dupli_out_path = f"{OUT_DIR}/doublons_{dataset_name.lower()}.csv"
    dupli.to_csv(dupli_out_path, index=False, encoding="utf-8")

    # Log synthétique
    logger.warning(f"⚠️ {len(dupli)} doublons détectés dans {dataset_name} → {dupli_out_path}")

    # Log détaillé : chaque identifiant doublon
    logger.info("🔍 Liste des doublons détectés :")
    for ident in dupli[id_col].unique():
        count = (dupli[id_col] == ident).sum()
        logger.info(f"   - {dataset_name} | {id_col} = {ident} (x{count})")

    # Écrire dans un log fichier dédié pour traçabilité pure (facultatif)
    log_txt_path = f"{LOG_DIR}/doublons_{dataset_name.lower()}.log"
    with open(log_txt_path, "w", encoding="utf-8") as f:
        f.write(f"=== DOUBLONS DANS {dataset_name.upper()} ===\n")
        f.write(f"Date : {pd.Timestamp.now()}\n\n")
        for ident in dupli[id_col].unique():
            count = (dupli[id_col] == ident).sum()
            f.write(f"{ident} — {count} occurrences\n")
        f.write("\n=== FIN DU RAPPORT ===\n")

    logger.info(f"📝 Détails des doublons consignés dans {log_txt_path}")

    return dupli[["identifiant", "colonne_concernee", "type_anomalie", "valeur"]]

# ---------------- MAIN FUNCTION ---------------- #

def analyse_fichier(path, seuils, id_col, dataset_name, out_path):
    logger.info(f"Analyse du fichier {dataset_name} → {path}")
    df = pd.read_csv(path)

    anomalies = []

    # 1️⃣ Doublons
    df_dupli = detect_duplicates(df, id_col, dataset_name)
    anomalies.append(df_dupli)

    # 2️⃣ Outliers
    df_outliers = detect_outliers(df, seuils, id_col)
    anomalies.append(df_outliers)

    # 3️⃣ Agrégation et suppression doublons de signalement
    anomalies_df = pd.concat(anomalies, ignore_index=True).drop_duplicates()

    # 4️⃣ Export du rapport global (doublons + outliers)
    anomalies_df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"✅ Rapport exporté : {out_path}")
    logger.info(f"🔢 Total anomalies {dataset_name} : {len(anomalies_df)}\n")

    return anomalies_df


if __name__ == "__main__":
    logger.info("=== DÉBUT DU SCRIPT 4 — Détection des anomalies internes ===\n")

    anom_prod = analyse_fichier(PROD_PATH, SEUILS["producteurs"], "code_producteur", "Producteurs", OUT_PROD)
    anom_plant = analyse_fichier(PLANT_PATH, SEUILS["plantations"], "code_plantation", "Plantations", OUT_PLANT)

    logger.info("✔️ Détection des anomalies et doublons terminée.")
    logger.info(f"🕒 Fin d’exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
