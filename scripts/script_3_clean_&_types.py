#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 3 (fix)
Nettoyage des valeurs et typage :
 - data_clean/coop_plantations_cols_normalized.csv
 - data_clean/coop_producteurs_cols_normalized.csv

Améliorations :
 - "Non" N'EST PAS traité comme NA
 - Séparation ENTIER / REEL avec cast explicite
 - Logs si une colonne ENTIER contient des décimales
 - Pas de cast numérique sur 'donnees_polygonales'
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import csv

# ------------------- Configuration -------------------

# ⚠️ "Non" retiré
VALS_NA = {
    "", " ", "na", "n/a", "nan", "none", "null",
    "Non Disponible", "NON DISPONIBLE", "Non disponible", "NULL", "NA", "N/A"
}

# --- Producteurs ---
INT_PRODUCTEURS = [
    "numero_ordre",
    "annee_naissance",
    "nb_plantations_cacao",
    "taille_menage",
    "estimation_totale_kg",
    "recolte_24_25_kg", "volume_livre_coop_24_25_kg",
    "recolte_23_24_kg", "volume_livre_coop_23_24_kg",
]
FLOAT_PRODUCTEURS = [
    "superficie_totale_exploitation_ha",
    "superficie_totale_cacao_ha",
]

# --- Plantations ---
INT_PLANTATIONS = [
    "numero_ordre",
    "estimation_kg",
]
FLOAT_PLANTATIONS = [
    "superficie_cacao_ha",
    "rendement_kg_ha",
    "latitude", "longitude",
]
# 'donnees_polygonales' doit rester texte → pas de cast

# ------------------- Logging -------------------

SCRIPT_ID = "script_3"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR = f"outputs/{SCRIPT_ID}"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
log_path = f"{LOG_DIR}/script_3_logs.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("clean_types")

# ------------------- Utils -------------------

def std_na(x):
    """Uniformise les NA (sans toucher aux 'Non', 'Inconnu', etc.)."""
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    return np.nan if s.lower() in {v.lower() for v in VALS_NA} else s

def to_float_smart(x):
    """Convertit en float, gère virgule et séparateur d'espace; sinon NaN."""
    if pd.isna(x):
        return np.nan
    s = str(x).strip().replace(",", ".").replace(" ", "")
    if s.lower() in {v.lower() for v in VALS_NA}:
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan

def cast_float_cols(df: pd.DataFrame, cols: list):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(to_float_smart).astype("float64")
            logger.info(f"→ Colonne réelle (float64) : {c}")
        else:
            logger.warning(f"⚠️ Colonne manquante (float attendue) : {c}")

def cast_int_cols_safe(df: pd.DataFrame, cols: list, epsilon: float = 1e-9):
    """
    Convertit en Int64 si et seulement si toutes les valeurs non-nulles sont entières (|x - round(x)| < epsilon).
    Sinon : laisse la colonne en float64 et LOG un avertissement.
    """
    for c in cols:
        if c not in df.columns:
            logger.warning(f"⚠️ Colonne manquante (int attendue) : {c}")
            continue

        # Convertir d'abord en float pour inspecter
        series_float = df[c].map(to_float_smart)

        # Détecter les valeurs non entières
        mask = series_float.notna()
        frac_mask = (np.abs(series_float[mask] - np.round(series_float[mask])) > epsilon)

        if mask.sum() > 0 and frac_mask.any():
            n_bad = int(frac_mask.sum())
            logger.warning(
                f"⚠️ {c}: {n_bad} valeur(s) avec décimales détectées dans une colonne attendue entière. "
                f"Conservation en float64 (aucune troncature automatique)."
            )
            df[c] = series_float.astype("float64")
        else:
            # tout est entier → cast propre vers Int64 (nullable)
            df[c] = pd.Series(series_float.round().where(series_float.notna(), np.nan), dtype="Int64")
            logger.info(f"→ Colonne entière (Int64) : {c}")

def percent_na(df: pd.DataFrame) -> pd.DataFrame:
    out = df.isna().mean().sort_values(ascending=False).to_frame("pourcentage_na")
    out["pourcentage_na"] = (out["pourcentage_na"] * 100).round(2)
    return out

# ------------------- Traitement principal -------------------

def clean_and_type(input_path, output_path, int_cols, float_cols, id_cols_text=None):
    logger.info(f"Lecture : {input_path}")
    df = pd.read_csv(input_path, dtype=str, encoding="utf-8")

    # 1) NA standard
    df = df.applymap(std_na)
    logger.info("→ Normalisation des valeurs manquantes : OK")

    # 2) Trims pour quelques IDs/textes clés
    if id_cols_text:
        for c in id_cols_text:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip()
                logger.info(f"→ Trim texte : {c}")

    # 3) Cast réels puis entiers (ordre important)
    cast_float_cols(df, float_cols)
    cast_int_cols_safe(df, int_cols)

    # 4) Résumé des types
    for k, v in df.dtypes.to_dict().items():
        logger.info(f"Type final - {k}: {v}")

    # 5) Diagnostic NA
    diag = percent_na(df)
    base = os.path.splitext(os.path.basename(output_path))[0]
    diag_path = f"{OUT_DIR}/na_{base}.csv"
    diag.to_csv(diag_path, encoding="utf-8")
    logger.info(f"🧪 Diagnostic NA → {diag_path}")

    # 6) Uniformiser les NaN → cellules vides
    # Uniformiser les NaN → cellules vides
    nb_nan = int(df.isna().sum().sum())
    logger.info(f"🔄 Normalisation des valeurs manquantes : {nb_nan} cellules NaN remplacées par vide")

    # Nettoyer les chaînes 'nan' explicites (en texte)
    df = df.replace("nan", None).replace("NaN", None).replace("NAN", None)

    # Remplacer les NaN numpy par None (pour éviter 'nan' en texte)
    df = df.where(pd.notnull(df), None)

    # Sauvegarde propre
    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
        na_rep="",               # cellules vides
        quoting=csv.QUOTE_MINIMAL
    )

# ------------------- Exécution -------------------

if __name__ == "__main__":
    # Producteurs
    clean_and_type(
        input_path="data_clean/coop_producteurs_cols_normalized.csv",
        output_path="data_clean/coop_producteurs_clean.csv",
        int_cols=INT_PRODUCTEURS,
        float_cols=FLOAT_PRODUCTEURS,
        id_cols_text=["cooperative", "code_producteur", "ville", "nom_coach", "type_matricule", "genre"]
    )

    # Plantations
    clean_and_type(
        input_path="data_clean/coop_plantations_cols_normalized.csv",
        output_path="data_clean/coop_plantations_clean.csv",
        int_cols=INT_PLANTATIONS,
        float_cols=FLOAT_PLANTATIONS,
        id_cols_text=["cooperative", "code_plantation", "code_producteur", "variete_cacao", "gerant", "donnees_polygonales"]
        # 'donnees_polygonales' laissé en texte
    )

    logger.info("✔️ Nettoyage et typage terminés sans suffixe .0 pour les entiers.")
