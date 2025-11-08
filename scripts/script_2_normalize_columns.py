#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SCRIPT 2
L'idée est de renommer les colonnes des fichiers coop_producteurs.csv et coop_plantations.csv
avec des noms normalisés en français, pour uniformiser le jeu de données.
"""

import os
import logging
import pandas as pd
from datetime import datetime

# ----------------------------- CONFIGURATION -----------------------------

MAPPING_PRODUCTEURS = {
    "Ordre": "numero_ordre",
    "Cooperative *": "cooperative",
    "Codes de certification": "codes_certification",
    "Code Producteur *": "code_producteur",
    "Autres Codes Disponibles": "autres_codes",
    "Année de Naissance *": "annee_naissance",
    "Date d'adhésion 4 (mois-année)": "date_adhesion",
    "Genre (F ou H) *": "genre",
    "Superficie Totale de L'Exploitation (HA) *": "superficie_totale_exploitation_ha",
    "Superficie Cacao Totale (Ha) *": "superficie_totale_cacao_ha",
    "Nombre de Plantations Cacao *": "nb_plantations_cacao",
    "Estimation totale (Kg) *": "estimation_totale_kg",
    "Ville": "ville",
    "Nom du Coach": "nom_coach",
    "Numéro de Matricule": "numero_matricule",
    "Type de Matricule (National, CCC…)": "type_matricule",
    "Taille du Menage (Nombre de personnes dans le menage du producteur) *": "taille_menage",
    "Recolte Totale 24-25 (en KG) *": "recolte_24_25_kg",
    "Volume livré à la Co-op 24-25 (en KG) *": "volume_livre_coop_24_25_kg",
    "Recolte Total 23-24 (en KG) *": "recolte_23_24_kg",
    "Volume livré à la Co-op 23-24 (en KG) *": "volume_livre_coop_23_24_kg",
}

MAPPING_PLANTATIONS = {
    "N°Ordre": "numero_ordre",
    "Cooperative/Groupe *": "cooperative",
    "Code plantation *": "code_plantation",
    "Code Producteur *": "code_producteur",
    "Autres Codes Disponibles": "autres_codes",
    "Superficie Cacao (Ha) *": "superficie_cacao_ha",
    "Rendement (kg/Ha) *": "rendement_kg_ha",
    "Estimation (Kg) *": "estimation_kg",
    "Varieté Cacao": "variete_cacao",
    "Gérant (Oui/Non/Inconnu)": "gerant",
    "Noms et Prénoms du Gérant (Si , gerant existe )": "nom_gerant",
    "Latitude *": "latitude",
    "Longitude *": "longitude",
    "Données Polygonales (Si possible, sinon a partager)": "donnees_polygonales",
}

# ----------------------------- LOGGING SETUP -----------------------------

SCRIPT_ID = "script_2"
LOG_DIR = f"logs/{SCRIPT_ID}"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = f"{LOG_DIR}/script_2_logs.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rename_columns")

# ----------------------------- FUNCTIONS -----------------------------

def rename_columns(input_path: str, output_path: str, mapping: dict):
    logger.info(f"Lecture du fichier : {input_path}")
    df = pd.read_csv(input_path, dtype=str, encoding="utf-8")

    logger.info(f"Colonnes originales : {list(df.columns)}")

    new_cols = []
    for col in df.columns:
        new_col = mapping.get(col.strip(), col.strip())
        if new_col != col:
            logger.info(f"→ '{col}' renommé en '{new_col}'")
        else:
            logger.warning(f"⚠️ Pas de correspondance trouvée pour : {col}")
        new_cols.append(new_col)

    df.columns = new_cols

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"✅ Fichier sauvegardé → {output_path}\n")

# ----------------------------- MAIN EXECUTION -----------------------------

if __name__ == "__main__":
    rename_columns("data_raw/coop_producteurs.csv", "data_clean/coop_producteurs_cols_normalized.csv", MAPPING_PRODUCTEURS)
    rename_columns("data_raw/coop_plantations.csv", "data_clean/coop_plantations_cols_normalized.csv", MAPPING_PLANTATIONS)
    logger.info("✔️ Normalisation terminée avec succès.")
