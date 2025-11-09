#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UTILS_EXPORT_DISPLAY -- Generation des fichiers agreges pour le dashboard Next.js.
Ce module centralise les exports simplifies depuis outputs/ vers display-data/.
Il ajoute une etape automatique d'aggregation JSON sans modifier les scripts existants.
"""

import json
import os
from datetime import datetime

import pandas as pd

DISPLAY_DIR = "display-data"
os.makedirs(DISPLAY_DIR, exist_ok=True)


def export_global_stats(stats_dict):
    """Export des indicateurs cles sous display-data/global_stats.json."""
    stats_dict["date_generation"] = datetime.now().isoformat()
    path = os.path.join(DISPLAY_DIR, "global_stats.json")
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(stats_dict, file_obj, indent=2, ensure_ascii=False)
    print(f"[EXPORT] {path}")


def export_anomalies_tabulaire(df_anomalies):
    """Agrege les anomalies tabulaires par type."""
    df_summary = (
        df_anomalies.groupby("type_anomalie")
        .size()
        .reset_index(name="nb")
        .sort_values("nb", ascending=False)
    )
    path = os.path.join(DISPLAY_DIR, "anomalies_tabulaire.json")
    df_summary.to_json(path, orient="records", indent=2, force_ascii=False)
    print(f"[EXPORT] {path}")


def export_surfaces_distribution(df_surfaces):
    """Histogramme des ecarts de surface groupes par tranches de 10%."""
    bins = [-100, -50, -10, 10, 50, 100]
    labels = ["-100% a -50%", "-50% a -10%", "-10% a +10%", "+10% a +50%", "+50% a +100%"]
    df_surfaces = df_surfaces.copy()
    df_surfaces["classe"] = pd.cut(df_surfaces["ecart_surface_pct"], bins=bins, labels=labels)
    dist = df_surfaces["classe"].value_counts().reset_index()
    dist.columns = ["classe", "count"]
    path = os.path.join(DISPLAY_DIR, "surfaces_distribution.json")
    dist.to_json(path, orient="records", indent=2, force_ascii=False)
    print(f"[EXPORT] {path}")


def export_top_producteurs_anomalies(df_synthese_prod):
    """Top 10 producteurs avec le plus d'anomalies."""
    top10 = (
        df_synthese_prod[["code_producteur", "nb_anomalies", "cooperative"]]
        .sort_values("nb_anomalies", ascending=False)
        .head(10)
    )
    path = os.path.join(DISPLAY_DIR, "top_producteurs_anomalies.json")
    top10.to_json(path, orient="records", indent=2, force_ascii=False)
    print(f"[EXPORT] {path}")


def export_anomalies_par_coop(df_synthese_coop):
    """Taux moyen d'anomalies par cooperative."""
    df = df_synthese_coop[["cooperative", "nb_producteurs", "taux_anomalies_moyen"]]
    path = os.path.join(DISPLAY_DIR, "anomalies_par_coop.json")
    df.to_json(path, orient="records", indent=2, force_ascii=False)
    print(f"[EXPORT] {path}")


def export_chevauchements(df_chev):
    """Nombre de chevauchements par id_1."""
    df_summary = (
        df_chev.groupby("id_1")
        .size()
        .reset_index(name="nb_chevauchements")
        .sort_values("nb_chevauchements", ascending=False)
    )
    path = os.path.join(DISPLAY_DIR, "chevauchements_summary.json")
    df_summary.to_json(path, orient="records", indent=2, force_ascii=False)
    print(f"[EXPORT] {path}")


def export_metadata():
    """Resume global de generation."""
    metadata = {
        "date_execution": datetime.now().isoformat(),
        "scripts_executed": ["script_1",
            "script_2",
            "script_3",
            "script_4",
            "script_5",
            "script_6",
            "script_7",
            "script_8",
            "script_9",
            "script_10",
        ],
        "folders": {
            "raw": "data_raw/",
            "clean": "data_clean/",
            "outputs": "outputs/",
            "display": "display-data/",
        },
    }
    path = os.path.join(DISPLAY_DIR, "report_metadata.json")
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(metadata, file_obj, indent=2, ensure_ascii=False)
    print(f"[EXPORT] {path}")
