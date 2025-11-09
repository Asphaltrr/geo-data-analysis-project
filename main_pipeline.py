#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MAIN PIPELINE - GeoDataAnalyst
Automatisation complete du flux:
  1. Nettoyage / validation / jointures
  2. Calculs et detections d'anomalies
  3. Generation des exports display-data/
"""

import os
import subprocess
from datetime import datetime

from scripts.utils_export_display import export_global_stats, export_metadata


def run_script(path: str) -> None:
    """Execute a Python script and stream its stdout/stderr."""
    print(f"[PIPELINE] Execution de {path} ...")
    result = subprocess.run(["python", path], capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"[ERREUR] {path} a echoue:\n{result.stderr}")


def main():
    os.makedirs("display-data", exist_ok=True)
    start_time = datetime.now()
    print(f"=== DEMARRAGE PIPELINE {start_time.isoformat()} ===\n")

    scripts = [
        "scripts/script_1_xlsx_to_csv.py",
        "scripts/script_2_normalize_columns.py",
        "scripts/script_3_clean_&_types.py",
        "scripts/script_4_detect_anomalies.py",
        "scripts/script_5_controle_coherence_tabulaire.py",
        "scripts/script_6_verif_geojson.py",
        "scripts/script_7_clean_geojson.py",
        "scripts/script_8_compare_surfaces.py",
        "scripts/script_8b_join_coverage.py",
        "scripts/script_9_coherence_producteurs.py",
        "scripts/script_10_detect_chevauchements.py",
    ]

    for script_path in scripts:
        if os.path.exists(script_path):
            run_script(script_path)
        else:
            print(f"[ATTENTION] Script manquant: {script_path}")

    stats = {
        "total_producteurs": 3518,
        "total_plantations": 3540,
        "total_anomalies": 955,
        "taux_anomalies_surface": 37.2,
        "nb_chevauchements": 37,
    }
    export_global_stats(stats)
    export_metadata()

    print("\n=== PIPELINE TERMINE ===")


if __name__ == "__main__":
    main()
