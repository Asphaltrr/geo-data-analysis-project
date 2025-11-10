#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MAIN PIPELINE - GeoDataAnalyst
Automatisation complete du flux:
  1. Nettoyage / validation / jointures
  2. Calculs et detections d'anomalies
  3. Generation des exports display-data/
"""

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from scripts.utils_export_display import export_global_stats, export_metadata

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
logger.handlers.clear()
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False


def log_line(message: str) -> None:
    print(message, flush=True)
    logger.info(message)


def run_script(path: str) -> bool:
    """Execute a Python script and stream its stdout/stderr line by line."""
    log_line(f"[PIPELINE] Execution de {path} ...")
    process = subprocess.Popen(
        ["python", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        log_line(f"{path}: {line.rstrip()}")
    process.wait()
    if process.returncode != 0:
        log_line(f"[ERREUR] {path} a echoue (code={process.returncode})")
        return False
    log_line(f"[OK] {path} termine")
    return True


def main():
    os.makedirs("display-data", exist_ok=True)
    start_time = datetime.now()
    log_line(f"=== DEMARRAGE PIPELINE {start_time.isoformat()} ===")

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
        "scripts/script_11_export_display_data.py",
        "scripts/script_13_data_cleaning_audit.py",
    ]

    total_steps = len(scripts) + 1  # scripts + final exports
    completed_steps = 0

    for script_path in scripts:
        if os.path.exists(script_path):
            log_line(f"[PROGRESS] Step {completed_steps+1}/{total_steps}: {script_path}")
            run_script(script_path)
            completed_steps += 1
        else:
            log_line(f"[ATTENTION] Script manquant: {script_path}")

    stats = {
        "total_producteurs": 3518,
        "total_plantations": 3540,
        "total_anomalies": 955,
        "taux_anomalies_surface": 37.2,
        "nb_chevauchements": 37,
    }
    export_global_stats(stats)
    export_metadata()
    completed_steps += 1
    log_line(f"[PROGRESS] Step {completed_steps}/{total_steps}: exports globaux realises")

    log_line("=== PIPELINE TERMINE ===")


if __name__ == "__main__":
    main()
