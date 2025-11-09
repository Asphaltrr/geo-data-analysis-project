#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoDataAnalyst FastAPI server exposing display JSON assets and source data.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="GeoDataAnalyst API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_PATHS: Dict[str, str] = {
    "display": "display-data",
    "clean": "data_clean",
    "outputs": "outputs",
    "raw": "data_raw",
}

DISPLAY_FILES: Dict[str, str] = {
    "global_stats": "global_stats.json",
    "anomalies_surface": "anomalies_surface.json",
    "doublons_producteurs": "doublons_producteurs.json",
    "doublons_parcelles": "doublons_parcelles.json",
    "chevauchements_parcelles": "chevauchements_parcelles.json",
    "resume_anomalies": "resume_anomalies.json",
    "meta_script_9": "meta_script_9.json",
    "report_metadata": "report_metadata.json",
    "synthese_coherence_coop": "synthese_coherence_coop.json",
    "synthese_coherence_producteurs": "synthese_coherence_producteurs.json",
    "data_cleaning_audit": "data_cleaning_audit.json",
}

os.makedirs(BASE_PATHS["display"], exist_ok=True)


def _display_path(filename: str) -> Path:
    return Path(BASE_PATHS["display"]) / filename


def _load_display_json(filename: str) -> JSONResponse:
    path = _display_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    with path.open(encoding="utf-8") as file_obj:
        data = json.load(file_obj)
    return JSONResponse(content=data)


def _file_metadata(path: Path, key: str | None = None) -> Dict[str, object]:
    info: Dict[str, object] = {"filename": path.name, "key": key, "exists": path.exists()}
    if path.exists():
        stats = path.stat()
        info.update(
            {
                "size_bytes": stats.st_size,
                "updated_at": datetime.fromtimestamp(stats.st_mtime).isoformat(),
            }
        )
    return info


def _list_display_manifest() -> List[Dict[str, object]]:
    manifest: List[Dict[str, object]] = []
    known_files: set[str] = set()
    for key, filename in DISPLAY_FILES.items():
        path = _display_path(filename)
        manifest.append(_file_metadata(path, key))
        known_files.add(filename)

    # include any extra JSON drops that may not be mapped yet
    display_dir = _display_path("").parent
    for extra_path in sorted(display_dir.glob("*.json"), key=lambda p: p.name):
        if extra_path.name in known_files:
            continue
        manifest.append(_file_metadata(extra_path, None))
    return manifest


@app.get("/")
def home():
    """Simple welcome route for quick health checks."""
    return {"message": "Bienvenue sur l'API GeoDataAnalyst", "version": "1.0.0"}


@app.get("/status")
def get_status():
    """Return folder overview and available display JSON files."""
    return {
        "project": "KOUAME_CHRISTIAN_GeoDataAnalyst",
        "data_folders": BASE_PATHS,
        "display_files": _list_display_manifest(),
    }


@app.get("/display-files")
def list_display_files():
    """List manifest entries for display-data JSON assets."""
    return {"files": _list_display_manifest()}


@app.get("/display/by-key/{key}")
def get_display_by_key(key: str):
    """Serve a JSON asset based on its logical key name."""
    filename = DISPLAY_FILES.get(key)
    if not filename:
        raise HTTPException(status_code=404, detail="Cle inconnue")
    return _load_display_json(filename)


@app.get("/display/{filename}")
def get_display(filename: str):
    """Serve a pre-rendered JSON payload from display-data/."""
    return _load_display_json(filename)


@app.get("/data/{folder}/{filename}")
def get_data(folder: str, filename: str):
    """
    Serve CSV (as JSON records) or GeoJSON from data_clean/ or outputs/.
    Folder must be one of the keys defined in BASE_PATHS.
    """
    folder_path = BASE_PATHS.get(folder)
    if not folder_path:
        raise HTTPException(status_code=400, detail="Dossier invalide")
    path = os.path.join(folder_path, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Fichier introuvable")

    if path.endswith(".csv"):
        df = pd.read_csv(path)
        return json.loads(df.to_json(orient="records"))
    if path.endswith(".geojson") or path.endswith(".json"):
        with open(path, encoding="utf-8") as file_obj:
            return json.load(file_obj)

    raise HTTPException(status_code=415, detail="Format non supporte")
