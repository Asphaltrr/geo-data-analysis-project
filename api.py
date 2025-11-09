#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoDataAnalyst FastAPI server exposing display JSON assets and source data.
"""

import json
import os
from typing import Dict

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
}

os.makedirs(BASE_PATHS["display"], exist_ok=True)


@app.get("/")
def home():
    """Simple welcome route for quick health checks."""
    return {"message": "Bienvenue sur l'API GeoDataAnalyst", "version": "1.0.0"}


@app.get("/status")
def get_status():
    """Return folder overview and available display JSON files."""
    display_files = sorted(os.listdir(BASE_PATHS["display"]))
    return {
        "project": "KOUAME_CHRISTIAN_GeoDataAnalyst",
        "data_folders": BASE_PATHS,
        "display_files": display_files,
    }


@app.get("/display/{filename}")
def get_display(filename: str):
    """Serve a pre-rendered JSON payload from display-data/."""
    path = os.path.join(BASE_PATHS["display"], filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    with open(path, encoding="utf-8") as file_obj:
        data = json.load(file_obj)
    return JSONResponse(content=data)


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
