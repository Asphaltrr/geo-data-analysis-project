#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoDataAnalyst FastAPI server exposing display JSON assets and orchestrating uploads/pipelines.
"""

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="GeoDataAnalyst API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

PIPELINE_SCRIPT = Path("main_pipeline.py")

DISPLAY_ASSETS: Dict[str, Dict[str, str]] = {
    "global_stats": {"folder": "display", "path": "global_stats.json"},
    "anomalies_surface": {"folder": "display", "path": "anomalies_surface.json"},
    "doublons_producteurs": {"folder": "display", "path": "doublons_producteurs.json"},
    "doublons_parcelles": {"folder": "display", "path": "doublons_parcelles.json"},
    "chevauchements_parcelles": {
        "folder": "outputs",
        "path": "script_10/parcelles_chevauchements.geojson",
    },
    "resume_anomalies": {"folder": "display", "path": "resume_anomalies.json"},
    "meta_script_9": {"folder": "display", "path": "meta_script_9.json"},
    "report_metadata": {"folder": "display", "path": "report_metadata.json"},
    "synthese_coherence_coop": {
        "folder": "display",
        "path": "synthese_coherence_coop.json",
    },
    "synthese_coherence_producteurs": {
        "folder": "display",
        "path": "synthese_coherence_producteurs.json",
    },
    "data_cleaning_audit": {"folder": "display", "path": "data_cleaning_audit.json"},
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


def _file_metadata(path: Path, key: str | None = None, *, folder: str | None = None, relative_path: str | None = None) -> Dict[str, object]:
    info: Dict[str, object] = {
        "filename": path.name,
        "key": key,
        "exists": path.exists(),
    }
    if folder:
        info["folder"] = folder
    if relative_path:
        info["relative_path"] = relative_path
    if path.exists():
        stats = path.stat()
        info.update(
            {
                "size_bytes": stats.st_size,
                "updated_at": datetime.fromtimestamp(stats.st_mtime).isoformat(),
            }
        )
    return info


def _asset_entry(key: str, entry: Dict[str, str]) -> Dict[str, object]:
    folder = entry.get("folder", "display")
    base_dir = BASE_PATHS.get(folder)
    if not base_dir:
        raise HTTPException(status_code=500, detail=f"Dossier inconnu pour {key}")
    path = Path(base_dir) / entry["path"]
    return _file_metadata(path, key, folder=folder, relative_path=entry["path"])


def _list_display_manifest() -> List[Dict[str, object]]:
    manifest: List[Dict[str, object]] = []
    known_files: set[str] = set()
    for key, entry in DISPLAY_ASSETS.items():
        manifest.append(_asset_entry(key, entry))
        if entry.get("folder", "display") == "display":
            known_files.add(entry["path"])

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
    entry = DISPLAY_ASSETS.get(key)
    if not entry:
        raise HTTPException(status_code=404, detail="Cle inconnue")
    folder = entry.get("folder", "display")
    base_dir = BASE_PATHS.get(folder)
    if not base_dir:
        raise HTTPException(status_code=500, detail="Dossier non configure")
    path = Path(base_dir) / entry["path"]
    if not path.exists():
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    with path.open(encoding="utf-8") as file_obj:
        data = json.load(file_obj)
    return JSONResponse(content=data)


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


@app.post("/upload")
async def upload_files(
    cooperatives_data: UploadFile = File(...),
    parcelles_geojson: UploadFile = File(...),
):
    """Upload des fichiers bruts (Excel + GeoJSON) dans data_raw/."""
    data_raw_dir = Path(BASE_PATHS["raw"])
    data_raw_dir.mkdir(parents=True, exist_ok=True)
    coop_path = data_raw_dir / "cooperatives_data.xlsx"
    parc_path = data_raw_dir / "parcelles.geojson"

    try:
        with open(coop_path, "wb") as file_obj:
            file_obj.write(await cooperatives_data.read())
        with open(parc_path, "wb") as file_obj:
            file_obj.write(await parcelles_geojson.read())
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Erreur upload: {exc}") from exc

    return JSONResponse(
        {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "message": "Fichiers importes avec succes",
            "files": [str(coop_path), str(parc_path)],
        }
    )


def _pipeline_stream():
    if not PIPELINE_SCRIPT.exists():
        yield "event: error\ndata: main_pipeline.py introuvable\n\n"
        return

    process = subprocess.Popen(
        ["python", str(PIPELINE_SCRIPT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        yield f"data: {line.rstrip()}\n\n"

    process.wait()
    status = "success" if process.returncode == 0 else f"exit={process.returncode}"
    yield f"event: done\ndata: {status}\n\n"


@app.post("/run-pipeline")
def run_pipeline():
    """Lance le pipeline complet et renvoie un flux de logs (Server-Sent Events)."""
    return StreamingResponse(_pipeline_stream(), media_type="text/event-stream")


def _delete_directory_contents(path: Path):
    if not path.exists():
        yield f"data: {path} inexistant\n\n"
        return
    for entry in path.iterdir():
        try:
            if entry.is_dir():
                for nested in _delete_directory_contents(entry):
                    yield nested
                entry.rmdir()
                yield f"data: dossier supprime -> {entry}\n\n"
            else:
                entry.unlink()
                yield f"data: fichier supprime -> {entry}\n\n"
        except Exception as exc:
            yield f"data: erreur suppression {entry}: {exc}\n\n"


def _purge_stream():
    targets = [
        ("outputs", OUTPUTS_DIR),
        ("data_raw", DATA_RAW_DIR),
        ("data_clean", DATA_CLEAN_DIR),
        ("display-data", DISPLAY_DIR),
    ]
    for label, directory in targets:
        yield f"data: Nettoyage de {label} ({directory})\n\n"
        for msg in _delete_directory_contents(directory):
            yield msg
    yield "event: done\ndata: purge terminee\n\n"


@app.post("/purge-data")
def purge_data():
    """Supprime les contenus des repertoires principaux avec un flux de logs SSE."""
    return StreamingResponse(_purge_stream(), media_type="text/event-stream")
