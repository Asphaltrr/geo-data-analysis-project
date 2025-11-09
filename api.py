"""
FastAPI server exposing cleaned data and script outputs for the
KOUAME_CHRISTIAN_GeoDataAnalyst project.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs"
DATA_CLEAN_DIR = BASE_DIR / "data_clean"
LOG_FILE = BASE_DIR / "logs" / "api_requests.log"

ALLOWED_CLEAN_FILES = {
    "coop_plantations_clean.csv",
    "coop_producteurs_clean.csv",
    "parcelles_clean.geojson",
}

ALLOWED_OUTPUT_FILES = {
    "script_4/anomalies_plantations.csv",
    "script_4/anomalies_producteurs.csv",
    "script_5/anomalies_coherence_tabulaire.csv",
    "script_6/geojson_verif_anomalies.csv",
    "script_8/anomalies_surfaces_parcelle.csv",
    "script_8/surfaces_compare_parcelle.csv",
}

ALLOWED_OUTPUT_DIRS = {
    "script_8b",
    "script_9",
    "script_10",
}

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
LOG_FILE.touch(exist_ok=True)

logger = logging.getLogger("api.requests")
logger.setLevel(logging.INFO)
if not any(
    isinstance(handler, logging.FileHandler)
    and handler.baseFilename == str(LOG_FILE)
    for handler in logger.handlers
):
    handler = logging.FileHandler(LOG_FILE)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


app = FastAPI(
    title="Geo Data Analyst API",
    description="Exposes cleaned CSV/GeoJSON artifacts for the Next.js dashboard.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "%s %s - %s - %.2fms",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


def _isoformat(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _safe_file(base_dir: Path, relative_path: str) -> Path:
    sanitized = relative_path.strip().lstrip("/")
    target = (base_dir / sanitized).resolve()
    try:
        target.relative_to(base_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=404, detail="File not found")

    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return target


def _directory_status(base_dir: Path) -> Dict[str, str | int | None]:
    if not base_dir.exists():
        return {"root": str(base_dir), "file_count": 0, "last_update": None, "total_size_bytes": 0}

    files: List[Path] = [p for p in base_dir.rglob("*") if p.is_file()]
    if not files:
        return {"root": str(base_dir), "file_count": 0, "last_update": None, "total_size_bytes": 0}

    total_size = sum(p.stat().st_size for p in files)
    last_update = _isoformat(max(p.stat().st_mtime for p in files))
    return {
        "root": str(base_dir),
        "file_count": len(files),
        "last_update": last_update,
        "total_size_bytes": total_size,
    }


def _json_response_for_file(file_path: Path, relative_path: str) -> JSONResponse | FileResponse:
    suffix = file_path.suffix.lower()
    stats = file_path.stat()
    metadata = {
        "path": relative_path,
        "size_bytes": stats.st_size,
        "updated_at": _isoformat(stats.st_mtime),
    }

    if suffix == ".csv":
        dataframe = pd.read_csv(file_path)
        metadata.update(
            {
                "row_count": len(dataframe),
                "columns": list(dataframe.columns),
                "data": dataframe.to_dict(orient="records"),
            }
        )
        return JSONResponse(content=metadata, media_type="application/json")

    if suffix in {".json", ".geojson"}:
        with file_path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
        return JSONResponse(
            content={"meta": metadata, "data": payload},
            media_type="application/json",
        )

    mime_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(file_path, media_type=mime_type or "application/octet-stream")


def _resolve_clean_file(relative_path: str) -> Path:
    sanitized = relative_path.strip().lstrip("/")
    if sanitized not in ALLOWED_CLEAN_FILES:
        raise HTTPException(status_code=404, detail="File not found")
    return _safe_file(DATA_CLEAN_DIR, sanitized)


def _resolve_output_file(relative_path: str) -> Path:
    sanitized = relative_path.strip().lstrip("/")
    path_inside_outputs = Path(sanitized)
    if sanitized in ALLOWED_OUTPUT_FILES:
        return _safe_file(OUTPUTS_DIR, sanitized)
    if path_inside_outputs.parts and path_inside_outputs.parts[0] in ALLOWED_OUTPUT_DIRS:
        return _safe_file(OUTPUTS_DIR, sanitized)
    raise HTTPException(status_code=404, detail="File not found")


@app.get("/data_clean/{relative_path:path}")
def serve_clean_dataset(relative_path: str):
    """Expose the curated data_clean files exactly as requested."""
    file_path = _resolve_clean_file(relative_path)
    return _json_response_for_file(
        file_path,
        f"data_clean/{file_path.relative_to(DATA_CLEAN_DIR)}",
    )


@app.get("/outputs/{relative_path:path}")
def serve_outputs_artifact(relative_path: str):
    """
    Serve only the whitelisted outputs files/directories.
    Example: /outputs/script_8b/csv_only_correspondances_base_id.csv
    """
    file_path = _resolve_output_file(relative_path)
    return _json_response_for_file(
        file_path,
        f"outputs/{file_path.relative_to(OUTPUTS_DIR)}",
    )


@app.get("/status")
def get_status():
    """Provide basic freshness stats for outputs/ and data_clean/ trees."""
    return {
        "outputs": _directory_status(OUTPUTS_DIR),
        "data_clean": _directory_status(DATA_CLEAN_DIR),
    }
