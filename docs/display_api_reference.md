GeoDataAnalyst API & Display Data Reference
===========================================

Base URL: `http://localhost:8000`

Overview
--------
The FastAPI service exposes pre-rendered JSON views located in `display-data/`, along with helper endpoints for raw CSV/GeoJSON access. This document lists every endpoint plus the data structure returned by each JSON feed so the dashboard or a report can consume them safely.

Endpoints
---------

### `GET /`
Simple health-check endpoint.
```json
{ "message": "Bienvenue sur l'API GeoDataAnalyst", "version": "1.0.0" }
```

### `GET /status`
Returns the folders tracked by the API and the current manifest of display files. Each entry contains:
```json
{
  "filename": "global_stats.json",
  "key": "global_stats",
  "exists": true,
  "size_bytes": 1824,
  "updated_at": "2025-11-09T19:52:32.536352"
}
```

### `GET /display-files`
Explicit manifest listing; same payload as `status["display_files"]`, wrapped under `{"files": [...]}`.

### `GET /display/by-key/{key}`
Serve a JSON asset via logical key (`global_stats`, `resume_anomalies`, `data_cleaning_audit`, etc.). Responds with the raw JSON structure described in the next section.

### `GET /display/{filename}`
Direct filename access for the same JSON assets (e.g., `/display/global_stats.json`).

### `GET /data/{folder}/{filename}`
Fetch source artifacts as JSON. `folder` must be one of `display`, `clean`, or `outputs`.
- `.csv` files are converted to JSON arrays of records (each row as an object).
- `.geojson` / `.json` files are returned verbatim.

Display JSON Structures
-----------------------
The API serves every file under `display-data/`. Below are the current feeds, their purpose, and schema extracted from the latest run.

### `global_stats.json`
Single object with overall KPIs:
```json
{
  "total_producteurs": 3518,
  "total_plantations": 3540,
  "total_anomalies": 955,
  "taux_anomalies_surface": 37.2,
  "nb_chevauchements": 37,
  "date_generation": "2025-11-09T19:52:32.536352"
}
```

### `resume_anomalies.json`
Array containing a summary snapshot:
```json
[
  {
    "timestamp": "2025-11-09T19:52:31.240276",
    "nb_anomalies_surface": 885,
    "nb_doublons_producteurs": 0,
    "nb_doublons_parcelles": 0,
    "nb_chevauchements": 0
  }
]
```

### `anomalies_surface.json`
Array of parcels with > ±10% surface discrepancy. Fields per row:
```json
{
  "code_plantation": "1030010003-P001",
  "code_producteur": 1030010003,
  "cooperative": "A",
  "superficie_cacao_ha": 3.1,
  "surface_calculee_ha": 2.1294487799,
  "ecart_surface_pct": -31.3081038752,
  "anomalie_surface": true
}
```

### `doublons_producteurs.json`
Array of duplicate producer records detected during cleaning. Presently empty (`[]`) but will contain the raw duplication rows.

### `doublons_parcelles.json`
Array of duplicated parcel geometries. Currently empty (`[]`); structure mirrors the CSV from `outputs/script_7/doublons_parcelles.csv`.

### `chevauchements_parcelles.json`
Array of overlap records produced by script 10 (currently `[]`). Each element matches `outputs/script_10/chevauchements_parcelles.csv`.

### `synthese_coherence_coop.json`
Cooperative-level stats:
```json
{
  "cooperative": "A",
  "nb_producteurs": 3540,
  "couverture_moyenne": 67.1751412429,
  "taux_anomalies_moyen": 25.0,
  "ecart_surface_moyen_ha": 0.9267429379
}
```

### `synthese_coherence_producteurs.json`
Producer-level aggregation (array). Example row:
```json
{
  "code_producteur": "1030010001",
  "nb_plantations_total": 1,
  "superficie_decl_totale": 1.1,
  "nb_jointes": 1.0,
  "superficie_calc_totale": 1.1213382033,
  "nb_anomalies": 0.0,
  "taux_couverture_geo": 100.0,
  "taux_anomalies": 0.0,
  "ecart_surface_total_ha": -0.02,
  "cooperative": "A"
}
```

### `meta_script_9.json`
Metadata emitted by script 9:
```json
{
  "script": "script_9",
  "timestamp": "2025-11-09T19:52:20.788851",
  "nb_producteurs": 3540,
  "nb_cooperatives": 1,
  "taux_anomalies_moyen_global": 25.0
}
```

### `report_metadata.json`
Pipeline execution journal:
```json
{
  "date_execution": "2025-11-09T19:52:32.536730",
  "scripts_executed": ["script_1", "...", "script_10"],
  "folders": { "raw": "data_raw/", "clean": "data_clean/", "outputs": "outputs/", "display": "display-data/" }
}
```

### `data_cleaning_audit.json`
Detailed lineage comparing raw vs. cleaned datasets. Structure:
```json
{
  "timestamp": "2025-11-09T19:52:31.592877",
  "datasets": [
    {
      "dataset": "coop_producteurs",
      "rows_raw": 3518,
      "rows_clean": 3518,
      "rows_removed": 0,
      "percent_retained": 100.0,
      "columns_raw": ["Ordre", "Cooperative *", ...],
      "columns_clean": ["Ordre", "Cooperative", ...],
      "columns_added": [],
      "columns_removed": [],
      "type_changes": { "superficie_cacao_ha": { "from": "object", "to": "float64" } },
      "missing_values_raw": 85,
      "missing_values_clean": 12,
      "missing_reduction": 73,
      "duplicates_removed": 14
    },
    {
      "dataset": "parcelles",
      "rows_raw": 3600,
      "rows_clean": 3590,
      "rows_removed": 10,
      "percent_retained": 99.7,
      "columns_added": ["surface_calculee_ha"],
      "columns_removed": [],
      "crs_raw": "EPSG:4326",
      "crs_clean": "EPSG:4326",
      "invalid_geometries_fixed": 8,
      "missing_reduction": 4
    }
  ]
}
```

Using the Manifest
------------------
- To list available files with metadata: `GET /display-files`.
- To fetch a specific dataset for the dashboard, choose either the logical key (preferred) or the filename:
  - `GET /display/by-key/data_cleaning_audit`
  - `GET /display/data_cleaning_audit.json`
- When new JSON exports are dropped into `display-data/`, they appear automatically in the manifest even without a defined key, ensuring the dashboard can discover them dynamically.
