#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, logging, pandas as pd, geopandas as gpd

CSV_PLANT = "data_clean/coop_plantations_clean.csv"
GEOJSON   = "data_clean/parcelles_clean.geojson"
SCRIPT_ID = "script_8b"
LOG_DIR = f"logs/{SCRIPT_ID}"
OUT_DIR   = f"outputs/{SCRIPT_ID}"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(f"{LOG_DIR}/script_8b_join_coverage.log", encoding="utf-8"),
              logging.StreamHandler()]
)
log = logging.getLogger("join_coverage")

dfp = pd.read_csv(CSV_PLANT, dtype=str)
gdf = gpd.read_file(GEOJSON)

# clés
csv_ids = dfp["code_plantation"].dropna().astype(str).str.strip()
geo_ids = gdf["Farms_ID"].dropna().astype(str).str.strip()

# ensembles
csv_set = set(csv_ids)
geo_set = set(geo_ids)

csv_only_ids = sorted(csv_set - geo_set)
geo_only_ids = sorted(geo_set - csv_set)

log.info(f"CSV only: {len(csv_only_ids)} | GEO only: {len(geo_only_ids)} | matched: {len(csv_set & geo_set)}")

# exports détaillés
csv_only = dfp[dfp["code_plantation"].isin(csv_only_ids)].copy()
geo_only = gdf[gdf["Farms_ID"].isin(geo_only_ids)][["Farms_ID","Farmer_ID","Superficie"]].copy()

csv_only.to_csv(f"{OUT_DIR}/csv_only_plantations_sans_geo.csv", index=False, encoding="utf-8")
geo_only.to_csv(f"{OUT_DIR}/geo_only_geo_sans_csv.csv", index=False, encoding="utf-8")

# résumés
def safe_count(df, col):
    return (df[col].fillna("NA").astype(str).str.strip()).value_counts().rename_axis(col).reset_index(name="n")

csv_by_coop = safe_count(csv_only, "cooperative") if "cooperative" in csv_only.columns else pd.DataFrame()
csv_by_prod = safe_count(csv_only, "code_producteur") if "code_producteur" in csv_only.columns else pd.DataFrame()
csv_by_coop.to_csv(f"{OUT_DIR}/csv_only_par_coop.csv", index=False, encoding="utf-8")
csv_by_prod.to_csv(f"{OUT_DIR}/csv_only_par_producteur.csv", index=False, encoding="utf-8")

# heuristique: base_id sans suffixe -P00x pour détecter des désalignements d’écriture
def base_id(s):
    s = str(s)
    return s.split("-P")[0] if "-P" in s else s

csv_only["base_id"] = csv_only["code_plantation"].map(base_id)
gdf_tmp = gdf[["Farms_ID"]].copy()
gdf_tmp["base_id"] = gdf_tmp["Farms_ID"].map(base_id)

# y a-t-il des correspondances au niveau base_id ?
approx_match = csv_only.merge(gdf_tmp.drop_duplicates("base_id"), on="base_id", how="inner", suffixes=("_csv","_geo"))
approx_match.to_csv(f"{OUT_DIR}/csv_only_correspondances_base_id.csv", index=False, encoding="utf-8")

log.info("Exports : "
         "csv_only_plantations_sans_geo.csv, geo_only_geo_sans_csv.csv, "
         "csv_only_par_coop.csv, csv_only_par_producteur.csv, "
         "csv_only_correspondances_base_id.csv")
