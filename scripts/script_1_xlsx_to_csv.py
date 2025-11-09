import os
import os
import pandas as pd
import re
from datetime import datetime

"""
SCRIPT 1
Conversion des fichiers xlsx en csv.
"""

coop_data = "data_raw/cooperatives_data.xlsx"
SCRIPT_ID = "script_1"
LOG_DIR = f"logs/{SCRIPT_ID}"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = f"{LOG_DIR}/xlsx_to_csv_log.txt"

with open(log_file, "w", encoding="utf-8") as log:
    log.write("=== LOG DE CONVERSION EXCEL → CSV ===\n")
    log.write(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log.write(f"Fichier source : {coop_data}\n\n")

    sheets = pd.read_excel(coop_data, sheet_name=None)

    os.makedirs("data_raw", exist_ok=True)
    # Mapping heuristics: write common sheets to canonical filenames to avoid root-level files
    sheet_map = {
        "producteur": "coop_producteurs.csv",
        "producteurs": "coop_producteurs.csv",
        "producteur.s": "coop_producteurs.csv",
        "plantation": "coop_plantations.csv",
        "plantations": "coop_plantations.csv",
        "registre plantations": "coop_plantations.csv",
    }

    def map_sheet_name(name: str) -> str:
        n = name.lower()
        for k, v in sheet_map.items():
            if k in n:
                return v
        # fallback: keep original sheet name as filename
        safe = name.replace(" ", "_").replace("/", "_")
        return f"{safe}.csv"

    for sheet_name, df in sheets.items():
        csv_name = map_sheet_name(sheet_name)
        out_path = os.path.join("data_raw", csv_name)

        # --- Automatic header sanitization ---
        # Replace any CR/LF in column names with a single space, collapse
        # multiple whitespace and strip edges to ensure the CSV header is
        # written on a single physical line (no embedded newlines).
        try:
            orig_cols = list(df.columns)
            new_cols = [re.sub(r"[\r\n]+", " ", str(c)) for c in orig_cols]
            new_cols = [re.sub(r"\s+", " ", c).strip() for c in new_cols]
            # Apply if any change
            if orig_cols != new_cols:
                log.write(f" - Colonnes sanitisees pour la feuille '{sheet_name}':\n")
                for o, n in zip(orig_cols, new_cols):
                    if o != n:
                        log.write(f"    '{o}' -> '{n}'\n")
            df.columns = new_cols
        except Exception as e:
            log.write(f" - WARNING: Impossible de sanitizer les colonnes: {e}\n")

        df.to_csv(out_path, index=False, encoding="utf-8")

        # verification roundtrip
        df_check = pd.read_csv(out_path)

        rows_before, cols_before = df.shape
        rows_after, cols_after = df_check.shape

        ok = (rows_before == rows_after) and (cols_before == cols_after)
        status = "✅ OK" if ok else "⚠️ MISMATCH"

        log.write(f"Feuille : {sheet_name}\n")
        log.write(f" - CSV : {csv_name}\n")
        log.write(f" - Lignes (Excel → CSV) : {rows_before} → {rows_after}\n")
        log.write(f" - Colonnes (Excel → CSV) : {cols_before} → {cols_after}\n")
        log.write(f" - Statut : {status}\n\n")

        print(f"{status} - Feuille '{sheet_name}' convertie en '{csv_name}'")

    log.write("=== FIN DU TRAITEMENT ===\n")