import pandas as pd
from pathlib import Path

# === 🔧 Paramètres à personnaliser ===
# Chemin du fichier CSV à convertir
csv_path = Path("outputs/script_5/anomalies_coherence_tabulaire.csv")

# Chemin du fichier Excel de sortie
xlsx_path = Path("outputs/script_5/anomalies_coherence_tabulaire.xlsx")

# === ⚙️ Conversion ===
try:
    print(f"📥 Lecture du fichier CSV : {csv_path.resolve()}")
    df = pd.read_csv(csv_path, sep=None, engine="python")  # détection auto du séparateur

    print(f"💾 Conversion vers Excel : {xlsx_path.resolve()}")
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)  # crée le dossier s'il n'existe pas
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    print("✅ Conversion terminée avec succès !")

except FileNotFoundError:
    print(f"❌ Le fichier source '{csv_path}' est introuvable.")
except Exception as e:
    print(f"⚠️ Une erreur est survenue : {e}")
