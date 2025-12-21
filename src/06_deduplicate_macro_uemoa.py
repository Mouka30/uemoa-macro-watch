import pandas as pd
from pathlib import Path

# Chemin vers le fichier macro
PROJECT_ROOT = Path(__file__).resolve().parents[1]
csv_path = PROJECT_ROOT / "data" / "processed" / "macro_uemoa.csv"

if not csv_path.exists():
    raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

df = pd.read_csv(csv_path)

key_cols = ["country", "indicator", "date_reference"]

# Trier pour garder la version la plus récente
if "collected_at" in df.columns:
    df = df.sort_values("collected_at")

before = len(df)

df = df.drop_duplicates(subset=key_cols, keep="last")

after = len(df)

df.to_csv(csv_path, index=False)

print("OK -> macro_uemoa.csv dédoublonné")
print(f"Lignes avant : {before}")
print(f"Lignes après : {after}")
