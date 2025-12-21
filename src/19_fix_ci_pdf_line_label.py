from pathlib import Path
import pandas as pd

CSV_PATH = Path("data/processed/macro_uemoa.csv")

df = pd.read_csv(CSV_PATH)

mask = (
    (df["country"].astype(str) == "Côte d’Ivoire")
    & (df["indicator"].astype(str) == "Inflation IHPC (glissement annuel)")
    & (df["date_reference"].astype(str) == "2025-06")
    & (df["source_name"].astype(str) == "ANStat")
)

if mask.any():
    df.loc[mask, "indicator"] = "IHPC (bulletin, glissement annuel – texte)"
    df.loc[mask, "comment"] = df.loc[mask, "comment"].astype(str) + " | Relabelled to avoid mixing with ANStat series."
    df.to_csv(CSV_PATH, index=False)
    print("OK -> Ligne PDF CI relabelled (anti-confusion).")
else:
    print("INFO -> Ligne cible non trouvée, aucune modification.")

print(df[df["country"].astype(str) == "Côte d’Ivoire"].sort_values(["indicator","date_reference"]).to_string(index=False))
