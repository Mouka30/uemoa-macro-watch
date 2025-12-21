from pathlib import Path
from datetime import datetime
import pandas as pd

OUT_CSV = Path("data/processed/macro_uemoa.csv")

def load_df():
    if OUT_CSV.exists():
        return pd.read_csv(OUT_CSV)
    return pd.DataFrame()

def save_df(df: pd.DataFrame):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

def upsert(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    new_df = pd.DataFrame([row])
    key_cols = ["country", "indicator", "date_reference"]
    if df.empty:
        return new_df
    old_keys = df[key_cols].astype(str).agg("|".join, axis=1)
    new_keys = new_df[key_cols].astype(str).agg("|".join, axis=1)
    return pd.concat([df[~old_keys.isin(new_keys)], new_df], ignore_index=True)

df = load_df()

# Purge des lignes de test (mauvaises extractions)
if not df.empty and "indicator" in df.columns:
    df = df[df["indicator"].astype(str) != "Inflation IHPC YoY"].copy()

now = datetime.now().isoformat(timespec="seconds")

# Sénégal (officiel, audit OK)
row_sen = {
    "country": "Sénégal",
    "indicator": "Inflation IHPC (annuelle)",
    "value": 0.8,
    "unit": "%",
    "date_reference": "2024",
    "source_name": "ANSD",
    "source_url": "https://www.ansd.sn/sites/default/files/2025-01/IHPC_12_2024.pdf",
    "collected_at": now,
    "comment": "Audit phrase: taux d’inflation annuel en 2024 s’établit à +0,8% (PDF IHPC décembre 2024).",
}
df = upsert(df, row_sen)

# Côte d’Ivoire (officiel, audit manuel)
row_civ = {
    "country": "Côte d’Ivoire",
    "indicator": "Inflation IHPC (glissement annuel)",
    "value": -0.6,
    "unit": "%",
    "date_reference": "2025-06",
    "source_name": "ANStat",
    "source_url": "https://www.anstat.ci/assets/publications/files/File_val_indicateur1752229928.pdf",
    "collected_at": now,
    "comment": "Audit manuel: IHPC global enregistre une baisse (-0,6%) en glissement annuel (juin 2025). Note: -7,1% concerne l'énergie (sous-indice).",
}
df = upsert(df, row_civ)

save_df(df)

print("OK -> macro_uemoa.csv mis à jour (officiel) + purge tests.")
print(df.sort_values(["country", "indicator", "date_reference"]).to_string(index=False))
