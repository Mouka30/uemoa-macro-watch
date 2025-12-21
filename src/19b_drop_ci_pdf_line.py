from pathlib import Path
import pandas as pd

CSV_PATH = Path("data/processed/macro_uemoa.csv")

df = pd.read_csv(CSV_PATH)

before = len(df)
df = df[~(
    (df["country"].astype(str) == "Côte d’Ivoire")
    & (df["indicator"].astype(str) == "Inflation IHPC (glissement annuel)")
    & (df["date_reference"].astype(str) == "2025-06")
)].copy()

after = len(df)
df.to_csv(CSV_PATH, index=False)

print(f"OK -> Deleted {before-after} row(s).")
print(df[df["country"].astype(str) == "Côte d’Ivoire"].sort_values(["indicator","date_reference"]).to_string(index=False))
