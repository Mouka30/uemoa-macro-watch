from bs4 import BeautifulSoup
import re
from pathlib import Path
import pandas as pd
from datetime import datetime

HTML_FILE = sorted(Path("data/raw").glob("bceao_cpm_20251203_*.html"))[-1]

with open(HTML_FILE, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f.read(), "html.parser")

text = soup.get_text(" ", strip=True)

def to_float(x: str) -> float:
    return float(x.replace(",", ".").strip())

patterns = [
    re.compile(r"principal\s+taux\s+directeur[^0-9]{0,80}([0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
    re.compile(r"maintenir\s+à\s*([0-9]+(?:[.,][0-9]+)?)\s*%[^.]{0,120}principal\s+taux\s+directeur", re.IGNORECASE),
    re.compile(r"taux\s+minimum\s+de\s+soumission[^0-9]{0,80}([0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
]

policy_rate = None
matched_pattern = None
for p in patterns:
    m = p.search(text)
    if m:
        policy_rate = to_float(m.group(1))
        matched_pattern = p.pattern
        break

if policy_rate is None:
    idx = text.lower().find("taux")
    snippet = text[max(0, idx-250): idx+400] if idx != -1 else text[:600]
    raise ValueError("Taux directeur non trouvé. Extrait utile:\n" + snippet)

row = {
    "country": "UEMOA",
    "indicator": "Taux directeur BCEAO (principal)",
    "value": policy_rate,
    "unit": "%",
    "date_reference": "2025-12-03",
    "source_name": "BCEAO",
    "source_url": "https://www.bceao.int/fr/communique-presse/reunion-ordinaire-du-comite-de-politique-monetaire-de-la-bceao-tenue-le-3",
    "collected_at": datetime.now().isoformat(timespec="seconds"),
    "comment": f"CPM BCEAO (pattern: {matched_pattern})",
}

out_csv = Path("data/processed") / "macro_uemoa.csv"
out_csv.parent.mkdir(parents=True, exist_ok=True)

new_df = pd.DataFrame([row])

# Upsert: clé = (country, indicator, date_reference)
key_cols = ["country", "indicator", "date_reference"]

if out_csv.exists():
    old_df = pd.read_csv(out_csv)
    # On enlève toute ligne existante avec la même clé, puis on ajoute la nouvelle
    merged = pd.concat(
        [old_df[~old_df[key_cols].astype(str).agg("|".join, axis=1).isin(
            new_df[key_cols].astype(str).agg("|".join, axis=1)
        )], new_df],
        ignore_index=True
    )
    merged.to_csv(out_csv, index=False)
else:
    new_df.to_csv(out_csv, index=False)

print("OK -> Taux directeur (principal) enregistré :", policy_rate, "%")
print(new_df.to_string(index=False))
