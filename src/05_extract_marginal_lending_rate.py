from bs4 import BeautifulSoup
import re
from pathlib import Path
import pandas as pd
from datetime import datetime

# Prend le dernier communiqué CPM téléchargé
HTML_FILE = sorted(Path("data/raw").glob("bceao_cpm_20251203_*.html"))[-1]

with open(HTML_FILE, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f.read(), "html.parser")

text = soup.get_text(" ", strip=True)

def to_float(x: str) -> float:
    return float(x.replace(",", ".").strip())

# Extraction du taux du guichet de prêt marginal
patterns = [
    re.compile(r"guichet\s+de\s+pr[eê]t\s+marginal[^0-9]{0,80}([0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
    # Variante fréquente : "taux du guichet de prêt marginal ... à 5,25%"
    re.compile(r"taux\s+du\s+guichet\s+de\s+pr[eê]t\s+marginal[^0-9]{0,120}([0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
    # Variante inversée : "maintenir à 5,25% le taux du guichet de prêt marginal"
    re.compile(r"maintenir\s+à\s*([0-9]+(?:[.,][0-9]+)?)\s*%[^.]{0,140}guichet\s+de\s+pr[eê]t\s+marginal", re.IGNORECASE),
]

marginal_rate = None
matched_pattern = None

for p in patterns:
    m = p.search(text)
    if m:
        marginal_rate = to_float(m.group(1))
        matched_pattern = p.pattern
        break

if marginal_rate is None:
    idx = text.lower().find("marginal")
    snippet = text[max(0, idx-250): idx+500] if idx != -1 else text[:800]
    raise ValueError("Taux du guichet de prêt marginal non trouvé. Extrait utile:\n" + snippet)

row = {
    "country": "UEMOA",
    "indicator": "Taux BCEAO (guichet de prêt marginal)",
    "value": marginal_rate,
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

# Upsert pour éviter les doublons : clé = (country, indicator, date_reference)
key_cols = ["country", "indicator", "date_reference"]

if out_csv.exists():
    old_df = pd.read_csv(out_csv)
    old_keys = old_df[key_cols].astype(str).agg("|".join, axis=1)
    new_keys = new_df[key_cols].astype(str).agg("|".join, axis=1)

    merged = pd.concat([old_df[~old_keys.isin(new_keys)], new_df], ignore_index=True)
    merged.to_csv(out_csv, index=False)
else:
    new_df.to_csv(out_csv, index=False)

print("OK -> Taux prêt marginal enregistré :", marginal_rate, "%")
print(new_df.to_string(index=False))
