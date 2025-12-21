from bs4 import BeautifulSoup
from pathlib import Path
import re
import pandas as pd
from datetime import datetime

RAW_DIR = Path("data/raw/inflation")
OUT_CSV = Path("data/processed/macro_uemoa.csv")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

def upsert_macro(row: dict):
    new_df = pd.DataFrame([row])
    key_cols = ["country", "indicator", "date_reference"]

    if OUT_CSV.exists():
        old_df = pd.read_csv(OUT_CSV)
        old_keys = old_df[key_cols].astype(str).agg("|".join, axis=1)
        new_keys = new_df[key_cols].astype(str).agg("|".join, axis=1)
        merged = pd.concat([old_df[~old_keys.isin(new_keys)], new_df], ignore_index=True)
        merged.to_csv(OUT_CSV, index=False)
    else:
        new_df.to_csv(OUT_CSV, index=False)

    print("UPSERT OK ->", row["country"], row["indicator"], row["date_reference"], row["value"])

def parse_number(x: str) -> float:
    return float(x.replace(",", ".").strip())

# 1) Sénégal (ANSD) : on vise la phrase "Le taux d’inflation s’est établi à +0,8% en 2024"
senegal_file = sorted(RAW_DIR.glob("Senegal_ANSD_IHPC_annual_*.html"))[-1]
senegal_html = senegal_file.read_text(encoding="utf-8", errors="replace")
senegal_text = BeautifulSoup(senegal_html, "html.parser").get_text(" ", strip=True)

m = re.search(r"taux d[’']inflation\s+s[’']est\s+établi\s+à\s*([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%\s+en\s+(20[0-9]{2})", senegal_text, flags=re.IGNORECASE)
if m:
    value = parse_number(m.group(1).replace("+", ""))
    year = m.group(2)
    upsert_macro({
        "country": "Sénégal",
        "indicator": "Inflation IHPC YoY",
        "value": value,
        "unit": "%",
        "date_reference": year,
        "source_name": "ANSD",
        "source_url": "https://www.ansd.sn/Indicateur/evolution-annuelle-de-lindice-harmonise-des-prix-la-consommation",
        "collected_at": datetime.now().isoformat(timespec="seconds"),
        "comment": "Extraction v0.1 depuis page ANSD (évolution annuelle IHPC)",
    })
else:
    print("Sénégal: motif inflation annuelle non trouvé. Il faudra ajuster le pattern.")

# 2) Côte d’Ivoire (ANStat) : sur la home, il y a souvent "hausse de ...% en novembre 2025 par rapport à novembre 2024"
civ_file = sorted(RAW_DIR.glob("CIV_ANStat_home_*.html"))[-1]
civ_html = civ_file.read_text(encoding="utf-8", errors="replace")
civ_text = BeautifulSoup(civ_html, "html.parser").get_text(" ", strip=True)

m2 = re.search(r"IHPC[^.]{0,120}hausse\s+de\s*([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%\s+en\s+([A-Za-zéûôîàç]+)\s+(20[0-9]{2})\s+par\s+rapport\s+à\s+\2\s+(20[0-9]{2})", civ_text, flags=re.IGNORECASE)
if m2:
    value = parse_number(m2.group(1).replace("+", ""))
    month = m2.group(2)
    year = m2.group(3)
    # date_reference au format YYYY-MM (v0.1 : on conserve le mois en texte dans comment)
    upsert_macro({
        "country": "Côte d’Ivoire",
        "indicator": "Inflation IHPC YoY",
        "value": value,
        "unit": "%",
        "date_reference": f"{year}-{month}",
        "source_name": "ANStat",
        "source_url": "https://www.anstat.ci/",
        "collected_at": datetime.now().isoformat(timespec="seconds"),
        "comment": "Extraction v0.1 depuis homepage ANStat (résumé bulletin IHPC). Mois gardé en texte dans date_reference.",
    })
else:
    print("Côte d’Ivoire: motif YoY non trouvé sur la home. On passera par la page Bulletin IHPC dédiée.")
