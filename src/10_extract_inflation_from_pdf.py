from pathlib import Path
from datetime import datetime
import re
import pandas as pd
import pdfplumber

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

    print("UPSERT OK ->", row["country"], row["date_reference"], row["value"])

def extract_text(pdf_path: Path) -> str:
    chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:4]:  # on limite (souvent la phrase YoY est dans les 1ères pages)
            t = page.extract_text() or ""
            chunks.append(t)
    return "\n".join(chunks)

def parse_number(x: str) -> float:
    return float(x.replace(",", ".").replace("+", "").strip())

# Patterns robustes: "en glissement annuel ... de X%" / "par rapport au même mois ... X%"
PATTERNS = [
    re.compile(r"glissement\s+annuel[^0-9+\-]{0,80}([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
    re.compile(r"variation\s+annuelle[^0-9+\-]{0,80}([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
    re.compile(r"par\s+rapport\s+au\s+m[êe]me\s+mois[^0-9+\-]{0,120}([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%", re.IGNORECASE),
]

# v0.1: on fixe date_reference à partir du fichier choisi.
# (En v0.2, on infère mois/année depuis le PDF)
PDF_MAP = [
    {
        "glob": "SEN_ANSD_IHPC_2024_12_*.pdf",
        "country": "Sénégal",
        "date_reference": "2024-12",
        "source_name": "ANSD",
        "source_url": "https://www.ansd.sn/sites/default/files/2025-01/IHPC_12_2024.pdf",
    },
    {
        "glob": "CIV_ANSTAT_IHPC_2023_10_*.pdf",
        "country": "Côte d’Ivoire",
        "date_reference": "2023-10",
        "source_name": "ANStat",
        "source_url": "https://www.anstat.ci/assets/publications/files/ihpc1023.pdf",
    },
    {
        "glob": "CIV_ANSTAT_IHPC_UEMOA_2025_06_*.pdf",
        "country": "Côte d’Ivoire",
        "date_reference": "2025-06",
        "source_name": "ANStat",
        "source_url": "https://www.anstat.ci/assets/publications/files/File_val_indicateur1752229928.pdf",
    },
]

pdf_dir = Path("data/raw/inflation/pdf")

for meta in PDF_MAP:
    matches = sorted(pdf_dir.glob(meta["glob"]))
    if not matches:
        print("SKIP -> PDF introuvable pour", meta["country"], meta["glob"])
        continue

    pdf_path = matches[-1]
    text = extract_text(pdf_path)

    yoy = None
    used = None
    for p in PATTERNS:
        m = p.search(text)
        if m:
            yoy = parse_number(m.group(1))
            used = p.pattern
            break

    if yoy is None:
        snippet = text[:1200]
        print("FAIL -> YoY non trouvé dans", pdf_path.name)
        print("Extrait utile:\n", snippet)
        continue

    row = {
        "country": meta["country"],
        "indicator": "Inflation IHPC YoY",
        "value": yoy,
        "unit": "%",
        "date_reference": meta["date_reference"],
        "source_name": meta["source_name"],
        "source_url": meta["source_url"],
        "collected_at": datetime.now().isoformat(timespec="seconds"),
        "comment": f"Extraction PDF v0.1 (pattern: {used}) | file: {pdf_path.name}",
    }

    upsert_macro(row)
