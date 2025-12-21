from pathlib import Path
from datetime import datetime
import re
import pandas as pd
import pdfplumber

PDF_DIR = Path("data/raw/inflation/pdf")
OUT_CSV = Path("data/processed/macro_uemoa.csv")

# --- Paramètres (CI juin 2025 pour le test) ---
COUNTRY = "Côte d’Ivoire"
DATE_REFERENCE = "2025-06"
SOURCE_NAME = "ANStat"
SOURCE_URL = "https://www.anstat.ci/assets/publications/files/File_val_indicateur1752229928.pdf"
INDICATOR = "Inflation IHPC (glissement annuel)"  # inflation globale YoY

def normalize(text: str) -> str:
    text = text or ""
    return re.sub(r"\s+", " ", text).strip()

def extract_full_text(pdf_path: Path) -> str:
    chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                chunks.append(t)
    return normalize(" ".join(chunks))

def find_ihpc_global_phrase(text: str):
    # Pattern strict: IHPC + verbe + baisse/hausse + %
    pattern = re.compile(
        r"((?:Indice\s+Harmonisé\s+des\s+Prix\s+à\s+la\s+Consommation\s*\(IHPC\)|IHPC)[^.]{0,400}?"
        r"(?:enregistre|affiche|présente|observe|marque|connait|connaît)[^.]{0,250}?"
        r"(?:une\s+)?(baisse|hausse)[^.]{0,250}?"
        r"\(?\s*([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%\s*\)?)",
        re.IGNORECASE
    )

    # Exclusion faible: mots qui signalent sous-indice
    exclude = re.compile(r"\b(énergie|energie|poste\s+énergie|sous[-\s]?indice|division)\b", re.IGNORECASE)

    for m in pattern.finditer(text):
        phrase = m.group(1)
        direction = m.group(2).lower()
        value_raw = m.group(3)

        if exclude.search(phrase):
            continue

        value = float(value_raw.replace(" ", "").replace(",", "."))
        if direction == "baisse" and value > 0:
            value = -value

        return phrase, value, pattern.pattern

    return None, None, None

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
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(OUT_CSV, index=False)

# --- Main ---
pdf_files = sorted(PDF_DIR.glob("CIV_ANSTAT_IHPC_UEMOA_2025_06_*.pdf"))
if not pdf_files:
    raise FileNotFoundError("PDF CI juin 2025 introuvable dans data/raw/inflation/pdf")

pdf_path = pdf_files[-1]
text = extract_full_text(pdf_path)

phrase, value, patt = find_ihpc_global_phrase(text)
if phrase is None:
    raise ValueError("IHPC global non trouvé avec le pattern strict (sans OCR).")

now = datetime.now().isoformat(timespec="seconds")

row = {
    "country": COUNTRY,
    "indicator": INDICATOR,
    "value": value,
    "unit": "%",
    "date_reference": DATE_REFERENCE,
    "source_name": SOURCE_NAME,
    "source_url": SOURCE_URL,
    "collected_at": now,
    "comment": f"Extraction IHPC global (pattern strict). Phrase: {phrase}",
}

upsert_macro(row)

print(f"OK -> {COUNTRY} {DATE_REFERENCE} IHPC global extrait : {value} %")
print("Source PDF:", pdf_path.name)
