from pathlib import Path
from datetime import datetime
import re
import sys
import pandas as pd
import pdfplumber

OUT_CSV = Path("data/processed/macro_uemoa.csv")
PDF_DIR = Path("data/raw/inflation/pdf")

# --- CONFIG: ajoute des pays ici au fur et à mesure ---
# pdf_glob: comment repérer les PDFs du pays
# source_name/url: pour tracer
TARGETS = [
    {
        "country": "Côte d’Ivoire",
        "pdf_glob": "CIV_ANSTAT_IHPC_UEMOA_*.pdf",
        "indicator": "Inflation IHPC (glissement annuel)",
        "source_name": "ANStat",
        "source_url": "https://www.anstat.ci/",
        # date_reference: essaie d'extraire YYYY-MM depuis le nom de fichier, sinon fallback manuel
        "date_from_filename": True,
    },
    {
        "country": "Sénégal",
        "pdf_glob": "SEN_ANSD_IHPC_*.pdf",
        "indicator": "Inflation IHPC (annuelle)",  # on ajustera plus tard si tu ajoutes un format mensuel YoY
        "source_name": "ANSD",
        "source_url": "https://www.ansd.sn/",
        "date_from_filename": True,
    },
]

# Pattern strict IHPC global
IHPC_PATTERN = re.compile(
    r"((?:Indice\s+Harmonisé\s+des\s+Prix\s+à\s+la\s+Consommation\s*\(IHPC\)|IHPC)[^.]{0,500}?"
    r"(?:enregistre|affiche|présente|observe|marque|connait|connaît|évolue|progresse|recule)[^.]{0,300}?"
    r"(?:une\s+)?(baisse|hausse)[^.]{0,300}?"
    r"\(?\s*([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%\s*\)?)",
    re.IGNORECASE
)

EXCLUDE = re.compile(r"\b(énergie|energie|poste\s+énergie|sous[-\s]?indice|division|transports?|aliments?)\b", re.IGNORECASE)

def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def extract_text(pdf_path: Path) -> str:
    chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                chunks.append(t)
    return normalize(" ".join(chunks))

def parse_date_reference_from_name(name: str):
    # Essaie de détecter YYYY_MM ou YYYY-MM ou YYYYMM
    m = re.search(r"(20\d{2})[_-]?(0[1-9]|1[0-2])", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # Sinon, détecter année seule
    y = re.search(r"(20\d{2})", name)
    if y:
        return y.group(1)
    return None

def load_df():
    if OUT_CSV.exists():
        return pd.read_csv(OUT_CSV)
    return pd.DataFrame()

def upsert(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    new_df = pd.DataFrame([row])
    key_cols = ["country", "indicator", "date_reference"]
    if df.empty:
        return new_df
    old_keys = df[key_cols].astype(str).agg("|".join, axis=1)
    new_keys = new_df[key_cols].astype(str).agg("|".join, axis=1)
    return pd.concat([df[~old_keys.isin(new_keys)], new_df], ignore_index=True)

def save_df(df: pd.DataFrame):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

def extract_value(text: str):
    for m in IHPC_PATTERN.finditer(text):
        phrase = m.group(1)
        if EXCLUDE.search(phrase):
            continue
        direction = m.group(2).lower()
        raw = m.group(3).replace(" ", "").replace(",", ".")
        val = float(raw)
        if direction == "baisse" and val > 0:
            val = -val
        return phrase, val
    return None, None

def main():
    df = load_df()
    now = datetime.now().isoformat(timespec="seconds")

    any_written = False

    for t in TARGETS:
        pdfs = sorted(PDF_DIR.glob(t["pdf_glob"]))
        if not pdfs:
            print(f"NEEDS AUDIT -> {t['country']} | Aucun PDF trouvé ({t['pdf_glob']})")
            continue

        pdf_path = pdfs[-1]
        date_ref = parse_date_reference_from_name(pdf_path.name) if t.get("date_from_filename") else None
        if not date_ref:
            print(f"NEEDS AUDIT -> {t['country']} | date_reference introuvable dans le nom: {pdf_path.name}")
            continue

        text = extract_text(pdf_path)
        phrase, value = extract_value(text)

        if phrase is None:
            print(f"NEEDS AUDIT -> {t['country']} {date_ref} | IHPC global non trouvé (pattern strict). PDF: {pdf_path.name}")
            continue

        row = {
            "country": t["country"],
            "indicator": t["indicator"],
            "value": value,
            "unit": "%",
            "date_reference": date_ref,
            "source_name": t["source_name"],
            "source_url": t["source_url"],
            "collected_at": now,
            "comment": f"Extraction IHPC global (pattern strict). Phrase: {phrase}",
        }

        df = upsert(df, row)
        any_written = True
        print(f"UPSERT OK -> {t['country']} {date_ref} = {value} % | PDF: {pdf_path.name}")

    if any_written:
        save_df(df)
        print("OK -> macro_uemoa.csv mis à jour.")
    else:
        print("Aucune écriture effectuée (tout en audit).")

if __name__ == "__main__":
    sys.exit(main())
