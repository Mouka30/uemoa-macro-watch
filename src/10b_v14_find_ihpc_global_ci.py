from pathlib import Path
import re
import pdfplumber

PDF_DIR = Path("data/raw/inflation/pdf")

# Prend le dernier PDF CI juin 2025
pdf_files = sorted(PDF_DIR.glob("CIV_ANSTAT_IHPC_UEMOA_2025_06_*.pdf"))
if not pdf_files:
    raise FileNotFoundError("PDF CI juin 2025 introuvable dans data/raw/inflation/pdf")
PDF_PATH = pdf_files[-1]

def normalize(text: str) -> str:
    text = text or ""
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_full_text(pdf_path: Path) -> str:
    chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:  # toutes les pages
            t = page.extract_text()
            if t:
                chunks.append(t)
    return normalize(" ".join(chunks))

text = extract_full_text(PDF_PATH)

# 1) Pattern "IHPC + verbe + (baisse|hausse) + %", plus strict que "glissement annuel"
# On accepte plusieurs écritures: Indice Harmonisé..., IHPC, etc.
PATTERNS = [
    re.compile(
        r"((?:Indice\s+Harmonisé\s+des\s+Prix\s+à\s+la\s+Consommation\s*\(IHPC\)|IHPC)[^.]{0,300}?"
        r"(?:enregistre|affiche|présente|observe|marque|connait|connaît)[^.]{0,200}?"
        r"(?:une\s+)?(baisse|hausse)[^.]{0,200}?"
        r"([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%)",
        re.IGNORECASE
    ),
    # Variante: "... en glissement annuel" parfois avant la valeur
    re.compile(
        r"((?:Indice\s+Harmonisé\s+des\s+Prix\s+à\s+la\s+Consommation\s*\(IHPC\)|IHPC)[^.]{0,400}?"
        r"glissement\s+annuel[^.]{0,200}?"
        r"(?:une\s+)?(baisse|hausse)[^.]{0,200}?"
        r"([+\-]?\s*[0-9]+(?:[.,][0-9]+)?)\s*%)",
        re.IGNORECASE
    ),
]

# 2) Filtres anti-faux-positifs (énergie, sous-indices)
EXCLUDE = re.compile(r"\b(énergie|energie|poste\s+énergie|divisions?|sous[-\s]?indice|transport|aliments?)\b", re.IGNORECASE)

print("\n=== TEST EXTRACTION IHPC GLOBAL (sans OCR) ===")
print("PDF:", PDF_PATH.name)

found = False
for p in PATTERNS:
    for m in p.finditer(text):
        phrase = m.group(1)
        direction = m.group(2).lower()
        value_raw = m.group(3)

        # Exclure si la phrase contient des mots qui suggèrent un sous-indice
        if EXCLUDE.search(phrase):
            continue

        value = float(value_raw.replace(" ", "").replace(",", "."))
        if direction == "baisse" and value > 0:
            value = -value

        print("\nPHRASE CANDIDATE (IHPC global):")
        print(phrase)
        print("\nVALEUR EXTRAITE:", value, "%")
        found = True
        break
    if found:
        break

if not found:
    print("\n⚠️ Aucun match IHPC global fiable trouvé dans le texte extrait (sans OCR).")
    print("Conclusion: le passage -0,6% est probablement dans une zone non extraite (image/graphique/bloc).")
