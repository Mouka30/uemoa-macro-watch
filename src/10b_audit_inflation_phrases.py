from pathlib import Path
import pdfplumber
import re

PDF_DIR = Path("data/raw/inflation/pdf")

def extract_text(pdf_path: Path) -> str:
    text = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:8]:
            t = page.extract_text()
            if t:
                text.append(t)
    joined = " ".join(text)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined

TARGETS = [
    {
        "country": "Sénégal",
        "glob": "SEN_ANSD_IHPC_2024_12_*.pdf",
        "patterns": [
            re.compile(
                r"(taux\s+d[’']inflation[^.]{0,200}?s[’']établit\s+à\s*[+\-]?\s*[0-9]+(?:[.,][0-9]+)?\s*%)",
                re.IGNORECASE
            )
        ],
        "fallback_context": "inflation"
    },
    {
        "country": "Côte d’Ivoire",
        "glob": "CIV_ANSTAT_IHPC_UEMOA_2025_06_*.pdf",
        "patterns": [
            # Attention: ce PDF contient aussi un glissement annuel énergie (-7,1%).
            # L'audit automatique peut tomber dessus: on garde l'audit manuel comme vérité métier.
            re.compile(r"(IHPC[^.]{0,400}?glissement\s+annuel[^.]{0,300}?[+\-]?\s*[0-9]+(?:[.,][0-9]+)?\s*%)", re.IGNORECASE),
            re.compile(r"(glissement\s+annuel[^.]{0,220}?[+\-]?\s*[0-9]+(?:[.,][0-9]+)?\s*%)", re.IGNORECASE),
        ],
        "fallback_context": "glissement annuel"
    }
]

print("\n=== AUDIT INFLATION : PHRASES (ne modifie pas le CSV) ===\n")

for t in TARGETS:
    matches = sorted(PDF_DIR.glob(t["glob"]))
    if not matches:
        print(f"[{t['country']}] PDF introuvable")
        continue

    pdf_path = matches[-1]
    text = extract_text(pdf_path)

    print(f"\n--- {t['country']} | fichier : {pdf_path.name} ---")

    found = False
    for p in t["patterns"]:
        m = p.search(text)
        if m:
            print("PHRASE / EXTRAIT TROUVÉ :")
            print(m.group(1))
            found = True
            break

    if not found:
        kw = t["fallback_context"].lower()
        idx = text.lower().find(kw)
        if idx != -1:
            start = max(0, idx - 400)
            end = min(len(text), idx + 700)
            print("⚠️  Pas de match strict. Extrait CONTEXTE :")
            print(text[start:end])
        else:
            print("⚠️  Aucun match et mot-clé introuvable dans les 8 premières pages.")
