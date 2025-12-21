import requests
from bs4 import BeautifulSoup
import re
import urllib3

# Désactiver les warnings SSL (sites institutionnels mal configurés)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UEMOA-Macro-Watch/1.0)"
}

TARGETS = [
    {
        "country": "Côte d’Ivoire",
        "name": "ANStat",
        "url": "https://www.anstat.ci/",
        "pdf_regex": re.compile(r"IHPC|Indice\s+Harmonisé", re.IGNORECASE),
    },
    {
        "country": "Sénégal",
        "name": "ANSD",
        "url": "https://www.ansd.sn/",
        "pdf_regex": re.compile(r"IHPC|Indice\s+Harmonisé", re.IGNORECASE),
    },
]

def list_pdfs(base_url: str, pdf_regex: re.Pattern):
    """
    Récupère tous les liens PDF correspondant au regex sur une page donnée
    """
    response = requests.get(
        base_url,
        headers=HEADERS,
        timeout=30,
        verify=False  # IMPORTANT : sites ANSD / ANStat
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    pdfs = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True)

        if not href.lower().endswith(".pdf"):
            continue

        if pdf_regex.search(href) or pdf_regex.search(text):
            if not href.startswith("http"):
                href = requests.compat.urljoin(base_url, href)
            pdfs.append({
                "title": text,
                "url": href
            })

    return pdfs

print("\n=== LISTE DES PDFs IHPC DISPONIBLES (SCRIPT 13) ===\n")

for target in TARGETS:
    print(f"--- {target['country']} | {target['name']} ---")

    try:
        pdfs = list_pdfs(target["url"], target["pdf_regex"])

        if not pdfs:
            print("⚠️  Aucun PDF IHPC détecté sur cette page.")
        else:
            for i, pdf in enumerate(pdfs, start=1):
                print(f"{i}. {pdf['title'] or '[sans titre]'}")
                print(f"   {pdf['url']}")

    except Exception as e:
        print("❌ Erreur lors du scraping :", e)

    print()
