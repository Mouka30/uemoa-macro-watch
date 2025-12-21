from pathlib import Path
from datetime import datetime
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; uemoa-macro-watch/1.0)"}

OUT_DIR = Path("data/raw/anstat/csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

INDICATOR_URLS = [
    ("IHPC_national", "https://www.anstat.ci/indicateur-details/1d9192e901c724a6217631034cc3cc45b74984f99fae02aa663715346db147d653d0854a7cd2e2fc1fe88b179a41f0b350a20a394b427fc24704d4e94cd0478e7elCY_4H4C8Hx69ardSve9-Jh_ZMncuulMUEm1kIUJc"),
    ("Infl_gliss_moy_ann", "https://www.anstat.ci/indicateur-details/4abbd412ba595b43eff78fc2aec8338fbae7a24b9826d2eabb1f255815c99670023507833469a4a09e5e30b8dd26350970e37c55b84ccb9fb62d9dc1b2dd98c43Xbezh96bRbC-ZRFbFQCJc5Uf8dDLuqYRhpwUFXDhvM"),
    ("Infl_moy_ann", "https://www.anstat.ci/indicateur-details/72c050f25f408ab781f73b7fc9375cf05f977ba89dffaf6e023ac1214bef56652503451b0719b1753a05a86669697213527f0f6524e5037df237d034c198bd7632wz--xp8G9SbscLHJJfzVscQUtFMpCD5Tqjl0NdCek"),
    ("Infl_moy_mens", "https://www.anstat.ci/indicateur-details/feb6932fe3d2331df4e834753637829d5632a1a406d8e60424b56de233e529c11a1a27719071cef73560a06e932c56d48e42be2e3d05eba549ddddf7106b32daakLsFCuBGDXOUjVdFKEbTxHSbPUv74Txm9qBy2XTlbE"),
]

DOWNLOAD_PREFIX = "https://www.anstat.ci/indicateur/download_csv/"

def get_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45, verify=False)
    r.raise_for_status()
    return r.text

def find_indicator_id(html: str) -> str | None:
    """
    Cherche un ID d'indicateur dans le HTML.
    Stratégies (dans cet ordre) :
    1) data-id="123"
    2) data-id='123'
    3) id: 123 dans un blob JS/JSON
    4) /download_csv/123 si l'URL est déjà écrite
    """
    # 1/2) data-id
    m = re.search(r'data-id\s*=\s*["\'](\d{1,10})["\']', html, re.IGNORECASE)
    if m:
        return m.group(1)

    # 3) JSON/JS style: "id":123 or id:123
    m = re.search(r'["\']id["\']\s*:\s*(\d{1,10})', html, re.IGNORECASE)
    if m:
        return m.group(1)

    m = re.search(r'\bid\s*:\s*(\d{1,10})\b', html, re.IGNORECASE)
    if m:
        return m.group(1)

    # 4) download_csv already present
    m = re.search(r'/indicateur/download_csv/(\d{1,10})', html, re.IGNORECASE)
    if m:
        return m.group(1)

    return None

def download_csv(indicator_id: str) -> bytes:
    url = DOWNLOAD_PREFIX + indicator_id
    r = requests.get(url, headers=HEADERS, timeout=60, verify=False)
    r.raise_for_status()
    return r.content

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n=== ANStat -> download_csv (extraction) ===\n")
    for label, url in INDICATOR_URLS:
        print(f"--- {label} ---")
        try:
            html = get_html(url)

            # Optionnel: essayer de lire un titre côté HTML
            soup = BeautifulSoup(html, "html.parser")
            page_title = (soup.select_one("h1") or soup.select_one("h2") or soup.select_one("title"))
            page_title = page_title.get_text(" ", strip=True) if page_title else ""

            ind_id = find_indicator_id(html)
            if not ind_id:
                print("NEEDS AUDIT -> ID indicateur introuvable dans le HTML.")
                print("  Astuce: cherche 'data-id' dans le fichier HTML sauvegardé.")
                continue

            print("Title:", page_title)
            print("Indicator ID:", ind_id)

            csv_bytes = download_csv(ind_id)
            out_csv = OUT_DIR / f"ANSTAT_{label}_{ind_id}_{ts}.csv"
            out_csv.write_bytes(csv_bytes)
            print("OK -> CSV téléchargé:", out_csv)

            # Lecture pandas (essai)
            try:
                df = pd.read_csv(out_csv)
                print("CSV columns:", list(df.columns)[:12])
                print("CSV rows:", len(df))

                # Tentative : détecter la colonne date
                date_cols = [c for c in df.columns if str(c).lower() in ("date", "periode", "période", "period", "mois", "annee", "année")]
                if date_cols:
                    print("Date-like columns:", date_cols)

                # Affiche 5 dernières lignes
                print("\nLast 5 rows:")
                print(df.tail(5).to_string(index=False))
            except Exception as e:
                print("WARN -> CSV lu mais format non standard:", e)

        except Exception as e:
            print("ERROR ->", e)

        print()

if __name__ == "__main__":
    main()
