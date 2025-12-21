import requests
from datetime import datetime
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# v0.1: URLs directes (on automatisera "dernier PDF" en v0.2)
PDF_SOURCES = {
    # Sénégal (ANSD) - exemple PDF mensuel
    "SEN_ANSD_IHPC_2024_12": "https://www.ansd.sn/sites/default/files/2025-01/IHPC_12_2024.pdf",
    # Côte d’Ivoire (ANStat) - exemple bulletin IHPC
    "CIV_ANSTAT_IHPC_2023_10": "https://www.anstat.ci/assets/publications/files/ihpc1023.pdf",
    # Côte d’Ivoire (ANStat) - IHPC UEMOA (ex: Juin 2025)
    "CIV_ANSTAT_IHPC_UEMOA_2025_06": "https://www.anstat.ci/assets/publications/files/File_val_indicateur1752229928.pdf",
}

# Chez toi, SSL casse pour ANSD/ANStat => exception contrôlée
NO_SSL_VERIFY_DOMAINS = ("ansd.sn", "anstat.ci")

out_dir = Path("data/raw/inflation/pdf")
out_dir.mkdir(parents=True, exist_ok=True)

headers = {"User-Agent": "Mozilla/5.0"}
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

for key, url in PDF_SOURCES.items():
    verify = not any(d in url for d in NO_SSL_VERIFY_DOMAINS)

    r = requests.get(url, timeout=60, headers=headers, verify=verify)
    r.raise_for_status()

    out_path = out_dir / f"{key}_{ts}.pdf"
    out_path.write_bytes(r.content)

    print("OK ->", key, "=>", out_path, "| ssl_verify =", verify)
