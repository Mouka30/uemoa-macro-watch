import requests
from datetime import datetime
from pathlib import Path
import certifi
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOURCES = {
    "Senegal_ANSD_IHPC_annual": "https://www.ansd.sn/Indicateur/evolution-annuelle-de-lindice-harmonise-des-prix-la-consommation",
    "CIV_ANStat_home": "https://www.anstat.ci/",
}

NO_SSL_VERIFY = {"Senegal_ANSD_IHPC_annual"}

out_dir = Path("data/raw/inflation")
out_dir.mkdir(parents=True, exist_ok=True)

headers = {"User-Agent": "Mozilla/5.0"}
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

for name, url in SOURCES.items():
    try:
        verify = False if name in NO_SSL_VERIFY else certifi.where()
        r = requests.get(url, timeout=30, headers=headers, verify=verify)
        r.raise_for_status()

        path = out_dir / f"{name}_{ts}.html"
        path.write_text(r.text, encoding="utf-8")

        print("OK ->", name, "=>", path, "| ssl_verify =", verify)

    except Exception as e:
        print("ERROR ->", name, "|", url)
        print("   ", repr(e))
