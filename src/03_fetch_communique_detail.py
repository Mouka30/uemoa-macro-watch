import requests
from datetime import datetime
from pathlib import Path

URL = "https://www.bceao.int/fr/communique-presse/reunion-ordinaire-du-comite-de-politique-monetaire-de-la-bceao-tenue-le-3"

r = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
r.raise_for_status()

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out = Path("data/raw") / f"bceao_cpm_20251203_{ts}.html"

with open(out, "w", encoding="utf-8") as f:
    f.write(r.text)

print("Communiqué téléchargé :", out)
