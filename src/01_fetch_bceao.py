import requests
from datetime import datetime

url = "https://www.bceao.int/fr/communique-presse"

r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
r.raise_for_status()

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"bceao_communique_presse_{ts}.html"

with open(filename, "w", encoding="utf-8") as f:
    f.write(r.text)

print("Page BCEAO téléchargée :", filename)
