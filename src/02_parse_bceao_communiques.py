from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import re

BASE = "https://www.bceao.int"
HTML_FILE = "bceao_communique_presse_20251218_044209.html"  # remplace par ton vrai fichier

with open(HTML_FILE, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f.read(), "html.parser")

rows = []
for a in soup.select("a[href^='/fr/communique-presse/']"):
    title = a.get_text(" ", strip=True)
    href = a.get("href", "").strip()
    if title and href:
        rows.append({"title": title, "url": urljoin(BASE, href)})

df = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)

# Tag utile : communiqués de politique monétaire
pattern = re.compile(r"(comit[eé].*politique mon[eé]taire|politique mon[eé]taire|taux directeur|guichet de pr[eê]t marginal)", re.I)
df["is_monetary_policy"] = df["title"].apply(lambda x: bool(pattern.search(str(x))))

df.to_csv("bceao_communique_presse.csv", index=False, encoding="utf-8")

print("OK -> bceao_communique_presse.csv | lignes:", len(df))
print("\n--- Extraits monétaires ---")
print(df[df["is_monetary_policy"]].head(20).to_string(index=False))
