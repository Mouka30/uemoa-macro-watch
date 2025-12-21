from pathlib import Path
from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; uemoa-macro-watch/1.0)"}

URLS = [
    "https://www.anstat.ci/indicateur-details/1d9192e901c724a6217631034cc3cc45b74984f99fae02aa663715346db147d653d0854a7cd2e2fc1fe88b179a41f0b350a20a394b427fc24704d4e94cd0478e7elCY_4H4C8Hx69ardSve9-Jh_ZMncuulMUEm1kIUJc",
    "https://www.anstat.ci/indicateur-details/4abbd412ba595b43eff78fc2aec8338fbae7a24b9826d2eabb1f255815c99670023507833469a4a09e5e30b8dd26350970e37c55b84ccb9fb62d9dc1b2dd98c43Xbezh96bRbC-ZRFbFQCJc5Uf8dDLuqYRhpwUFXDhvM",
    "https://www.anstat.ci/indicateur-details/72c050f25f408ab781f73b7fc9375cf05f977ba89dffaf6e023ac1214bef56652503451b0719b1753a05a86669697213527f0f6524e5037df237d034c198bd7632wz--xp8G9SbscLHJJfzVscQUtFMpCD5Tqjl0NdCek",
    "https://www.anstat.ci/indicateur-details/feb6932fe3d2331df4e834753637829d5632a1a406d8e60424b56de233e529c11a1a27719071cef73560a06e932c56d48e42be2e3d05eba549ddddf7106b32daakLsFCuBGDXOUjVdFKEbTxHSbPUv74Txm9qBy2XTlbE",
]

OUT_DIR = Path("data/raw/anstat")
OUT_DIR.mkdir(parents=True, exist_ok=True)

API_HINTS = re.compile(r"(api|graphql|endpoint|json|indicateur|indicator|series|chart|data)", re.IGNORECASE)

def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45, verify=False)
    r.raise_for_status()
    return r.text

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def extract_candidates(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")

    title = None
    for sel in ["h1", "h2", "h3", "title"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(" ", strip=True)
            break
    title = title or "UNKNOWN_TITLE"

    # PDF links
    pdfs = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if href.lower().endswith(".pdf"):
            pdfs.add(requests.compat.urljoin(base_url, href))

    # Any absolute PDFs in raw html
    for m in re.finditer(r"(https?://[^\s\"']+\.pdf)", html, re.IGNORECASE):
        pdfs.add(m.group(1))

    # Potential API endpoints in raw html
    endpoints = set()
    for m in re.finditer(r"(https?://[^\s\"']+)", html):
        u = m.group(1)
        if "anstat.ci" in u and API_HINTS.search(u):
            endpoints.add(u)

    # JS script src (could contain app bundle)
    scripts = set()
    for s in soup.select("script[src]"):
        src = s.get("src", "").strip()
        if not src:
            continue
        full = requests.compat.urljoin(base_url, src)
        scripts.add(full)

    # Look for embedded JSON blobs
    json_snippets = []
    for m in re.finditer(r"(\{[^{}]{50,2000}\})", html):
        block = m.group(1)
        if API_HINTS.search(block):
            json_snippets.append(block[:400] + ("..." if len(block) > 400 else ""))

    return title, sorted(pdfs), sorted(endpoints), sorted(scripts), json_snippets

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = "https://www.anstat.ci/"

    print("\n=== PROBE ANStat: indicateur-details (diagnostic) ===\n")

    for i, url in enumerate(URLS, start=1):
        print(f"\n[{i}] URL: {url}")

        try:
            html = fetch(url)
        except Exception as e:
            print("  ERROR fetch:", e)
            continue

        # Save HTML for inspection
        out_html = OUT_DIR / f"anstat_indicator_{i}_{ts}.html"
        out_html.write_text(html, encoding="utf-8")
        print(f"  Saved HTML -> {out_html}")

        title, pdfs, endpoints, scripts, json_snips = extract_candidates(html, base)
        print(f"  Title: {title}")

        print(f"  PDF links found: {len(pdfs)}")
        for p in pdfs[:5]:
            print("   -", p)

        print(f"  API-like endpoints found in HTML: {len(endpoints)}")
        for e in endpoints[:8]:
            print("   -", e)

        print(f"  JS script bundles found: {len(scripts)}")
        for s in scripts[:5]:
            print("   -", s)

        if json_snips:
            print(f"  Embedded JSON-like snippets (sample): {len(json_snips)}")
            print("   -", normalize_ws(json_snips[0])[:300])

        # Also show a small text preview (to see if data is server-rendered)
        text_preview = normalize_ws(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))[:400]
        print("  Text preview:", text_preview)

    print("\n=== FIN PROBE ===\n")

if __name__ == "__main__":
    main()
