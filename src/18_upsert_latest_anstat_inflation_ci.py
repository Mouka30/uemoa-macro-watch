from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; uemoa-macro-watch/1.0)"}

OUT_CSV = Path("data/processed/macro_uemoa.csv")
DOWNLOAD_PREFIX = "https://www.anstat.ci/indicateur/download_csv/"

# Mapping: label -> (indicator_id, indicator_name, unit)
SERIES = [
    ("IHPC_national", 1868, "IHPC (index national)", "index"),
    ("Infl_moy_ann", 1917, "Inflation IHPC (moyenne annuelle, national)", "%"),
    ("Infl_gliss_moy_ann", 1871, "Inflation IHPC (glissement, moyenne annuelle, national)", "%"),
    ("Infl_moy_mens", 624, "Inflation IHPC (moyenne mensuelle, national)", "%"),
]

def load_df():
    if OUT_CSV.exists():
        return pd.read_csv(OUT_CSV)
    return pd.DataFrame()

def save_df(df: pd.DataFrame):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

def upsert(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    new_df = pd.DataFrame([row])
    key_cols = ["country", "indicator", "date_reference"]
    if df.empty:
        return new_df

    # garantir colonnes
    for c in new_df.columns:
        if c not in df.columns:
            df[c] = None
    for c in df.columns:
        if c not in new_df.columns:
            new_df[c] = None

    old_keys = df[key_cols].astype(str).agg("|".join, axis=1)
    new_keys = new_df[key_cols].astype(str).agg("|".join, axis=1)
    return pd.concat([df[~old_keys.isin(new_keys)], new_df], ignore_index=True)

def download_csv(indicator_id: int) -> pd.DataFrame:
    url = DOWNLOAD_PREFIX + str(indicator_id)
    r = requests.get(url, headers=HEADERS, timeout=60, verify=False)
    r.raise_for_status()
    # le CSV est petit, on le lit direct depuis le contenu
    from io import BytesIO
    return pd.read_csv(BytesIO(r.content))

def last_observation(df: pd.DataFrame) -> tuple[str, float]:
    """
    Sort by (annee_fin_couv, mois_fin_couv) ascending, take last.
    Returns date_reference 'YYYY-MM' and value float.
    """
    sort_cols = ["annee_fin_couv", "mois_fin_couv"]
    df2 = df.copy()
    df2[sort_cols] = df2[sort_cols].astype(int)
    df2 = df2.sort_values(sort_cols, ascending=True)

    last = df2.iloc[-1]
    y = int(last["annee_fin_couv"])
    m = int(last["mois_fin_couv"])
    date_ref = f"{y:04d}-{m:02d}"
    val = float(last["valeur"])
    return date_ref, val

def main():
    country = "Côte d’Ivoire"
    source_name = "ANStat"
    now = datetime.now().isoformat(timespec="seconds")

    df_out = load_df()

    print("\n=== ANStat -> UPSERT latest series (CI) ===\n")

    for label, indicator_id, indicator_name, unit in SERIES:
        series_df = download_csv(indicator_id)
        date_ref, val = last_observation(series_df)

        row = {
            "country": country,
            "indicator": indicator_name,
            "value": val,
            "unit": unit,
            "date_reference": date_ref,
            "source_name": source_name,
            "source_url": f"https://www.anstat.ci/indicateur/download_csv/{indicator_id}",
            "collected_at": now,
            "comment": f"Auto from ANStat CSV (indicator_id={indicator_id}, label={label}) - last observation.",
        }

        df_out = upsert(df_out, row)
        print(f"UPSERT OK -> {indicator_name} | {date_ref} = {val} {unit}")

    save_df(df_out)

    print("\nOK -> macro_uemoa.csv mis à jour.")
    print(df_out[df_out["country"] == country].sort_values(["indicator", "date_reference"]).tail(12).to_string(index=False))

if __name__ == "__main__":
    main()
