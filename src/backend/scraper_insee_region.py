import re, unicodedata, sqlite3, requests, pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

URL_POP    = "https://www.insee.fr/fr/statistiques/7728783"
URL_REVPOV = "https://www.insee.fr/fr/statistiques/7941411"
URL_CHOM   = "https://www.insee.fr/fr/statistiques/2012804"

DB = Path(__file__).resolve().parents[2] / "data" / "homepedia.db"

# ---------- fonctions utilitaires ----------
def _read_tables(url: str):
    html = requests.get(url, headers=HEADERS, timeout=30).text
    return pd.read_html(StringIO(html), thousands="\u202f", decimal=",", flavor="lxml")

def _clean_numeric(series: pd.Series) -> pd.Series:
    """Supprime tout sauf chiffres, point, signe – puis convertit en float."""
    return (
        series.astype(str)
              .str.replace(r"[^\d\.-]", "", regex=True)
              .replace({"": None})
              .astype(float)
    )

def _slug(text: str) -> str:
    """Normalise un libellé : minuscules, sans accents, sans espaces/tirets."""
    txt = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", txt.lower())

# ---------- extraction individuelle ----------
def fetch_population() -> pd.DataFrame:
    tbl = next(t for t in _read_tables(URL_POP) if "Code région" in t.columns[0])
    df = tbl.iloc[:, :3]                                   # Code | Région | Population
    df.columns = ["code_region", "region", "population"]
    df["code_region"] = df["code_region"].astype(str).str.zfill(2)
    df["population"]  = _clean_numeric(df["population"])
    df["slug"]        = df["region"].map(_slug)
    return df[["code_region", "region", "slug", "population"]]

def fetch_rev_pov(pop: pd.DataFrame) -> pd.DataFrame:
    tbl = next(t for t in _read_tables(URL_REVPOV)
               if "Niveau de vie annuel" in " ".join(map(str, t.columns)))
    df = tbl.iloc[:, :6]                                   # Région | … | Médian | … | Taux pauvreté
    df.columns = ["region", "d1", "income_median", "d9", "rapport", "poverty_rate"]
    df["slug"] = df["region"].map(_slug)
    # jointure sur slug (libellé harmonisé)
    df = df.merge(pop[["code_region", "slug"]], on="slug", how="left")
    df["income_median"] = _clean_numeric(df["income_median"])
    df["poverty_rate"]  = _clean_numeric(df["poverty_rate"])
    return df[["code_region", "income_median", "poverty_rate"]]

def fetch_unemployment(pop: pd.DataFrame) -> pd.DataFrame:
    tbl = next(t for t in _read_tables(URL_CHOM)
               if "trim. 2025" in " ".join(map(str, t.columns)))
    df = tbl.iloc[:, :4]                                   # Région | T1-2025 | … | …
    df.columns = ["region", "taux_chomage", "_", "__"]
    df["slug"] = df["region"].map(_slug)
    df = df.merge(pop[["code_region", "slug"]], on="slug", how="left")
    df["taux_chomage"] = _clean_numeric(df["taux_chomage"])
    return df[["code_region", "taux_chomage"]]

# ---------- pipeline principal ----------
if __name__ == "__main__":
    pop  = fetch_population()          # 18 régions attendues
    revp = fetch_rev_pov(pop)
    cho  = fetch_unemployment(pop)

    df = (
        pop.drop(columns=["region", "slug"])
           .merge(revp, on="code_region", how="left")
           .merge(cho,  on="code_region", how="left")
    )
    df["ingested_at"] = datetime.utcnow().isoformat()

    # contrôle rapide
    print("🗹 Populations récupérées :", len(pop))
    print("🗹 Lignes finales         :", len(df))

    with sqlite3.connect(DB) as conn:
        df.to_sql("region_indicators", conn, if_exists="replace", index=False)

    print("✅ region_indicators mis à jour :", len(df), "régions")