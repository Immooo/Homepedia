import sqlite3, pandas as pd
from pathlib import Path
from datetime import datetime
from pynsee.localdata import get_local_metadata, get_local_data

DB = Path(__file__).resolve().parents[2] / "data" / "homepedia.db"

def latest_dataset(keyword: str) -> tuple[str, str]:
    """
    Retourne (dataset_id, dataset_version) du jeu le plus récent
    dont un des champs *title* contient `keyword` ET Geography = REGION
    """
    meta = get_local_metadata()
    meta.columns = meta.columns.str.lower()

    # repère toutes les colonnes contenant "title"
    title_cols = [c for c in meta.columns if "title" in c]

    if not title_cols:
        raise RuntimeError("Aucune colonne titre trouvée dans get_local_metadata()")

    # booléen : au moins un titre matche le mot-clé
    mask_title = meta[title_cols].apply(
        lambda row: any(keyword.lower() in str(v).lower() for v in row),
        axis=1
    )
    sub = meta[mask_title & meta["geography"].str.contains("region", case=False, na=False)]

    if sub.empty:
        raise ValueError(f"Pas de dataset contenant « {keyword} » au niveau régional.")

    ds_id_col  = [c for c in meta.columns if "datasetid" in c][0]
    ds_ver_col = [c for c in meta.columns if "datasetversion" in c][0]

    ds_id  = sub[ds_id_col].iloc[0]
    ds_ver = sub[ds_ver_col].max()
    return ds_id, ds_ver

def fetch(keyword: str, new_col: str) -> pd.DataFrame:
    ds_id, ds_ver = latest_dataset(keyword)
    df = get_local_data(ds_id, ds_ver)
    df.columns = df.columns.str.lower()
    df = (df[["reg", "obs_value"]]
          .rename(columns={"reg": "code_region", "obs_value": new_col})
          .assign(code_region=lambda d: d.code_region.astype(str).str.zfill(2)))
    return df

# Téléchargements
pop  = fetch("population légale",        "population")
chom = fetch("taux de chômage",          "taux_chomage")
rev  = fetch("revenu disponible médian", "income_median")
pov  = fetch("taux de pauvreté",         "poverty_rate")
print(meta.columns.tolist())
print(meta.head(3))

# Jointure
df = pop.merge(chom, on="code_region", how="outer") \
        .merge(rev,  on="code_region", how="outer") \
        .merge(pov,  on="code_region", how="outer") \
        .assign(ingested_at=datetime.utcnow().isoformat())

with sqlite3.connect(DB) as conn:
    df.to_sql("region_indicators", conn, if_exists="replace", index=False)
print(f"✅ {len(df)} lignes insérées dans region_indicators")
