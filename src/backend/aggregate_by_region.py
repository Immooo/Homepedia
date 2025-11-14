import os
import sqlite3

import pandas as pd

from backend.logging_setup import setup_logging

logger = setup_logging()

# 1. Chemins
DB_PATH = os.path.join("data", "homepedia.db")
DEP_REG_CSV = os.path.join("data", "raw", "insee", "dept_region.csv")
OUT_TABLE = "region_analysis"

# 2. Chargement de la correspondance département→région
# Lecture avec tentative de détection automatique du séparateur
try:
    df_dep_reg = pd.read_csv(DEP_REG_CSV, dtype=str, sep=";", engine="python")
except Exception:
    df_dep_reg = pd.read_csv(DEP_REG_CSV, dtype=str)

# Normalisation colonnes : trouver celles contenant 'dep' et 'reg'
cols = df_dep_reg.columns.tolist()
dep_col = next((c for c in cols if "dep" in c.lower()), None)
reg_col = next((c for c in cols if "reg" in c.lower()), None)
if not dep_col or not reg_col:
    raise KeyError(f"Colonnes Dépt/Région introuvables dans {DEP_REG_CSV}: {cols}")
# Renommage standard
df_dep_reg = df_dep_reg.rename(columns={dep_col: "DEP", reg_col: "REG"})

# Zéro-pad codes
df_dep_reg["DEP"] = df_dep_reg["DEP"].str.zfill(2)
df_dep_reg["REG"] = df_dep_reg["REG"].str.zfill(2)

# 3. Connexion SQLite
conn = sqlite3.connect(DB_PATH)

# 4. Transactions par région
pdf_tx = pd.read_sql_query(
    "SELECT code_postal, valeur_fonciere, surface_reelle_bati FROM transactions", conn
)
pdf_tx["dept"] = pdf_tx["code_postal"].str[:2]
pdf_tx = pdf_tx.merge(df_dep_reg, left_on="dept", right_on="DEP", how="left")
pdf_tx = pdf_tx.dropna(subset=["REG"])
pdf_tx["valeur_fonciere"] = pd.to_numeric(pdf_tx["valeur_fonciere"], errors="coerce")
pdf_tx["surface_reelle_bati"] = pd.to_numeric(
    pdf_tx["surface_reelle_bati"], errors="coerce"
)
pdf_tx = pdf_tx[pdf_tx["surface_reelle_bati"] > 0]
pdf_tx["prix_m2"] = pdf_tx["valeur_fonciere"] / pdf_tx["surface_reelle_bati"]
rg_tx = (
    pdf_tx.groupby("REG")
    .agg(nb_transactions=("prix_m2", "size"), prix_m2_moyen=("prix_m2", "mean"))
    .reset_index()
    .rename(columns={"REG": "code_region"})
)

# 5. Indicateurs INSEE agrégés
agg_dfs = []
for table, col, aggfunc in [
    ("population", "population", "sum"),
    ("income", "income_median", "median"),
    ("unemployment", "taux_chomage", "mean"),
    ("poverty", "poverty_rate", "mean"),
]:
    df = pd.read_sql_query(f"SELECT code, {col} FROM {table}", conn)
    df = df.rename(columns={"code": "DEPCODE"})
    df = df.merge(df_dep_reg, left_on="DEPCODE", right_on="DEP", how="left")
    df = df.dropna(subset=["REG"])
    df[col] = pd.to_numeric(df[col], errors="coerce")
    summary = getattr(df.groupby("REG")[col], aggfunc)().reset_index()
    summary = summary.rename(columns={"REG": "code_region", col: table})
    agg_dfs.append(summary)

# 6. Fusion de toutes les tables
df_all = rg_tx.copy()
for agg_df in agg_dfs:
    df_all = df_all.merge(agg_df, on="code_region", how="left")

# 7. Écriture en SQLite
df_all.to_sql(OUT_TABLE, conn, if_exists="replace", index=False)
conn.close()
logger.info("✅ Table '%s' créée avec %d lignes.", OUT_TABLE, len(df_all))
