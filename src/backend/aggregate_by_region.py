import os
import sqlite3

import pandas as pd

# from src.backend.logging_setup import setup_logging
from src.backend.logging_setup import setup_logging


def normalize_dept(code: str) -> str:
    if pd.isna(code):
        return ""
    code = str(code).replace(".0", "")
    if code in ("2A", "2B"):
        return code
    return code.zfill(2)


logger = setup_logging()

# 1. Chemins
DB_PATH = os.path.join("data", "homepedia.db")
DEP_REG_CSV = os.path.join("data", "raw", "insee", "dept_region.csv")
OUT_TABLE = "analyse_regionale"

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

# 4. Transactions par région à partir de la pré-agrégation Spark.
# On évite ainsi de recharger les 5,8 millions de transactions en mémoire.
pdf_tx = pd.read_sql_query(
    "SELECT dept, nb_transactions, prix_m2_moyen FROM analyse_departementale", conn
)
pdf_tx["dept"] = pdf_tx["dept"].apply(normalize_dept)
pdf_tx = pdf_tx.merge(df_dep_reg, left_on="dept", right_on="DEP", how="left")
pdf_tx = pdf_tx.dropna(subset=["REG"])
pdf_tx["montant_pondere"] = pdf_tx["prix_m2_moyen"] * pdf_tx["nb_transactions"]
rg_tx = pdf_tx.groupby("REG", as_index=False).agg(
    nb_transactions=("nb_transactions", "sum"),
    montant_pondere=("montant_pondere", "sum"),
)
rg_tx["prix_m2_moyen"] = rg_tx["montant_pondere"] / rg_tx["nb_transactions"]
rg_tx = rg_tx.drop(columns=["montant_pondere"]).rename(columns={"REG": "code_region"})

# 5. Indicateurs INSEE agrégés
agg_dfs = []
for table, col, aggfunc in [
    ("population", "population", "sum"),
    ("revenus", "revenu_median", "median"),
    ("chomage", "taux_chomage", "mean"),
    ("pauvrete", "taux_pauvrete", "mean"),
]:
    df = pd.read_sql_query(f"SELECT code, {col} FROM {table}", conn)

    # 🔥 NORMALISATION CRITIQUE
    df["DEPCODE"] = df["code"].apply(normalize_dept)

    df = df.merge(
        df_dep_reg,
        left_on="DEPCODE",
        right_on="DEP",
        how="left",
        indicator=True,
    )

    # LOG des lignes perdues (important)
    lost = df[df["_merge"] == "left_only"]
    if not lost.empty:
        logger.warning(
            "[WARN] %d lignes %s non rattachées à une région",
            len(lost),
            table,
        )

    df = df.dropna(subset=["REG"])
    df[col] = pd.to_numeric(df[col], errors="coerce")

    summary = getattr(df.groupby("REG")[col], aggfunc)().reset_index()

    column_map = {
        "population": "population",
        "revenu_median": "revenu_median",
        "taux_chomage": "taux_chomage",
        "taux_pauvrete": "taux_pauvrete",
    }

    summary = summary.rename(
        columns={
            "REG": "code_region",
            col: column_map[col],
        }
    )

    agg_dfs.append(summary)

# 6. Fusion de toutes les tables
df_all = rg_tx.copy()
for agg_df in agg_dfs:
    df_all = df_all.merge(agg_df, on="code_region", how="left")

# 7. Écriture en SQLite
df_all.to_sql(OUT_TABLE, conn, if_exists="replace", index=False)
conn.close()
logger.info("✅ Table '%s' créée avec %d lignes.", OUT_TABLE, len(df_all))
