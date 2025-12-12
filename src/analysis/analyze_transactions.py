import os
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd

from backend.logging_setup import setup_logging

logger = setup_logging()

DB_PATH = os.path.join("data", "homepedia.db")
conn = sqlite3.connect(DB_PATH)

query = """
SELECT
  *,
  CAST(REPLACE(REPLACE(valeur_fonciere,' ',''),',','.') AS REAL) AS valeur_fonciere_num
FROM transactions
"""

df = pd.read_sql_query(query, conn, parse_dates=["date_mutation"])

# Nettoyage surface
df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
df = df[df["surface_reelle_bati"] > 0]

df["prix_m2"] = df["valeur_fonciere_num"] / df["surface_reelle_bati"]

# 🔥 Correction code postal
df["code_postal"] = (
    df["code_postal"].astype(str).str.replace(".0", "", regex=False).str.zfill(5)
)

df["dept"] = df["code_postal"].str[:2]

# Graphiques
out_dir = os.path.join("outputs", "figures")
os.makedirs(out_dir, exist_ok=True)

counts = df["dept"].value_counts().sort_index()
plt.figure()
counts.plot.bar()
plt.title("Nombre de transactions par département")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "transactions_by_dept.png"))
plt.close()

mean_price = df.groupby("dept")["prix_m2"].mean().sort_index()
plt.figure()
mean_price.plot.bar()
plt.title("Prix moyen au m² par département")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "mean_price_m2_by_dept.png"))
plt.close()

conn.close()
logger.info("✅ Analyse transactions terminée")
