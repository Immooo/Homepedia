import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd

from backend.logging_setup import setup_logging

logger = setup_logging()

# 1. Connexion à la base SQLite
DB_PATH = os.path.join("data", "homepedia.db")
logger.info("Ouverture de la base SQLite : %s", DB_PATH)
conn = sqlite3.connect(DB_PATH)

# 2. Chargement des données et conversion de la valeur foncière
query = """
SELECT
  *,
  CAST(REPLACE(REPLACE(valeur_fonciere,' ',''),',','.') AS REAL) AS valeur_fonciere_num
FROM transactions
"""
logger.info("Exécution de la requête SQL pour charger les transactions")
df = pd.read_sql_query(query, conn, parse_dates=["date_mutation"])

# 3. Prétraitements
logger.info("Prétraitements : casting, filtrage des surfaces > 0 et calcul prix/m²")
df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
df = df[df["surface_reelle_bati"] > 0]
df["prix_m2"] = df["valeur_fonciere_num"] / df["surface_reelle_bati"]
df["dept"] = df["code_postal"].astype(str).str[:2]

# 4. Statistiques de base
logger.info("=== Aperçu des données ===\n%s", df.head(5))
logger.info(
    "=== Statistiques numériques ===\n%s",
    df[
        ["valeur_fonciere_num", "surface_reelle_bati", "nombre_pieces_principales"]
    ].describe(),
)

# 5. Nombre de transactions par département
counts = df["dept"].value_counts().sort_index()
out_dir = os.path.join("outputs", "figures")
os.makedirs(out_dir, exist_ok=True)
plt.figure()
counts.plot.bar()
plt.title("Nombre de transactions par département")
plt.xlabel("Département")
plt.ylabel("Nombre de transactions")
plt.tight_layout()
out_counts = os.path.join(out_dir, "transactions_by_dept.png")
plt.savefig(out_counts)
plt.close()
logger.info("Graphique sauvegardé : %s", out_counts)

# 6. Prix moyen au m² par département
mean_price = df.groupby("dept")["prix_m2"].mean().sort_index()
plt.figure()
mean_price.plot.bar()
plt.title("Prix moyen au m² par département")
plt.xlabel("Département")
plt.ylabel("Prix moyen (€)")
plt.tight_layout()
out_mean = os.path.join(out_dir, "mean_price_m2_by_dept.png")
plt.savefig(out_mean)
plt.close()
logger.info("Graphique sauvegardé : %s", out_mean)

logger.info("✅ Graphiques enregistrés dans %s", out_dir)
conn.close()
logger.info("Fermeture de la base SQLite")
