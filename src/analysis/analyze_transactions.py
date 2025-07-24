import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# 1. Connexion à la base SQLite
DB_PATH = os.path.join('data', 'homepedia.db')
conn = sqlite3.connect(DB_PATH)

# 2. Chargement des données et conversion de la valeur foncière
query = """
SELECT
  *,
  CAST(REPLACE(REPLACE(valeur_fonciere,' ',''),',','.') AS REAL) AS valeur_fonciere_num
FROM transactions
"""
df = pd.read_sql_query(query, conn, parse_dates=['date_mutation'])

# 3. Prétraitements
df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
df = df[df['surface_reelle_bati'] > 0]
df['prix_m2'] = df['valeur_fonciere_num'] / df['surface_reelle_bati']
df['dept'] = df['code_postal'].str[:2]

# 4. Statistiques de base
print("\n=== Aperçu des données ===")
print(df.head(5))
print("\n=== Statistiques numériques ===")
print(df[['valeur_fonciere_num','surface_reelle_bati','nombre_pieces_principales']].describe())

# 5. Nombre de transactions par département
counts = df['dept'].value_counts().sort_index()
out_dir = os.path.join('outputs', 'figures')
os.makedirs(out_dir, exist_ok=True)
plt.figure()
counts.plot.bar()
plt.title('Nombre de transactions par département')
plt.xlabel('Département')
plt.ylabel('Nombre de transactions')
plt.tight_layout()
plt.savefig(os.path.join(out_dir, 'transactions_by_dept.png'))
plt.close()

# 6. Prix moyen au m² par département
mean_price = df.groupby('dept')['prix_m2'].mean().sort_index()
plt.figure()
mean_price.plot.bar()
plt.title('Prix moyen au m² par département')
plt.xlabel('Département')
plt.ylabel('Prix moyen (€)')
plt.tight_layout()
plt.savefig(os.path.join(out_dir, 'mean_price_m2_by_dept.png'))
plt.close()

print(f"\nGraphiques enregistrés dans {out_dir}")
conn.close()
