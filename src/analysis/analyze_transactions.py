import os
import pandas as pd
import matplotlib.pyplot as plt

# 1. Charger le CSV
df = pd.read_csv(
    os.path.join('data', 'processed', 'transactions_2024.csv'),
    parse_dates=['date_mutation'],
    dtype={'code_postal': str}
)

# 2. Prétraitements
# 2.1. S'assurer que code_postal est une chaîne (et combler les NaN par '')
df['code_postal'] = df['code_postal'].fillna('').astype(str)

# 2.2. Convertir valeur_fonciere en float (virgule → point)
df['valeur_fonciere'] = (
    df['valeur_fonciere']
      .str.replace(' ', '')          # retirer espaces milliers s’il y en a
      .str.replace(',', '.', regex=False)
      .astype(float)
)

# 3. Statistiques de base
print("\n=== Aperçu des données ===")
print(df.head(5))
print("\n=== Statistiques numériques ===")
print(df[['valeur_fonciere','surface_reelle_bati','nombre_pieces_principales']].describe())

# 4. Transactions par département
df['dept'] = df['code_postal'].str[:2]
counts = df['dept'].value_counts().sort_index()

out_dir = os.path.join('outputs','figures')
os.makedirs(out_dir, exist_ok=True)

plt.figure()
counts.plot.bar()
plt.title('Nombre de transactions par département')
plt.xlabel('Département')
plt.ylabel('Nombre de transactions')
plt.tight_layout()
plt.savefig(os.path.join(out_dir, 'transactions_by_dept.png'))
plt.close()

# 5. Prix moyen au m² par département
df_valid = df[df['surface_reelle_bati'] > 0].copy()
df_valid['prix_m2'] = df_valid['valeur_fonciere'] / df_valid['surface_reelle_bati']
mean_price = df_valid.groupby('dept')['prix_m2'].mean().sort_index()

plt.figure()
mean_price.plot.bar()
plt.title('Prix moyen au m² par département')
plt.xlabel('Département')
plt.ylabel('Prix moyen (€)')
plt.tight_layout()
plt.savefig(os.path.join(out_dir, 'mean_price_m2_by_dept.png'))
plt.close()

print(f"\nGraphiques enregistrés dans {out_dir}")