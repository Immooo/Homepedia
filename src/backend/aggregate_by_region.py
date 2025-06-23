import os
import sqlite3
import pandas as pd

# 1. Chemins
DB_PATH      = os.path.join('data', 'homepedia.db')
DEP_REG_CSV  = os.path.join('data', 'raw', 'insee', 'dept_region.csv')
OUT_TABLE    = 'region_analysis'

# 2. Chargement
conn = sqlite3.connect(DB_PATH)
df_dep_reg = pd.read_csv(DEP_REG_CSV, dtype=str, sep=';')  # ou sep=',' selon le CSV
# On garde colonnes code_dept (2 chars) et code_reg (2 chars)
df_dep_reg['DEP'] = df_dep_reg['DEP'].str.zfill(2)
df_dep_reg['REG'] = df_dep_reg['REG'].str.zfill(2)

# Fonctions d'agrégation
agg_list = []
# 3. Transactions
pdf_tx = pd.read_sql_query('SELECT code_postal, valeur_fonciere, surface_reelle_bati FROM transactions', conn)
pdf_tx['dept'] = pdf_tx['code_postal'].str[:2]
pdf_tx = pdf_tx.merge(df_dep_reg, left_on='dept', right_on='DEP', how='left')
pdf_tx = pdf_tx.dropna(subset=['REG'])
pdf_tx['prix_m2'] = pdf_tx['valeur_fonciere'] / pdf_tx['surface_reelle_bati']
rg_tx = pdf_tx.groupby('REG').agg(
    nb_transactions=('prix_m2','size'),
    prix_m2_moyen=('prix_m2','mean')
).reset_index().rename(columns={'REG':'code_region'})

# 4. Indicateurs INSEE
for table, col, fun in [
    ('population','population','sum'),
    ('income','income_median','median'),
    ('unemployment','taux_chomage','mean'),
    ('poverty','poverty_rate','mean')
]:
    df = pd.read_sql_query(f'SELECT * FROM {table}', conn)
    df = df.merge(df_dep_reg, left_on='code', right_on='DEP', how='left').dropna(subset=['REG'])
    agg = getattr(df.groupby('REG')[col], fun)().reset_index().rename(columns={'REG':'code_region', col:table})
    agg_list.append(agg)

# 5. Fusion de tous
df_all = rg_tx.copy()
for df in agg_list:
    df_all = df_all.merge(df, on='code_region', how='left')

# 6. Sauvegarde en base
df_all.to_sql(OUT_TABLE, conn, if_exists='replace', index=False)
conn.close()
print(f"✅ Table '{OUT_TABLE}' créée dans SQLite avec {len(df_all)} régions.")