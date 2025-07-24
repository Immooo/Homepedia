import os
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Date, Numeric
)

# 1. Définition des chemins
DB_FILE     = os.path.join('data', 'homepedia.db')
TX_CSV      = os.path.join('data', 'processed', 'transactions_2024.csv')
POP_CSV     = os.path.join('data', 'processed', 'population_dept.csv')
POV_CSV     = os.path.join('data', 'processed', 'poverty_dept.csv')

# 2. Création de l'engine SQLite
engine = create_engine(f"sqlite:///{DB_FILE}")
metadata = MetaData()

# 3. Définition des tables
transactions = Table(
    'transactions', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('date_mutation', Date, nullable=False),
    Column('nature_mutation', String, nullable=True),
    Column('valeur_fonciere', Numeric(12, 2), nullable=False),
    Column('code_postal', String(10), nullable=False),
    Column('commune', String(100), nullable=False),
    Column('type_local', String(50), nullable=True),
    Column('surface_reelle_bati', Numeric(10, 2), nullable=True),
    Column('nombre_pieces_principales', Integer, nullable=True),
)

population = Table(
    'population', metadata,
    Column('code', String(2), primary_key=True),
    Column('population', Integer, nullable=False),
)

poverty = Table(
    'poverty', metadata,
    Column('code', String(2), primary_key=True),
    Column('poverty_rate', Numeric(5,2), nullable=False),
)

# 4. Création de la base et des tables
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
metadata.create_all(engine)
print(f"Base SQLite créée : {DB_FILE}")

# 5. Chargement des CSV
# Transactions
print(f"Lecture et chargement de : {TX_CSV}")
df_tx = pd.read_csv(
    TX_CSV,
    parse_dates=['date_mutation'],
    dtype={'code_postal': str}
)
# Conversion colonne valeur_fonciere si nécessaire
if df_tx['valeur_fonciere'].dtype == object:
    df_tx['valeur_fonciere'] = (
        df_tx['valeur_fonciere']
            .str.replace(' ', '')
            .str.replace(',', '.', regex=False)
            .astype(float)
    )
df_tx.to_sql('transactions', engine, if_exists='append', index=False)
print("Table 'transactions' chargée.")

# Population
print(f"Lecture et chargement de : {POP_CSV}")
df_pop = pd.read_csv(POP_CSV, dtype={'code': str})
df_pop['code'] = df_pop['code'].str.zfill(2)
df_pop.to_sql('population', engine, if_exists='replace', index=False)
print("Table 'population' chargée (replace).")

# Pauvreté
print(f"Lecture et chargement de : {POV_CSV}")
df_pov = pd.read_csv(POV_CSV, dtype={'code': str})
df_pov['code'] = df_pov['code'].str.zfill(2)
df_pov.to_sql('poverty', engine, if_exists='append', index=False)
print("Table 'poverty' chargée.")

print("Chargement dans SQLite terminé.")
