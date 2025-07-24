import os
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, Numeric, MetaData, Table
)
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
import pandas as pd

# 1. Charger les variables d'env
load_dotenv()

db_url = URL.create(
    drivername='postgresql+psycopg2',
    username=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
)

# 2. Créer l'engine et le metadata
engine = create_engine(db_url)
metadata = MetaData()

transactions = Table(
    'transactions', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('date_mutation', Date, nullable=False),
    Column('nature_mutation', String(50), nullable=True),
    Column('valeur_fonciere', Numeric(12, 2), nullable=False),
    Column('code_postal', String(10), nullable=False),
    Column('commune', String(100), nullable=False),
    Column('type_local', String(50), nullable=True),
    Column('surface_reelle_bati', Numeric(10, 2), nullable=True),
    Column('nombre_pieces_principales', Integer, nullable=True),
)

def main():
    # 3. Créer la table si nécessaire
    metadata.create_all(engine)
    print("Table 'transactions' OK")

    # 4. Lire le CSV
    df = pd.read_csv(
        os.path.join('data', 'processed', 'transactions_2024.csv'),
        parse_dates=['date_mutation']
    )
    print(f"{len(df)} lignes prêtes à être chargées")

    # 5. Charger avec to_sql (append + pas d'index)
    df.to_sql('transactions', engine, if_exists='append', index=False)
    print("Import terminé.")

if __name__ == '__main__':
    main()