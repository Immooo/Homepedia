import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
)
from sqlalchemy.engine.url import URL

from backend.logging_setup import setup_logging

logger = setup_logging()

# 1. Charger les variables d'env
load_dotenv()

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
)

# 2. Créer l'engine et le metadata
engine = create_engine(db_url)
metadata = MetaData()

transactions = Table(
    "transactions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("date_mutation", Date, nullable=False),
    Column("nature_mutation", String(50), nullable=True),
    Column("valeur_fonciere", Numeric(12, 2), nullable=False),
    Column("code_postal", String(10), nullable=False),
    Column("commune", String(100), nullable=False),
    Column("type_local", String(50), nullable=True),
    Column("surface_reelle_bati", Numeric(10, 2), nullable=True),
    Column("nombre_pieces_principales", Integer, nullable=True),
)


def main():
    # 3. Créer la table si nécessaire
    metadata.create_all(engine)
    safe_url = db_url.set(password="***")
    logger.info("Table 'transactions' prête (DB: %s)", safe_url)

    # 4. Lire le CSV
    csv_path = os.path.join("data", "processed", "transactions_2024.csv")
    logger.info("Lecture CSV: %s", csv_path)
    df = pd.read_csv(csv_path, parse_dates=["date_mutation"])
    logger.info("%d lignes prêtes à être chargées", len(df))

    # 5. Charger avec to_sql (append + pas d'index)
    logger.info("Insertion en base (append) dans la table 'transactions'")
    df.to_sql("transactions", engine, if_exists="append", index=False)
    logger.info("✅ Import terminé.")


if __name__ == "__main__":
    main()
