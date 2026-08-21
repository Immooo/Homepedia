# File: src/backend/ingest_insee_population.py

import os

import pandas as pd

from src.backend.logging_setup import setup_logging

logger = setup_logging()


def main():
    # 1. Chemins
    RAW = os.path.join("data", "raw", "insee", "population_dept.csv")
    OUT = os.path.join("data", "processed", "population_dept.csv")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    logger.info("Lecture du brut INSEE : %s", RAW)
    # Lecture en précisant le séparateur semicolon
    df = pd.read_csv(RAW, sep=";", dtype=str)

    # 2. Renommages
    if "DEP" in df.columns:
        logger.info("Renommage de la colonne 'DEP' en 'code'")
        df = df.rename(columns={"DEP": "code"})
    if "PTOT" in df.columns:
        logger.info("Renommage de la colonne 'PTOT' en 'population'")
        df = df.rename(columns={"PTOT": "population"})
    elif "pop_totale" in df.columns:
        logger.info("Renommage de la colonne 'pop_totale' en 'population'")
        df = df.rename(columns={"pop_totale": "population"})

    # 3. Sélection des colonnes
    df = df[["code", "population"]].copy()

    # 4. Nettoyage
    logger.info("Nettoyage des codes et conversion de la population en entier")
    df["code"] = df["code"].str.zfill(2)
    df["population"] = df["population"].str.replace(" ", "").astype(int)

    # 5. Export
    logger.info("Écriture du CSV INSEE traité : %s", OUT)
    df.to_csv(OUT, index=False)
    logger.info("✅ Ingestion INSEE terminée avec %d lignes.", len(df))


if __name__ == "__main__":
    main()
