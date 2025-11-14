import os

import pandas as pd
from tinydb import TinyDB

from backend.logging_setup import setup_logging

logger = setup_logging()


def main():
    # Chemins
    RAW_CSV = os.path.join("data", "raw", "comments", "Hotel_Reviews.csv")
    TDB = os.path.join("data", "processed", "comments.json")
    os.makedirs(os.path.dirname(TDB), exist_ok=True)

    logger.info("Lecture brut commentaires : %s", RAW_CSV)
    df = pd.read_csv(
        RAW_CSV,
        usecols=["Positive_Review", "Negative_Review"],
        encoding="latin1",
        low_memory=False,
    )

    # Concaténer
    df["commentaire"] = (
        df["Positive_Review"].fillna("").str.strip()
        + " "
        + df["Negative_Review"].fillna("").str.strip()
    )
    df = df[df["commentaire"].str.len() > 0].copy()
    df = df[["commentaire"]]

    # Charger TinyDB
    logger.info("Écriture TinyDB JSON → %s", TDB)
    db = TinyDB(TDB)
    db.truncate()  # vider l'ancienne base

    # Insérer chaque avis comme doc TinyDB
    for text in df["commentaire"].tolist():
        db.insert({"commentaire": text})

    logger.info("Ingestion TinyDB terminée : %d documents.", len(db))


if __name__ == "__main__":
    main()
