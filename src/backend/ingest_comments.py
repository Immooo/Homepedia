import os

import pandas as pd

from src.backend.logging_setup import setup_logging

logger = setup_logging()


def main():
    RAW = os.path.join("data", "raw", "comments", "Hotel_Reviews.csv")
    OUT = os.path.join("data", "processed", "comments.csv")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    logger.info("Lecture brut commentaires : %s", RAW)
    # On charge les colonnes neg et pos
    df = pd.read_csv(
        RAW,
        usecols=["Negative_Review", "Positive_Review"],
        encoding="latin1",
        low_memory=False,
    )

    # Concaténer positif + négatif
    df["commentaire"] = (
        df["Positive_Review"].fillna("").str.strip()
        + " "
        + df["Negative_Review"].fillna("").str.strip()
    )

    # Filtrer lignes vides
    df = df[df["commentaire"].str.len() > 0].copy()

    # Conserver seulement la colonne de commentaire
    df = df[["commentaire"]]

    logger.info("Nombre de commentaires à ingérer : %d", len(df))

    # Écriture du CSV traité
    df.to_csv(OUT, index=False, encoding="utf-8")
    logger.info("Ingestion commentaires OK → %s", OUT)


if __name__ == "__main__":
    main()
