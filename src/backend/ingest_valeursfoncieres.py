import os
import pandas as pd
from src.backend.logging_setup import setup_logging

logger = setup_logging()

RAW_DIR = os.path.join("data", "raw", "dvf2024")
OUTPUT_DIR = os.path.join("data", "processed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

INPUT_FILE = os.path.join(RAW_DIR, "valeursfoncieres-2024.txt")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "transactions_2024.csv")

TARGET_COLS = [
    "date_mutation",
    "nature_mutation",
    "valeur_fonciere",
    "code_postal",
    "commune",
    "type_local",
    "surface_reelle_bati",
    "nombre_pieces_principales",
]


def main():
    logger.info("Lecture du fichier brut : %s", INPUT_FILE)

    df = pd.read_csv(INPUT_FILE, sep="|", low_memory=False)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    missing = set(TARGET_COLS) - set(df.columns)
    if missing:
        logger.error("Colonnes manquantes après normalisation : %s", missing)
        raise KeyError(f"Colonnes manquantes après normalisation : {missing}")

    df["code_postal"] = (
        df["code_postal"].astype(str).str.replace(".0", "", regex=False).str.zfill(5)
    )

    logger.info("Filtrage et nettoyage des données DVF 2024")

    df = df[TARGET_COLS]

    df["date_mutation"] = pd.to_datetime(
        df["date_mutation"], dayfirst=True, errors="coerce"
    )

    df = df.drop_duplicates()
    df = df.dropna(subset=["date_mutation", "valeur_fonciere", "code_postal"])

    logger.info("Écriture du CSV nettoyé : %s", OUTPUT_FILE)

    df.to_csv(OUTPUT_FILE, index=False)

    logger.info("✅ Ingestion DVF terminée avec %d lignes.", len(df))


if __name__ == "__main__":
    main()
