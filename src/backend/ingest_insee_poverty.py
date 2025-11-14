# File: src/backend/ingest_insee_poverty.py

import os
import sqlite3

import pandas as pd

from backend.logging_setup import setup_logging

logger = setup_logging()


def main():
    # 1. Chemins
    RAW = os.path.join("data", "raw", "insee", "base_cc_comparateur.csv")
    OUT_CSV = os.path.join("data", "processed", "poverty_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    # 2. Lecture du brut INSEE
    logger.info("Lecture du fichier brut INSEE : %s", RAW)
    df = pd.read_csv(RAW, sep=";", dtype=str)

    # 3. Sélection et renommage
    logger.info("Sélection des colonnes CODGEO et TP6021 (taux de pauvreté)")
    df = df[["CODGEO", "TP6021"]].rename(
        columns={"CODGEO": "geo", "TP6021": "poverty_rate"}
    )

    # 4. Extraction du code département
    logger.info("Extraction du code département à partir de la colonne GEO")
    df["code"] = df["geo"].str[:2]

    # 5. Conversion en float + suppression des non numériques
    logger.info(
        "Conversion du taux de pauvreté en float et suppression des valeurs non numériques"
    )
    df["poverty_rate"] = (
        df["poverty_rate"].str.replace(",", ".").pipe(pd.to_numeric, errors="coerce")
    )
    df = df.dropna(subset=["poverty_rate"])

    # 6. Agrégation par département (médiane)
    logger.info("Agrégation par département (médiane)")
    df_dept = df.groupby("code", as_index=False)["poverty_rate"].median()

    # 7. Sauvegarde CSV
    logger.info("Écriture du CSV de sortie : %s", OUT_CSV)
    df_dept.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # 8. Insertion dans SQLite
    logger.info("Insertion dans la base SQLite : %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    df_dept.to_sql("poverty", conn, if_exists="replace", index=False)
    conn.close()

    logger.info("✅ Table 'poverty' créée avec %d lignes.", len(df_dept))


if __name__ == "__main__":
    main()
