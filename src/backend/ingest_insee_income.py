# File: src/backend/ingest_insee_income.py

import os
import sqlite3

import pandas as pd

from backend.logging_setup import setup_logging

logger = setup_logging()


def extract_dept(code_insee: str) -> str:
    """
    Extrait le code départemental à partir du code INSEE commune.
    Gestion particulière :
    - DOM-TOM commencent par 97 ou 98 → 3 caractères code.
    - Corse (20) → 2A ou 2B selon le 3ème chiffre.
    """
    code5 = str(code_insee).zfill(5)
    if code5.startswith(("97", "98")):
        return code5[:3]
    elif code5[:2] == "20":
        return "2A" if int(code5[2]) < 2 else "2B"
    return code5[:2]


def main():
    RAW = os.path.join("data", "raw", "insee", "DS_FILOSOFI_CC_2021_data.csv")
    OUT_CSV = os.path.join("data", "processed", "income_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    logger.info("Lecture du fichier brut : %s", RAW)
    df = pd.read_csv(RAW, dtype=str, sep=";")

    logger.info("Filtrage des données pertinentes")
    df_filt = df[
        (df["FILOSOFI_MEASURE"] == "MED_SL")
        & (df["UNIT_MEASURE"] == "EUR_YR")
        & (df["CONF_STATUS"] == "F")
        & (df["TIME_PERIOD"] == "2021")
        & df["OBS_VALUE"].notna()
        & (df["OBS_VALUE"] != "")
    ].copy()

    df_filt["dept"] = df_filt["GEO"].apply(extract_dept)

    logger.info("Conversion des valeurs en float")
    df_filt["income_median"] = df_filt["OBS_VALUE"].str.replace(",", ".").astype(float)

    logger.info("Agrégation par département (médiane)")
    df_dept = (
        df_filt.groupby("dept", as_index=False)["income_median"]
        .median()
        .rename({"dept": "code"}, axis=1)
    )

    logger.info("Écriture du CSV intermédiaire : %s", OUT_CSV)
    df_dept.to_csv(OUT_CSV, index=False, encoding="utf-8")

    logger.info("Écriture dans la base SQLite : %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    df_dept.to_sql("income", conn, if_exists="replace", index=False)
    conn.close()

    logger.info("✅ Table 'income' créée et remplie dans SQLite")


if __name__ == "__main__":
    main()
