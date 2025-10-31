import os
import pandas as pd
import sqlite3

from backend.logging_setup import setup_logging
logger = setup_logging()


def main():
    RAW_XLS = os.path.join("data", "raw", "insee", "ts_chomage_dept_T1_2025.xls")
    OUT_CSV = os.path.join("data", "processed", "unemployment_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    logger.info("Lecture Excel : %s", RAW_XLS)
    # On saute les 3 premières lignes (donc header ligne 4, index 3)
    df = pd.read_excel(
        RAW_XLS,
        sheet_name=0,
        header=3,  # <---- ATTENTION : header=3, pas skiprows
        dtype={"Code": "str"},
        engine="xlrd",
    )

    # On garde juste Code, Libellé et T1_2025
    expected_cols = {"Code", "Libellé", "T1_2025"}
    if not expected_cols.issubset(set(df.columns)):
        missing = expected_cols - set(df.columns)
        logger.error("Colonnes attendues non trouvées: %s. Colonnes présentes: %s", missing, list(df.columns))
        raise ValueError("Colonnes attendues non trouvées !")

    df = df[["Code", "Libellé", "T1_2025"]].copy()
    df = df.rename(columns={"Code": "code", "Libellé": "libelle", "T1_2025": "taux_chomage"})

    # Sécurise le type (au cas où)
    df["taux_chomage"] = pd.to_numeric(df["taux_chomage"], errors="coerce")
    df["code"] = df["code"].astype(str).str.zfill(2)  # Conserve 01, 02, 2A, 2B, etc.

    # Filtre les codes vides ou anormaux
    # (regroupe correctement l'alternative avec une non-capturing group)
    df = df[df["code"].str.match(r"^(?:\d{2}|\d{1,2}[A-B])$")]

    # Save to CSV
    logger.info("Écriture du CSV chômage : %s", OUT_CSV)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    logger.info("CSV écrit avec %d lignes.", len(df))

    # Save to SQLite
    logger.info("Insertion dans la base SQLite : %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("unemployment", conn, if_exists="replace", index=False)
    conn.close()
    logger.info("✅ Table 'unemployment' créée dans SQLite avec %d lignes.", len(df))


if __name__ == "__main__":
    main()
