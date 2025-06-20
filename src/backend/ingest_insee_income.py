import os
import pandas as pd
import sqlite3

def main():
    RAW = os.path.join("data", "raw", "insee", "DS_FILOSOFI_CC_2021_data.csv")
    OUT_CSV = os.path.join("data", "processed", "income_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    # Lecture
    df = pd.read_csv(RAW, dtype=str, sep=";")  # change sep=";" si nécessaire, sinon "," par défaut

    # Filtrage des départements (codes à 2 ou 3 chiffres), mesure D1_SL (revenu médian), unité euro/an
    df = df[
        (df["FILOSOFI_MEASURE"] == "D1_SL") &
        (df["UNIT_MEASURE"] == "EUR_YR") &
        (df["CONF_STATUS"] == "C") &
        (df["TIME_PERIOD"] == "2021")
    ].copy()

    # Correction types et renommage
    df["code"] = df["GEO"].str.zfill(2)   # les codes INSEE département sont sur 2 ou 3 caractères, ici on complète avec des zéros
    df["income_median"] = df["OBS_VALUE"].str.replace(",", ".").astype(float)
    df = df[["code", "income_median"]]

    # Regrouper si jamais il y a plusieurs lignes par département (moyenne, mais normalement il n'y en a qu'une)
    df = df.groupby("code", as_index=False)["income_median"].mean()

    # Écriture CSV
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"CSV revenu médian écrit → {OUT_CSV}")

    # Insertion SQLite
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("income", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Table 'income' créée dans SQLite")

if __name__ == "__main__":
    main()