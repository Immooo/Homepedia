import os
import pandas as pd
import sqlite3

def main():
    # 1. Chemins vers vos fichiers CSV INSEE
    RAW_COMMUNES = os.path.join("data", "raw", "insee", "communes.csv")
    RAW_REGIONS = os.path.join("data", "raw", "insee", "regions.csv")
    DB_PATH = os.path.join("data", "homepedia.db")

    # 2. Lecture & nettoyage des régions
    df_reg = pd.read_csv(RAW_REGIONS, dtype=str, sep=";")
    df_reg = df_reg[["code", "libelle"]].rename(
        columns={"code": "code_region", "libelle": "nom_region"}
    )

    # 3. Lecture & nettoyage des communes
    df_com = pd.read_csv(RAW_COMMUNES, dtype=str, sep=";")
    if "DEP" in df_com.columns:
        df_com = df_com.rename(columns={"DEP": "code_departement"})
    df_com = df_com.rename(columns={
        "code": "code_commune",
        "libelle": "nom_commune",
        # "code_departement" reste tel quel,
        # "code_region" doit exister dans votre CSV
    })[["code_commune", "nom_commune", "code_departement", "code_region"]]

    # 4. Enregistrement en SQLite
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    df_reg.to_sql("regions", conn, if_exists="replace", index=False)
    df_com.to_sql("communes", conn, if_exists="replace", index=False)
    conn.close()

    print(f"✅ Tables 'regions' ({len(df_reg)} lignes) et 'communes' ({len(df_com)} lignes) créées.")

if __name__ == "__main__":
    main()
