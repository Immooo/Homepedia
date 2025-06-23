import os
import pandas as pd
import sqlite3

def main():
    # 1. Chemins
    RAW     = os.path.join("data", "raw", "insee", "base_cc_comparateur.csv")
    OUT_CSV = os.path.join("data", "processed", "poverty_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    # 2. Lecture du brut INSEE
    df = pd.read_csv(RAW, sep=";", dtype=str)

    # 3. Sélection et renommage
    df = df[["CODGEO", "TP6021"]] .rename(columns={
        "CODGEO": "geo",
        "TP6021": "poverty_rate"
    })

    # 4. Extraction du code département
    df["code"] = df["geo"].str[:2]

    # 5. Conversion en float + suppression des non numériques
    df["poverty_rate"] = (
        df["poverty_rate"]
        .str.replace(",", ".")
        .pipe(pd.to_numeric, errors="coerce")
    )
    df = df.dropna(subset=["poverty_rate"])

    # 6. Agrégation par département (médiane)
    df_dept = (
        df.groupby("code", as_index=False)["poverty_rate"]
          .median()
    )

    # 7. Sauvegarde CSV
    df_dept.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"CSV pauvreté écrit → {OUT_CSV}")

    # 8. Insertion dans SQLite
    conn = sqlite3.connect(DB_PATH)
    df_dept.to_sql("poverty", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Table 'poverty' créée dans SQLite")

if __name__ == "__main__":
    main()
