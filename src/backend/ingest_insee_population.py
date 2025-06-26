import os
import pandas as pd

def main():
    # 1. Chemins
    RAW = os.path.join("data", "raw", "insee", "population_dept.csv")
    OUT = os.path.join("data", "processed", "population_dept.csv")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    print(f"Lecture du brut INSEE : {RAW}")
    # Lecture en précisant le séparateur semicolon
    df = pd.read_csv(RAW, sep=";", dtype=str)

    # 2. Renommages
    # Colonne département
    if "DEP" in df.columns:
        df = df.rename(columns={"DEP": "code"})
    # Colonne population totale
    if "PTOT" in df.columns:
        df = df.rename(columns={"PTOT": "population"})
    elif "pop_totale" in df.columns:
        df = df.rename(columns={"pop_totale": "population"})

    # 3. Sélection des colonnes
    df = df[["code", "population"]].copy()

    # 4. Nettoyage
    # veille à 2 caractères pour le code
    df["code"] = df["code"].str.zfill(2)
    df["population"] = df["population"].str.replace(" ", "").astype(int)

    # 5. Export
    print(f"Écriture du CSV INSEE traité : {OUT}")
    df.to_csv(OUT, index=False)
    print("Ingestion INSEE terminée.")

if __name__ == "__main__":
    main()
