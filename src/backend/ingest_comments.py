import os
import pandas as pd

def main():
    RAW  = os.path.join("data", "raw", "comments", "Hotel_Reviews.csv")
    OUT  = os.path.join("data", "processed", "comments.csv")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    print(f"Lecture brut commentaires : {RAW}")
    # On charge les colonnes neg et pos
    df = pd.read_csv(
        RAW,
        usecols=["Negative_Review", "Positive_Review"],
        encoding="latin1",
        low_memory=False
    )

    # Concaténer positif + négatif
    df["commentaire"] = (
        df["Positive_Review"].fillna("").str.strip() + " " +
        df["Negative_Review"].fillna("").str.strip()
    )

    # Filtrer lignes vides
    df = df[df["commentaire"].str.len() > 0].copy()

    # Conserver seulement la colonne de commentaire
    df = df[["commentaire"]]

    print(f"Nombre de commentaires à ingérer : {len(df)}")

    # Écriture du CSV traité
    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Ingestion commentaires OK → {OUT}")

if __name__ == "__main__":
    main()