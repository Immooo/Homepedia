import os
import pandas as pd
from tinydb import TinyDB, where

def main():
    # Chemins
    RAW_CSV = os.path.join("data", "raw", "comments", "Hotel_Reviews.csv")
    TDB     = os.path.join("data", "processed", "comments.json")
    os.makedirs(os.path.dirname(TDB), exist_ok=True)

    print(f"Lecture brut commentaires : {RAW_CSV}")
    df = pd.read_csv(
        RAW_CSV,
        usecols=["Positive_Review", "Negative_Review"],
        encoding="latin1",
        low_memory=False
    )
    # Concaténer
    df["commentaire"] = (
        df["Positive_Review"].fillna("").str.strip()
        + " " +
        df["Negative_Review"].fillna("").str.strip()
    )
    df = df[df["commentaire"].str.len() > 0].copy()
    df = df[["commentaire"]]

    # Charger TinyDB
    print(f"Écriture TinyDB JSON → {TDB}")
    db = TinyDB(TDB)
    db.truncate()  # vider l'ancienne base

    # Insérer chaque avis comme doc TinyDB
    for text in df["commentaire"].tolist():
        db.insert({"commentaire": text})

    print(f"Ingestion TinyDB terminée : {len(db)} documents.")

if __name__ == "__main__":
    main()