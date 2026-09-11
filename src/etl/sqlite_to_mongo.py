import os
import sqlite3
from pymongo import MongoClient

DB_PATH = os.getenv("DB_PATH", "data/homepedia.db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "homepedia")

# Nom de la table SQLite à synchroniser
SQLITE_TABLE = "properties"  # 🔴 À adapter au vrai nom de ta table principale

def row_to_dict(cursor, row):
    """Transforme une ligne SQLite en dict Python générique."""
    columns = [desc[0] for desc in cursor.description]
    return {col: row[idx] for idx, col in enumerate(columns)}

def main():
    print(f"[INFO] SQLite path : {DB_PATH}")
    print(f"[INFO] Mongo URI   : {MONGO_URI}")
    print(f"[INFO] Mongo DB    : {MONGO_DB}")
    print(f"[INFO] Table       : {SQLITE_TABLE}")

    # 1) Connexion SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = lambda cursor, row: row_to_dict(cursor, row)
    cur = conn.cursor()

    # 2) Connexion Mongo
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[SQLITE_TABLE]

    # 3) Lecture des données SQLite
    print("[INFO] Lecture des données SQLite...")
    cur.execute(f"SELECT * FROM {SQLITE_TABLE}")
    rows = cur.fetchall()
    print(f"[INFO] Nombre de lignes lues : {len(rows)}")

    if not rows:
        print("[WARN] Aucune donnée à importer.")
        return

    # 4) Nettoyage / remplacement de la collection
    print("[INFO] Suppression de la collection Mongo existante (si elle existe)...")
    collection.drop()

    # 5) Insertion en masse dans Mongo
    print("[INFO] Insertion dans MongoDB...")
    collection.insert_many(rows)
    print(f"[INFO] Import terminé : {len(rows)} documents insérés dans '{SQLITE_TABLE}'.")

    conn.close()
    client.close()
    print("[INFO] Connexions fermées.")

if __name__ == "__main__":
    main()
