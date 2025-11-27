import os
import sqlite3
from pymongo import MongoClient

DB_PATH = os.getenv("DB_PATH", "data/homepedia.db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "homepedia")


def fetch_table_names(conn):
    """Retourne la liste des tables utilisateur (hors sqlite_*)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
        """
    )
    rows = cur.fetchall()
    return [r[0] for r in rows]


def table_rows_generator(conn, table_name):
    """
    Générateur qui retourne les lignes d'une table
    sous forme de dicts {colonne: valeur}.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cur.description]

    for row in cur:
        # Ici on utilise bien les noms de colonnes comme clés
        yield dict(zip(columns, row))


def main():
    print(f"[INFO] SQLite path : {DB_PATH}")
    print(f"[INFO] Mongo URI   : {MONGO_URI}")
    print(f"[INFO] Mongo DB    : {MONGO_DB}")

    # 1) Connexion SQLite
    conn = sqlite3.connect(DB_PATH)

    # 2) Connexion Mongo
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 3) Récupérer toutes les tables
    tables = fetch_table_names(conn)
    print(f"[INFO] Tables trouvées dans SQLite : {tables}")

    if not tables:
        print("[WARN] Aucune table trouvée dans la base SQLite.")
        conn.close()
        client.close()
        return

    # 4) Pour chaque table, migrer les données vers Mongo
    for table in tables:
        print(f"\n[INFO] Traitement de la table '{table}'...")

        collection = db[table]

        # a) Compter le nombre de lignes dans SQLite
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cur.fetchone()[0]
        print(f"[INFO]  - Nombre de lignes dans SQLite : {total_rows}")

        if total_rows == 0:
            print("[INFO]  - Table vide, on saute.")
            continue

        # b) Drop de la collection existante
        print("[INFO]  - Suppression de la collection Mongo existante (si présente)...")
        collection.drop()

        # c) Insertion par batch pour éviter de tout charger en RAM
        batch_size = 5000
        batch = []
        inserted = 0

        print("[INFO]  - Insertion dans Mongo par batch...")
        for row_dict in table_rows_generator(conn, table):
            batch.append(row_dict)
            if len(batch) >= batch_size:
                collection.insert_many(batch)
                inserted += len(batch)
                print(f"[INFO]    > {inserted}/{total_rows} documents insérés...")
                batch.clear()

        # Dernier batch restant
        if batch:
            collection.insert_many(batch)
            inserted += len(batch)

        print(f"[INFO]  - Import terminé pour '{table}' : {inserted} documents insérés.")

    # 5) Fermeture
    conn.close()
    client.close()
    print("\n[INFO] Migration SQLite → Mongo terminée pour toutes les tables.")


if __name__ == "__main__":
    main()
