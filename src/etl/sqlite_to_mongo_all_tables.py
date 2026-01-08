import os
import sqlite3
from pymongo import MongoClient

DB_PATH = os.getenv("DB_PATH", "data/homepedia.db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "homepedia_buffer")
PROTECTED_COLLECTIONS = {"system.indexes"}


def fetch_table_names(conn: sqlite3.Connection) -> list[str]:
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


def table_rows_generator(conn: sqlite3.Connection, table_name: str):
    """
    Générateur qui retourne les lignes d'une table
    sous forme de dicts {colonne: valeur}.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cur.description]

    for row in cur:
        yield dict(zip(columns, row, strict=False))


def main():
    print(f"[INFO] SQLite path : {DB_PATH}")
    print(f"[INFO] Mongo URI   : {MONGO_URI}")
    print(f"[INFO] Mongo DB    : {MONGO_DB}")

    conn = sqlite3.connect(DB_PATH)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]

    tables = fetch_table_names(conn)
    print(f"[INFO] Tables trouvées dans SQLite : {tables}")

    if not tables:
        print("[WARN] Aucune table trouvée dans la base SQLite.")
        conn.close()
        client.close()
        return

    existing_collections = set(db.list_collection_names())
    sqlite_tables = set(tables)
    orphans = sorted(
        c
        for c in existing_collections
        if c not in sqlite_tables and c not in PROTECTED_COLLECTIONS
    )

    if orphans:
        print(
            f"[INFO] Collections orphelines à supprimer (absentes de SQLite) : {orphans}"
        )
        for c in orphans:
            db[c].drop()
            print(f"[INFO]  - Dropped orphan collection: {c}")
    else:
        print("[INFO] Aucune collection orpheline à supprimer.")

    for table in tables:
        print(f"\n[INFO] Traitement de la table '{table}'...")
        collection = db[table]

        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cur.fetchone()[0]
        print(f"[INFO]  - Nombre de lignes dans SQLite : {total_rows}")
        print("[INFO]  - Suppression de la collection Mongo existante (si présente)...")
        collection.drop()

        if total_rows == 0:
            print("[INFO]  - Table vide, collection Mongo supprimée (mirror).")
            continue

        batch_size = 5000
        batch = []
        inserted = 0

        print("[INFO]  - Insertion dans Mongo par batch...")
        for row_dict in table_rows_generator(conn, table):
            batch.append(row_dict)
            if len(batch) >= batch_size:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
                print(f"[INFO]    > {inserted}/{total_rows} documents insérés...")
                batch.clear()

        if batch:
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)

        print(
            f"[INFO]  - Import terminé pour '{table}' : {inserted} documents insérés."
        )

    conn.close()
    client.close()
    print("\n[INFO] Migration SQLite → Mongo terminée (mirror exact).")


if __name__ == "__main__":
    main()
