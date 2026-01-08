import sqlite3
from pathlib import Path

from src.backend.logging_setup import setup_logging

logger = setup_logging()


def has_col(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Vérifie si une colonne existe dans une table SQLite."""
    cur = conn.execute(f"PRAGMA table_info({table});")
    exists = any(row[1] == column for row in cur.fetchall())
    logger.debug("Vérification colonne '%s' dans '%s' : %s", column, table, exists)
    return exists


def safe_index(cur, table: str, column: str, suffix: str):
    """Crée un index si la colonne existe."""
    if has_col(cur.connection, table, column):
        idx_name = f"idx_{table}_{suffix}"
        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column});")
        logger.info("Index créé/vérifié : %s sur %s(%s)", idx_name, table, column)
    else:
        logger.warning(
            "Colonne '%s' absente dans la table '%s' — index ignoré.", column, table
        )


def create_indexes(conn: sqlite3.Connection) -> None:
    """Crée tous les index nécessaires sur la base SQLite."""
    logger.info("Création des index dans la base SQLite.")
    c = conn.cursor()

    # ---- DVF transactions ----
    safe_index(c, "transactions", "date_mutation", "date")
    safe_index(c, "transactions", "commune", "commune")

    # composite : date_mutation + type_local + valeur_fonciere
    logger.info(
        "Création de l’index composite transactions(date_mutation, type_local, valeur_fonciere)"
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tx_date_type_price
        ON transactions(date_mutation, type_local, valeur_fonciere);
        """
    )

    # ---- Indicateurs INSEE ----
    for tbl in ("unemployment", "income", "population", "poverty"):
        safe_index(c, tbl, "code", "code")

    # ---- Agrégats ----
    safe_index(c, "spark_dept_analysis", "dept", "dept")
    safe_index(c, "region_analysis", "code_region", "code_region")

    conn.commit()
    logger.info("✅ Indexes créés / vérifiés sans erreur.")


if __name__ == "__main__":
    db = Path(__file__).resolve().parents[2] / "data" / "homepedia.db"
    logger.info("Connexion à la base SQLite : %s", db)
    with sqlite3.connect(db) as conn:
        create_indexes(conn)
