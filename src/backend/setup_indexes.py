import sqlite3
from pathlib import Path

def has_col(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())

def safe_index(cur, table: str, column: str, suffix: str):
    if has_col(cur.connection, table, column):
        idx_name = f"idx_{table}_{suffix}"
        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column});")

def create_indexes(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    # ---- DVF transactions ----
    safe_index(c, "transactions", "date_mutation", "date")
    safe_index(c, "transactions", "commune", "commune")
    # composite : date_mutation + type_local + valeur_fonciere
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_tx_date_type_price
        ON transactions(date_mutation, type_local, valeur_fonciere);
    """)

    # ---- Indicateurs INSEE ----
    for tbl in ("unemployment", "income", "population", "poverty"):
        safe_index(c, tbl, "code", "code")

    # ---- Agrégats ----
    safe_index(c, "spark_dept_analysis", "dept", "dept")
    safe_index(c, "region_analysis", "code_region", "code_region")

    conn.commit()
    print("✅ Indexes créés / vérifiés sans erreur.")

if __name__ == "__main__":
    db = Path(__file__).resolve().parents[2] / "data" / "homepedia.db"
    create_indexes(sqlite3.connect(db))
