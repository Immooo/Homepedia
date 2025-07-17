import sqlite3
from pathlib import Path

def has_col(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())

def safe_index(cur, table: str, column: str, suffix: str):
    if has_col(cur.connection, table, column):
        idx_name = f"idx_{table}_{suffix}"
        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")

def create_indexes(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    # DVF
    safe_index(c, "transactions", "date_mutation", "date")
    safe_index(c, "transactions", "commune", "commune")

    # Indicateurs INSEE
    for tbl in ("unemployment", "income", "population", "poverty"):
        safe_index(c, tbl, "code", "code")

    # Agrégat Spark par département
    safe_index(c, "spark_dept_analysis", "dept", "dept")

    # Agrégat par région (table déjà là)
    safe_index(c, "region_analysis", "code_region", "code_region")

    conn.commit()
    print("✅ Indexes créés / vérifiés sans erreur.")

if __name__ == "__main__":
    db = Path(__file__).resolve().parents[2] / "data" / "homepedia.db"
    create_indexes(sqlite3.connect(db))