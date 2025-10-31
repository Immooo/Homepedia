import os
import sqlite3
import pandas as pd
import pytest

DB_FILE = os.path.join("data", "homepedia.db")
PROCESSED_DIR = os.path.join("data", "processed")


# --- Helpers ------------------------------------------------------------
def csv_exists(name: str) -> str:
    path = os.path.join(PROCESSED_DIR, name)
    assert os.path.exists(path), f"{path} manquant"
    return path


def table_rowcount(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# --- Paramétrage des jeux de données -------------------------------------
CSV_TABLES = [
    ("population_dept.csv", "population"),
    ("poverty_dept.csv", "poverty"),
    ("unemployment_dept.csv", "unemployment"),
    ("income_dept.csv", "income"),
]


# -------------------------------------------------------------------------
@pytest.mark.parametrize("csv_name,table", CSV_TABLES)
def test_csv_present(csv_name, table):
    """Chaque CSV traité existe et n'est pas vide."""
    path = csv_exists(csv_name)
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    assert not df.empty, f"{csv_name} est vide"


@pytest.mark.parametrize("csv_name,table", CSV_TABLES)
def test_sqlite_table_created(csv_name, table):
    """Chaque table SQLite existe et contient au moins 95 lignes."""
    conn = sqlite3.connect(DB_FILE)
    try:
        n = table_rowcount(conn, table)
    except sqlite3.OperationalError:
        pytest.fail(f"Table {table} absente de {DB_FILE}")
    finally:
        conn.close()
    assert n >= 95, f"{table} ne contient que {n} lignes (<95)"


@pytest.mark.parametrize("csv_name,table", CSV_TABLES)
def test_no_null_values(csv_name, table):
    """Aucune colonne numérique clé ne doit contenir NULL."""
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()
    # on exclut la colonne 'code'
    numeric_cols = [c for c in df.columns if c != "code"]
    assert df[numeric_cols].notna().all().all(), f"{table} possède des NULL !"
