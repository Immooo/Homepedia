import sqlite3
import sys
from pathlib import Path

if len(sys.argv) != 2:
    print("Usage: python list_columns_table.py <table_name>")
    sys.exit(1)

table = sys.argv[1]
db_path = Path(__file__).resolve().parent.parent / "data" / "homepedia.db"
conn = sqlite3.connect(db_path)
cursor = conn.execute(f"PRAGMA table_info({table});")
print(f"Colonnes de la table `{table}` :")
for cid, name, col_type, notnull, dflt, pk in cursor:
    print(f" - {name} ({col_type})")
conn.close()
