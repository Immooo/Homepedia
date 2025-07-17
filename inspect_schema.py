# inspect_schema.py
import sqlite3
import pandas as pd
from pathlib import Path

# 1️⃣ Chemin vers la base
db_path = Path(__file__).parent / "data" / "homepedia.db"

# 2️⃣ Connexion
conn = sqlite3.connect(db_path)

# 3️⃣ Lister les tables
tables = pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
    conn
)
print("Tables dans la DB :", tables["name"].tolist())

# 4️⃣ Pour chaque table, afficher PRAGMA table_info
for table in tables["name"]:
    info = pd.read_sql_query(f"PRAGMA table_info('{table}');", conn)
    print(f"\nSchéma de `{table}` :")
    print(info[["cid","name","type","notnull","dflt_value","pk"]])

conn.close()
