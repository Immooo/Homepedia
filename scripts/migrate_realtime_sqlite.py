"""Copy the small real-time subsystem out of the large analytical SQLite DB."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REALTIME_TABLES = (
    "realtime_price_latest",
    "realtime_price_history",
    "realtime_price_runs",
)


def migrate(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source_path) as source, sqlite3.connect(target_path) as target:
        target.execute("PRAGMA journal_mode = WAL")
        for table in REALTIME_TABLES:
            schema_row = source.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if schema_row is None:
                continue
            target.execute(f"DROP TABLE IF EXISTS {table}")
            target.execute(schema_row[0])
            rows = source.execute(f"SELECT * FROM {table}").fetchall()
            if rows:
                placeholders = ",".join("?" for _ in rows[0])
                target.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

        indexes = source.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type='index' AND tbl_name IN (?, ?, ?) AND sql IS NOT NULL
            """,
            REALTIME_TABLES,
        ).fetchall()
        for (index_sql,) in indexes:
            target.execute(index_sql)
        target.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path("data/homepedia.db"))
    parser.add_argument("--target", type=Path, default=Path("data/realtime_price.db"))
    args = parser.parse_args()
    migrate(args.source, args.target)
    print(f"Migration temps réel terminée : {args.target}")
