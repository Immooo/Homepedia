import sqlite3

import pytest

from src.app.db.sqlite_reader import connect_readonly


def test_dashboard_connection_is_read_only(tmp_path):
    db_path = tmp_path / "dashboard.db"
    with sqlite3.connect(db_path) as writer:
        writer.execute("CREATE TABLE sample (value INTEGER)")
        writer.execute("INSERT INTO sample VALUES (42)")

    reader = connect_readonly(str(db_path), attempts=1)
    try:
        assert reader.execute("SELECT value FROM sample").fetchone()[0] == 42
        with pytest.raises(sqlite3.OperationalError):
            reader.execute("INSERT INTO sample VALUES (43)")
    finally:
        reader.close()


def test_dashboard_connection_fails_cleanly_for_missing_database(tmp_path):
    with pytest.raises(sqlite3.OperationalError, match="temporairement indisponible"):
        connect_readonly(str(tmp_path / "missing.db"), attempts=1)
