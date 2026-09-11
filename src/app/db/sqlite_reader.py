from __future__ import annotations

import sqlite3
import time
from pathlib import Path


def connect_readonly(
    db_path: str, attempts: int = 6, retry_delay_seconds: float = 0.5
) -> sqlite3.Connection:
    """Open SQLite for dashboard reads, tolerating brief Docker bind-mount delays."""
    resolved = Path(db_path).resolve()
    last_error: sqlite3.OperationalError | None = None

    for attempt in range(attempts):
        try:
            connection = sqlite3.connect(
                f"{resolved.as_uri()}?mode=ro",
                uri=True,
                timeout=30,
            )
            connection.execute("PRAGMA query_only = ON")
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("SELECT 1").fetchone()
            return connection
        except sqlite3.OperationalError as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(retry_delay_seconds * (attempt + 1))

    raise sqlite3.OperationalError(
        f"Base SQLite temporairement indisponible après {attempts} tentatives : {resolved}"
    ) from last_error
