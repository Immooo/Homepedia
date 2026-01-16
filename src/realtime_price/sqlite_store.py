from __future__ import annotations

import sqlite3
from datetime import datetime, UTC

from src.realtime_price.insee_scraper import PricePoint


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS realtime_price_latest (
  metric_uid TEXT PRIMARY KEY,
  metric_name TEXT NOT NULL,
  geo TEXT NOT NULL,
  unit TEXT NOT NULL,

  period TEXT NOT NULL,
  value REAL NOT NULL,

  source TEXT NOT NULL,
  source_url TEXT NOT NULL,

  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  scraped_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS realtime_price_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  metric_uid TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  geo TEXT NOT NULL,
  unit TEXT NOT NULL,

  period TEXT NOT NULL,
  value REAL NOT NULL,

  source TEXT NOT NULL,
  source_url TEXT NOT NULL,

  scraped_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rph_metric_scraped ON realtime_price_history(metric_uid, scraped_at);
CREATE INDEX IF NOT EXISTS idx_rph_scraped ON realtime_price_history(scraped_at);

-- Runs table: new schema for fresh DBs.
-- Existing DBs will be migrated by ensure_schema().
CREATE TABLE IF NOT EXISTS realtime_price_runs (
  run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,

  duration_ms INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'ok',

  source_url TEXT NOT NULL,

  points_count INTEGER NOT NULL,
  points_valid_count INTEGER NOT NULL DEFAULT 0,
  stored_latest_count INTEGER NOT NULL DEFAULT 0,
  stored_history_count INTEGER NOT NULL DEFAULT 0,
  skipped_history_count INTEGER NOT NULL DEFAULT 0,
  dq_errors_count INTEGER NOT NULL DEFAULT 0,

  errors_count INTEGER NOT NULL,
  error_sample TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_rpr_started ON realtime_price_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_rpr_finished ON realtime_price_runs(finished_at);
"""


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, col_def: str) -> None:
    # col_def example: "duration_ms INTEGER NOT NULL DEFAULT 0"
    col_name = col_def.strip().split()[0]
    cols = _table_columns(conn, table)
    if col_name not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Ensures base tables + applies lightweight migrations on existing DBs.
    """
    conn.executescript(SCHEMA_SQL)

    # Migrate realtime_price_runs if it was created with the old schema.
    try:
        _ensure_column(
            conn, "realtime_price_runs", "duration_ms INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(conn, "realtime_price_runs", "status TEXT NOT NULL DEFAULT 'ok'")
        _ensure_column(
            conn, "realtime_price_runs", "points_valid_count INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(
            conn,
            "realtime_price_runs",
            "stored_latest_count INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "realtime_price_runs",
            "stored_history_count INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "realtime_price_runs",
            "skipped_history_count INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn, "realtime_price_runs", "dq_errors_count INTEGER NOT NULL DEFAULT 0"
        )
    except Exception:
        # If table doesn't exist for some reason, SCHEMA_SQL already creates it.
        pass

    # Ensure indexes (idempotent)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rpr_started ON realtime_price_runs(started_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rpr_finished ON realtime_price_runs(finished_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rph_metric_scraped ON realtime_price_history(metric_uid, scraped_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rph_scraped ON realtime_price_history(scraped_at)"
    )

    conn.commit()


def upsert_latest(conn: sqlite3.Connection, p: PricePoint) -> bool:
    """
    Upsert latest and return True if this point is NEW or CHANGED (period/value changed),
    False if unchanged (same period + same value).
    """
    now = p.scraped_at
    cur = conn.execute(
        "SELECT metric_uid, first_seen_at, period, value FROM realtime_price_latest WHERE metric_uid = ?",
        (p.metric_uid,),
    )
    row = cur.fetchone()

    first_seen = (
        now
        if row is None
        else datetime.fromisoformat(row["first_seen_at"].replace("Z", "+00:00"))
    )

    changed = (
        row is None
        or str(row["period"]) != str(p.period)
        or float(row["value"]) != float(p.value)
    )

    conn.execute(
        """
        INSERT INTO realtime_price_latest(
          metric_uid, metric_name, geo, unit, period, value,
          source, source_url, first_seen_at, last_seen_at, scraped_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(metric_uid) DO UPDATE SET
          metric_name=excluded.metric_name,
          geo=excluded.geo,
          unit=excluded.unit,
          period=excluded.period,
          value=excluded.value,
          source=excluded.source,
          source_url=excluded.source_url,
          last_seen_at=excluded.last_seen_at,
          scraped_at=excluded.scraped_at
        """,
        (
            p.metric_uid,
            p.metric_name,
            p.geo,
            p.unit,
            p.period,
            p.value,
            p.source,
            p.source_url,
            _iso(first_seen),
            _iso(now),
            _iso(now),
        ),
    )

    return changed


def insert_history(conn: sqlite3.Connection, p: PricePoint) -> None:
    conn.execute(
        """
        INSERT INTO realtime_price_history(
          metric_uid, metric_name, geo, unit, period, value, source, source_url, scraped_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            p.metric_uid,
            p.metric_name,
            p.geo,
            p.unit,
            p.period,
            p.value,
            p.source,
            p.source_url,
            _iso(p.scraped_at),
        ),
    )


def insert_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    source_url: str,
    points_count: int,
    points_valid_count: int,
    stored_latest_count: int,
    stored_history_count: int,
    skipped_history_count: int,
    dq_errors_count: int,
    errors_count: int,
    error_sample: str | None,
    status: str,
) -> None:
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)

    conn.execute(
        """
        INSERT INTO realtime_price_runs(
          run_id, started_at, finished_at, duration_ms, status,
          source_url,
          points_count, points_valid_count,
          stored_latest_count, stored_history_count, skipped_history_count,
          dq_errors_count,
          errors_count, error_sample
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            _iso(started_at),
            _iso(finished_at),
            duration_ms,
            status,
            source_url,
            points_count,
            points_valid_count,
            stored_latest_count,
            stored_history_count,
            skipped_history_count,
            dq_errors_count,
            errors_count,
            error_sample,
        ),
    )
