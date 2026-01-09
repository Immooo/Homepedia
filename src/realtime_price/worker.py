from __future__ import annotations

import time
import uuid
from datetime import datetime, UTC

from src.realtime_price.config import RealtimePriceConfig
from src.realtime_price.insee_scraper import scrape_insee_housing_prices
from src.realtime_price import sqlite_store, mongo_store


def log(level: str, msg: str, **fields: object) -> None:
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "level": level.upper(),
        "msg": msg,
        **fields,
    }
    print(payload, flush=True)


def run_once(cfg: RealtimePriceConfig) -> None:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    points_count = 0
    errors_count = 0
    error_sample = None

    try:
        points, raw_debug = scrape_insee_housing_prices(
            cfg.source_url, timeout_seconds=cfg.request_timeout_seconds
        )
        points_count = len(points)

        # SQLite
        conn = sqlite_store.connect(cfg.sqlite_db_path)
        sqlite_store.ensure_schema(conn)
        for p in points:
            sqlite_store.upsert_latest(conn, p)
            sqlite_store.insert_history(conn, p)
        conn.commit()
        conn.close()

        # Mongo (homepedia_buffer)
        client = mongo_store.get_client(cfg.mongo_uri)
        db = client[cfg.mongo_db]
        raw_col = db[cfg.mongo_raw_collection]
        obs_col = db[cfg.mongo_obs_collection]
        latest_col = db[cfg.mongo_latest_collection]
        mongo_store.ensure_indexes(raw_col, obs_col, latest_col)

        mongo_store.insert_raw_run(
            raw_col, run_id, cfg.source_url, scraped_at=started_at, raw_debug=raw_debug
        )
        mongo_store.insert_observations(obs_col, points)
        mongo_store.upsert_latest(latest_col, points)

        client.close()

        log(
            "INFO",
            "realtime_price_scrape_ok",
            run_id=run_id,
            points_count=points_count,
            source_url=cfg.source_url,
        )

    except Exception as e:
        errors_count += 1
        error_sample = repr(e)
        log(
            "ERROR",
            "realtime_price_scrape_failed",
            run_id=run_id,
            error=error_sample,
            source_url=cfg.source_url,
        )

    finally:
        finished_at = datetime.now(UTC)
        try:
            conn = sqlite_store.connect(cfg.sqlite_db_path)
            sqlite_store.ensure_schema(conn)
            sqlite_store.insert_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                source_url=cfg.source_url,
                points_count=points_count,
                errors_count=errors_count,
                error_sample=error_sample,
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log(
                "ERROR",
                "realtime_price_run_persist_failed",
                run_id=run_id,
                error=repr(e),
            )

    log(
        "INFO",
        "realtime_price_run_done",
        run_id=run_id,
        interval_seconds=cfg.poll_interval_seconds,
    )


def main() -> None:
    cfg = RealtimePriceConfig()
    log(
        "INFO",
        "realtime_price_worker_start",
        interval_seconds=cfg.poll_interval_seconds,
        source_url=cfg.source_url,
    )

    while True:
        run_once(cfg)
        time.sleep(max(5, cfg.poll_interval_seconds))


if __name__ == "__main__":
    main()
