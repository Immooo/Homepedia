from __future__ import annotations

import math
import time
import uuid
from datetime import datetime, UTC

from src.realtime_price.config import RealtimePriceConfig
from src.realtime_price.insee_scraper import PricePoint, scrape_insee_housing_prices
from src.realtime_price import mongo_store, sqlite_store


def log(level: str, msg: str, **fields: object) -> None:
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "level": level.upper(),
        "msg": msg,
        **fields,
    }
    print(payload, flush=True)


def _is_finite_number(x: float) -> bool:
    return isinstance(x, int | float) and not (math.isnan(x) or math.isinf(x))


def _dq_validate_point(p: PricePoint) -> list[str]:
    """
    Data Quality checks (simple but robust).
    On veut éviter d'écrire n'importe quoi en base en cas de parsing cassé.
    """
    errs: list[str] = []

    if not p.metric_uid or not p.metric_uid.strip():
        errs.append("metric_uid_empty")

    if not p.period or not p.period.strip():
        errs.append("period_empty")
    else:
        # attendu: YYYY-T[1-4]
        # (ton scraper normalise déjà, donc ici c'est surtout une barrière anti-bug)
        import re

        if not re.match(r"^\d{4}-T[1-4]$", p.period.strip()):
            errs.append(f"period_invalid:{p.period}")

    if not _is_finite_number(float(p.value)):
        errs.append("value_not_finite")
        return errs  # inutile d'aller plus loin

    v = float(p.value)

    # Plages larges pour éviter les faux positifs tout en détectant les erreurs grossières
    if p.unit == "index_base" or p.metric_uid.startswith("insee_indices"):
        if v <= 0 or v > 2000:
            errs.append(f"value_out_of_range_index:{v}")
    elif p.unit == "pct" or p.metric_uid.startswith("insee_yoy"):
        if v < -200 or v > 200:
            errs.append(f"value_out_of_range_pct:{v}")

    if not p.source_url or not p.source_url.strip():
        errs.append("source_url_empty")

    return errs


def run_once(cfg: RealtimePriceConfig) -> None:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    points_count = 0
    points_valid_count = 0

    dq_errors_count = 0
    stored_latest_count = 0
    stored_history_count = 0
    skipped_history_count = 0

    errors_count = 0
    error_sample = None
    status = "ok"

    try:
        points, raw_debug = scrape_insee_housing_prices(
            cfg.source_url, timeout_seconds=cfg.request_timeout_seconds
        )
        points_count = len(points)

        # DQ filtering
        valid_points: list[PricePoint] = []
        for p in points:
            errs = _dq_validate_point(p)
            if errs:
                dq_errors_count += 1
                if error_sample is None:
                    error_sample = f"dq:{p.metric_uid}:{'|'.join(errs)}"
                continue
            valid_points.append(p)

        points_valid_count = len(valid_points)
        if dq_errors_count > 0:
            status = "warn"

        # SQLite
        conn = sqlite_store.connect(cfg.sqlite_db_path)
        try:
            sqlite_store.ensure_schema(conn)

            for p in valid_points:
                changed = sqlite_store.upsert_latest(conn, p)
                stored_latest_count += 1

                # Dédup: on n'écrit l'history QUE si (period/value) change
                if changed:
                    sqlite_store.insert_history(conn, p)
                    stored_history_count += 1
                else:
                    skipped_history_count += 1

            conn.commit()
        finally:
            conn.close()

        # Mongo (homepedia_buffer)
        client = mongo_store.get_client(cfg.mongo_uri)
        try:
            db = client[cfg.mongo_db]
            raw_col = db[cfg.mongo_raw_collection]
            obs_col = db[cfg.mongo_obs_collection]
            latest_col = db[cfg.mongo_latest_collection]
            mongo_store.ensure_indexes(raw_col, obs_col, latest_col)

            mongo_store.insert_raw_run(
                raw_col,
                run_id,
                cfg.source_url,
                scraped_at=started_at,
                raw_debug=raw_debug,
            )

            # On garde l'historique Mongo "observations" par run (utile debug),
            # mais on n'y met que les points validés DQ.
            mongo_store.upsert_observations(obs_col, valid_points)
            mongo_store.upsert_latest(latest_col, valid_points)
        finally:
            client.close()

        log(
            "INFO",
            "realtime_price_scrape_ok",
            run_id=run_id,
            source_url=cfg.source_url,
            points_count=points_count,
            points_valid_count=points_valid_count,
            dq_errors_count=dq_errors_count,
            stored_latest_count=stored_latest_count,
            stored_history_count=stored_history_count,
            skipped_history_count=skipped_history_count,
            status=status,
        )

    except Exception as e:
        errors_count += 1
        error_sample = repr(e)
        status = "error"
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
            try:
                sqlite_store.ensure_schema(conn)
                sqlite_store.insert_run(
                    conn,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    source_url=cfg.source_url,
                    points_count=points_count,
                    points_valid_count=points_valid_count,
                    stored_latest_count=stored_latest_count,
                    stored_history_count=stored_history_count,
                    skipped_history_count=skipped_history_count,
                    dq_errors_count=dq_errors_count,
                    errors_count=errors_count,
                    error_sample=error_sample,
                    status=status,
                )
                conn.commit()
            finally:
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
        status=status,
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
