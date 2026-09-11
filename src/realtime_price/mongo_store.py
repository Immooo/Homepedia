from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.collection import Collection

from src.realtime_price.insee_scraper import PricePoint


def _bson_dt(dt: datetime) -> datetime:
    # Mongo stores naive datetimes as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None)


def get_client(uri: str) -> MongoClient:
    return MongoClient(uri)


def ensure_indexes(raw_col: Collection, obs_col: Collection, latest_col: Collection) -> None:
    raw_col.create_index([("run_id", ASCENDING)], unique=True, name="uniq_run_id")
    raw_col.create_index([("scraped_at", ASCENDING)], name="idx_scraped_at")

    # One observation per metric per scrape timestamp (idempotent)
    obs_col.create_index([("metric_uid", ASCENDING), ("scraped_at", ASCENDING)], unique=True, name="uniq_metric_scraped")
    obs_col.create_index([("scraped_at", ASCENDING)], name="idx_scraped_at")

    latest_col.create_index([("metric_uid", ASCENDING)], unique=True, name="uniq_metric_uid")
    latest_col.create_index([("last_seen_at", ASCENDING)], name="idx_last_seen_at")


def insert_raw_run(raw_col: Collection, run_id: str, source_url: str, scraped_at: datetime, raw_debug: Dict[str, Any]) -> None:
    raw_col.insert_one(
        {
            "run_id": run_id,
            "source_url": source_url,
            "scraped_at": _bson_dt(scraped_at),
            "raw_debug": raw_debug,
        }
    )


def upsert_observations(obs_col: Collection, points: List[PricePoint]) -> None:
    if not points:
        return

    ops: List[UpdateOne] = []
    for p in points:
        scraped_at = _bson_dt(p.scraped_at)
        key = {"metric_uid": p.metric_uid, "scraped_at": scraped_at}
        doc = {
            "metric_uid": p.metric_uid,
            "metric_name": p.metric_name,
            "geo": p.geo,
            "unit": p.unit,
            "period": p.period,
            "value": p.value,
            "source": p.source,
            "source_url": p.source_url,
            "scraped_at": scraped_at,
        }
        ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

    obs_col.bulk_write(ops, ordered=False)


def upsert_latest(latest_col: Collection, points: List[PricePoint]) -> None:
    for p in points:
        latest_col.update_one(
            {"metric_uid": p.metric_uid},
            {
                "$setOnInsert": {"first_seen_at": _bson_dt(p.scraped_at)},
                "$set": {
                    "metric_uid": p.metric_uid,
                    "metric_name": p.metric_name,
                    "geo": p.geo,
                    "unit": p.unit,
                    "period": p.period,
                    "value": p.value,
                    "source": p.source,
                    "source_url": p.source_url,
                    "last_seen_at": _bson_dt(p.scraped_at),
                    "scraped_at": _bson_dt(p.scraped_at),
                },
            },
            upsert=True,
        )
