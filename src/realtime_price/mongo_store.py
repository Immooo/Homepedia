from __future__ import annotations

from datetime import datetime, UTC
from typing import Any

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from src.realtime_price.insee_scraper import PricePoint


def _bson_dt(dt: datetime) -> datetime:
    # Mongo stores naive datetimes as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    return dt.replace(tzinfo=None)


def get_client(uri: str) -> MongoClient:
    return MongoClient(uri)


def ensure_indexes(
    raw_col: Collection, obs_col: Collection, latest_col: Collection
) -> None:
    raw_col.create_index([("run_id", ASCENDING)], unique=True, name="uniq_run_id")
    raw_col.create_index([("scraped_at", ASCENDING)], name="idx_scraped_at")

    obs_col.create_index(
        [("metric_uid", ASCENDING), ("scraped_at", ASCENDING)],
        name="idx_metric_scraped",
    )
    obs_col.create_index([("scraped_at", ASCENDING)], name="idx_scraped_at")

    latest_col.create_index(
        [("metric_uid", ASCENDING)], unique=True, name="uniq_metric_uid"
    )
    latest_col.create_index([("last_seen_at", ASCENDING)], name="idx_last_seen_at")


def insert_raw_run(
    raw_col: Collection,
    run_id: str,
    source_url: str,
    scraped_at: datetime,
    raw_debug: dict[str, Any],
) -> None:
    raw_col.insert_one(
        {
            "run_id": run_id,
            "source_url": source_url,
            "scraped_at": _bson_dt(scraped_at),
            "raw_debug": raw_debug,
        }
    )


def insert_observations(obs_col: Collection, points: list[PricePoint]) -> None:
    if not points:
        return
    docs = []
    for p in points:
        docs.append(
            {
                "metric_uid": p.metric_uid,
                "metric_name": p.metric_name,
                "geo": p.geo,
                "unit": p.unit,
                "period": p.period,
                "value": p.value,
                "source": p.source,
                "source_url": p.source_url,
                "scraped_at": _bson_dt(p.scraped_at),
            }
        )
    obs_col.insert_many(docs, ordered=False)


def upsert_latest(latest_col: Collection, points: list[PricePoint]) -> None:
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
