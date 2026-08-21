import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    return int(v)


@dataclass(frozen=True)
class RealtimePriceConfig:
    # Scraping target (INSEE “Informations rapides” containing HTML tables)
    source_url: str = os.getenv(
        "INSEE_SOURCE_URL", "https://www.insee.fr/fr/statistiques/8669035"
    )
    request_timeout_seconds: int = _env_int("REALTIME_REQUEST_TIMEOUT_SECONDS", 20)

    # Polling
    poll_interval_seconds: int = _env_int("POLL_INTERVAL_SECONDS", 300)

    # Storage
    sqlite_db_path: str = os.getenv(
        "DB_PATH", os.getenv("SQLITE_DB_PATH", "/app/data/homepedia.db")
    )

    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://homepedia-mongo:27017")
    mongo_db: str = os.getenv("MONGO_DB", "homepedia_buffer")

    mongo_raw_collection: str = os.getenv(
        "MONGO_PRICE_RAW_COLLECTION", "realtime_price_raw_runs"
    )
    mongo_obs_collection: str = os.getenv(
        "MONGO_PRICE_OBS_COLLECTION", "realtime_price_observations"
    )
    mongo_latest_collection: str = os.getenv(
        "MONGO_PRICE_LATEST_COLLECTION", "realtime_price_latest"
    )
