from __future__ import annotations

from src.realtime_price.config import RealtimePriceConfig
from src.realtime_price.worker import run_once


def main() -> None:
    cfg = RealtimePriceConfig()
    run_once(cfg)


if __name__ == "__main__":
    main()
