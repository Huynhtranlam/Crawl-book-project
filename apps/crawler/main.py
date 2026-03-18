from __future__ import annotations

import json

from apps.crawler.config import MarketDataConfig
from apps.crawler.source import fetch_normalized_batch


def main() -> int:
    config = MarketDataConfig.from_env()
    records = fetch_normalized_batch(config)
    print(json.dumps([record.to_dict() for record in records], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
