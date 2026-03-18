from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CrawlerConfig:
    source_url: str
    source_items_key: str | None
    source_name: str
    batch_limit: int
    http_timeout_seconds: int
    default_currency: str

    @classmethod
    def from_env(cls) -> "CrawlerConfig":
        source_url = os.getenv("CRAWLER_SOURCE_URL", "").strip()
        if not source_url:
            raise ValueError(
                "CRAWLER_SOURCE_URL is required. "
                "TODO: set the real target source URL once it is specified."
            )

        source_items_key = os.getenv("CRAWLER_SOURCE_ITEMS_KEY", "").strip() or None

        return cls(
            source_url=source_url,
            source_items_key=source_items_key,
            source_name=os.getenv("CRAWLER_SOURCE_NAME", "unspecified-source").strip(),
            batch_limit=int(os.getenv("CRAWLER_BATCH_LIMIT", "10")),
            http_timeout_seconds=int(os.getenv("CRAWLER_HTTP_TIMEOUT_SECONDS", "30")),
            default_currency=os.getenv("CRAWLER_DEFAULT_CURRENCY", "USD").strip() or "USD",
        )
