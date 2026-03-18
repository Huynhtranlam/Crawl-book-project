from __future__ import annotations

from typing import Any

import requests

from apps.crawler.config import CrawlerConfig
from apps.crawler.models import ProductRecord, normalize_product


def fetch_normalized_batch(config: CrawlerConfig) -> list[ProductRecord]:
    response = requests.get(config.source_url, timeout=config.http_timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    raw_items = _extract_items(payload, config)

    normalized_records: list[ProductRecord] = []
    for raw_item in raw_items[: config.batch_limit]:
        if not isinstance(raw_item, dict):
            continue
        normalized_records.append(normalize_product(raw_item, config))

    return normalized_records


def _extract_items(payload: Any, config: CrawlerConfig) -> list[Any]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if config.source_items_key:
            items = payload.get(config.source_items_key)
            if isinstance(items, list):
                return items
            raise ValueError(
                f"Configured items key '{config.source_items_key}' did not resolve to a list."
            )

        # TODO: Replace this generic fallback with source-specific extraction once the real
        # target source structure is defined in project inputs.
        for key in ("products", "items", "results", "data"):
            items = payload.get(key)
            if isinstance(items, list):
                return items

    raise ValueError("Could not extract a list of product items from the source payload.")
