from __future__ import annotations

import time
from typing import Any

import requests

from apps.crawler.config import MarketDataConfig
from apps.crawler.models import MarketEvent, normalize_klines, normalize_ticker


def fetch_normalized_batch(config: MarketDataConfig) -> list[MarketEvent]:
    if config.event_type == "ticker":
        payload = _request_json("/ticker/24hr", {"symbol": config.symbol}, config)
        return [normalize_ticker(payload, config)]

    payload = _request_json(
        "/klines",
        {
            "symbol": config.symbol,
            "interval": config.kline_interval,
            "limit": config.kline_limit,
        },
        config,
    )
    if not isinstance(payload, list):
        raise ValueError("Binance kline endpoint did not return a list payload.")

    return normalize_klines(payload, config)


def _request_json(
    path: str, params: dict[str, Any], config: MarketDataConfig
) -> dict[str, Any] | list[Any]:
    last_error: Exception | None = None

    for attempt in range(1, config.http_max_retries + 1):
        try:
            response = requests.get(
                f"{config.api_base_url}{path}",
                params=params,
                timeout=config.http_timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == config.http_max_retries:
                break
            time.sleep(config.http_backoff_seconds * attempt)

    raise RuntimeError(f"Failed to fetch market data after retries: {last_error}")
