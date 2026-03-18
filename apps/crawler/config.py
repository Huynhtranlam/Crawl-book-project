from __future__ import annotations

import os
from dataclasses import dataclass

ALLOWED_KLINE_INTERVALS = (
    "1m",
    "5m",
    "15m",
    "1h",
    "4h",
    "1d",
    "1w",
)


@dataclass(frozen=True)
class MarketDataConfig:
    api_base_url: str
    source_name: str
    symbol: str
    event_type: str
    kline_interval: str
    kline_intervals: tuple[str, ...]
    kline_limit: int
    http_timeout_seconds: int
    http_max_retries: int
    http_backoff_seconds: int

    @classmethod
    def from_env(cls) -> "MarketDataConfig":
        api_base_url = os.getenv(
            "MARKET_DATA_API_BASE_URL", "https://api.binance.com/api/v3"
        ).strip()
        symbol = os.getenv("MARKET_SYMBOL", "BTCUSDT").strip().upper()
        event_type = os.getenv("MARKET_DATA_EVENT_TYPE", "ticker").strip().lower()

        if not api_base_url:
            raise ValueError("MARKET_DATA_API_BASE_URL is required.")
        if not symbol:
            raise ValueError("MARKET_SYMBOL is required.")
        if event_type not in {"ticker", "kline"}:
            raise ValueError("MARKET_DATA_EVENT_TYPE must be either 'ticker' or 'kline'.")

        kline_interval = os.getenv("MARKET_KLINE_INTERVAL", "5m").strip() or "5m"
        kline_intervals = _parse_kline_intervals(
            os.getenv("MARKET_KLINE_INTERVALS", kline_interval)
        )

        return cls(
            api_base_url=api_base_url.rstrip("/"),
            source_name=os.getenv("MARKET_SOURCE_NAME", "binance").strip() or "binance",
            symbol=symbol,
            event_type=event_type,
            kline_interval=kline_interval,
            kline_intervals=kline_intervals,
            kline_limit=int(os.getenv("MARKET_KLINE_LIMIT", "500")),
            http_timeout_seconds=int(os.getenv("MARKET_HTTP_TIMEOUT_SECONDS", "30")),
            http_max_retries=int(os.getenv("MARKET_HTTP_MAX_RETRIES", "3")),
            http_backoff_seconds=int(os.getenv("MARKET_HTTP_BACKOFF_SECONDS", "2")),
        )


def _parse_kline_intervals(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ("5m",)

    intervals = tuple(
        interval.strip()
        for interval in raw_value.split(",")
        if interval.strip()
    )
    if not intervals:
        raise ValueError("MARKET_KLINE_INTERVALS must include at least one interval.")

    invalid_intervals = sorted(set(intervals) - set(ALLOWED_KLINE_INTERVALS))
    if invalid_intervals:
        raise ValueError(
            "Unsupported MARKET_KLINE_INTERVALS values: "
            + ", ".join(invalid_intervals)
        )

    return intervals
