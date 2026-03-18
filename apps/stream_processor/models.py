from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class CleanTickerRecord:
    event_id: str
    source: str
    symbol: str
    event_time: str
    ingest_time: str
    last_price: Decimal
    price_change_24h: Decimal
    price_change_pct_24h: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    open_price_24h: Decimal
    high_price_24h: Decimal
    low_price_24h: Decimal
    trade_count_24h: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class CleanKlineRecord:
    event_id: str
    source: str
    symbol: str
    interval: str
    open_time: str
    close_time: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_asset_volume: Decimal
    trade_count: int
    is_closed: bool
    ingest_time: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class InvalidRecord:
    reason: str
    payload: dict[str, Any]
