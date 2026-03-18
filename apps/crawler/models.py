from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from apps.crawler.config import MarketDataConfig


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    event_type: str
    source: str
    symbol: str
    event_time: str
    ingest_time: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_ticker(raw_item: dict[str, Any], config: MarketDataConfig) -> MarketEvent:
    symbol = _require_non_empty_text(raw_item.get("symbol"), "symbol")
    event_time_ms = raw_item.get("closeTime")
    event_time = _require_timestamp(event_time_ms, "closeTime")
    ingest_time = datetime.now(timezone.utc).isoformat()

    payload = {
        "event_id": f"ticker-{symbol}-{event_time_ms}",
        "event_type": "ticker",
        "source": config.source_name,
        "symbol": symbol,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "last_price": _require_decimal_text(raw_item.get("lastPrice"), "lastPrice"),
        "price_change_24h": _require_decimal_text(
            raw_item.get("priceChange"), "priceChange"
        ),
        "price_change_pct_24h": _require_decimal_text(
            raw_item.get("priceChangePercent"), "priceChangePercent"
        ),
        "volume_24h": _require_decimal_text(raw_item.get("volume"), "volume"),
        "quote_volume_24h": _require_decimal_text(
            raw_item.get("quoteVolume"), "quoteVolume"
        ),
        "open_price_24h": _require_decimal_text(raw_item.get("openPrice"), "openPrice"),
        "high_price_24h": _require_decimal_text(raw_item.get("highPrice"), "highPrice"),
        "low_price_24h": _require_decimal_text(raw_item.get("lowPrice"), "lowPrice"),
        "trade_count_24h": _require_int(raw_item.get("count"), "count"),
        "raw_payload": raw_item,
    }
    return MarketEvent(
        event_id=payload["event_id"],
        event_type="ticker",
        source=config.source_name,
        symbol=symbol,
        event_time=event_time,
        ingest_time=ingest_time,
        payload=payload,
    )


def normalize_klines(
    raw_items: list[list[Any]], config: MarketDataConfig
) -> list[MarketEvent]:
    normalized_events: list[MarketEvent] = []

    for raw_item in raw_items:
        if not isinstance(raw_item, list) or len(raw_item) < 11:
            raise ValueError("Invalid Binance kline payload shape.")

        open_time_ms = raw_item[0]
        close_time_ms = raw_item[6]
        ingest_time = datetime.now(timezone.utc).isoformat()
        payload = {
            "event_id": f"kline-{config.symbol}-{config.kline_interval}-{open_time_ms}",
            "event_type": "kline",
            "source": config.source_name,
            "symbol": config.symbol,
            "interval": config.kline_interval,
            "event_time": _require_timestamp(close_time_ms, "close_time"),
            "ingest_time": ingest_time,
            "open_time": _require_timestamp(open_time_ms, "open_time"),
            "close_time": _require_timestamp(close_time_ms, "close_time"),
            "open_price": _require_decimal_text(raw_item[1], "open_price"),
            "high_price": _require_decimal_text(raw_item[2], "high_price"),
            "low_price": _require_decimal_text(raw_item[3], "low_price"),
            "close_price": _require_decimal_text(raw_item[4], "close_price"),
            "volume": _require_decimal_text(raw_item[5], "volume"),
            "quote_asset_volume": _require_decimal_text(
                raw_item[7], "quote_asset_volume"
            ),
            "trade_count": _require_int(raw_item[8], "trade_count"),
            "is_closed": True,
            "raw_payload": raw_item,
        }
        normalized_events.append(
            MarketEvent(
                event_id=payload["event_id"],
                event_type="kline",
                source=config.source_name,
                symbol=config.symbol,
                event_time=payload["event_time"],
                ingest_time=ingest_time,
                payload=payload,
            )
        )

    return normalized_events


def _require_non_empty_text(value: Any, field_name: str) -> str:
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"Missing required field: {field_name}.")
    return cleaned


def _require_decimal_text(value: Any, field_name: str) -> str:
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Invalid decimal field: {field_name}.") from None


def _require_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer field: {field_name}.") from None


def _require_timestamp(value: Any, field_name: str) -> str:
    try:
        timestamp_ms = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid timestamp field: {field_name}.") from None

    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
