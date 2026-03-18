from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from apps.stream_processor.models import (
    CleanKlineRecord,
    CleanTickerRecord,
    InvalidRecord,
)


def validate_and_clean(
    payload: dict[str, Any], expected_event_type: str
) -> CleanTickerRecord | CleanKlineRecord | InvalidRecord:
    event_type = str(payload.get("event_type", "")).strip().lower()
    if event_type != expected_event_type:
        return InvalidRecord(
            reason=(
                f"Unexpected event_type '{event_type or 'missing'}'. "
                f"Expected '{expected_event_type}'."
            ),
            payload=payload,
        )

    if expected_event_type == "ticker":
        return _validate_ticker(payload)
    return _validate_kline(payload)


def _validate_ticker(payload: dict[str, Any]) -> CleanTickerRecord | InvalidRecord:
    required_fields = (
        "event_id",
        "source",
        "symbol",
        "event_time",
        "ingest_time",
        "last_price",
        "price_change_24h",
        "price_change_pct_24h",
        "volume_24h",
        "quote_volume_24h",
        "open_price_24h",
        "high_price_24h",
        "low_price_24h",
        "trade_count_24h",
    )
    missing_fields = [field for field in required_fields if not _has_value(payload.get(field))]
    if missing_fields:
        return InvalidRecord(
            reason=f"Missing required ticker fields: {', '.join(missing_fields)}",
            payload=payload,
        )

    event_time = _parse_timestamp(payload.get("event_time"))
    ingest_time = _parse_timestamp(payload.get("ingest_time"))
    last_price = _parse_decimal(payload.get("last_price"))
    open_price = _parse_decimal(payload.get("open_price_24h"))
    high_price = _parse_decimal(payload.get("high_price_24h"))
    low_price = _parse_decimal(payload.get("low_price_24h"))
    volume = _parse_decimal(payload.get("volume_24h"))
    quote_volume = _parse_decimal(payload.get("quote_volume_24h"))
    price_change = _parse_decimal(payload.get("price_change_24h"))
    price_change_pct = _parse_decimal(payload.get("price_change_pct_24h"))
    trade_count = _parse_int(payload.get("trade_count_24h"))

    if None in (
        event_time,
        ingest_time,
        last_price,
        open_price,
        high_price,
        low_price,
        volume,
        quote_volume,
        price_change,
        price_change_pct,
        trade_count,
    ):
        return InvalidRecord(reason="Ticker payload contains invalid typed values.", payload=payload)

    if any(value < 0 for value in (last_price, open_price, high_price, low_price, volume, quote_volume)):
        return InvalidRecord(reason="Ticker payload contains negative values.", payload=payload)
    if high_price < low_price:
        return InvalidRecord(reason="Ticker high_price_24h is lower than low_price_24h.", payload=payload)
    if trade_count < 0:
        return InvalidRecord(reason="Ticker trade_count_24h must be zero or greater.", payload=payload)

    return CleanTickerRecord(
        event_id=str(payload["event_id"]).strip(),
        source=str(payload["source"]).strip(),
        symbol=str(payload["symbol"]).strip().upper(),
        event_time=str(payload["event_time"]).strip(),
        ingest_time=str(payload["ingest_time"]).strip(),
        last_price=last_price,
        price_change_24h=price_change,
        price_change_pct_24h=price_change_pct,
        volume_24h=volume,
        quote_volume_24h=quote_volume,
        open_price_24h=open_price,
        high_price_24h=high_price,
        low_price_24h=low_price,
        trade_count_24h=trade_count,
        raw_payload=payload,
    )


def _validate_kline(payload: dict[str, Any]) -> CleanKlineRecord | InvalidRecord:
    required_fields = (
        "event_id",
        "source",
        "symbol",
        "interval",
        "open_time",
        "close_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_asset_volume",
        "trade_count",
        "is_closed",
        "ingest_time",
    )
    missing_fields = [field for field in required_fields if not _has_value(payload.get(field))]
    if missing_fields:
        return InvalidRecord(
            reason=f"Missing required kline fields: {', '.join(missing_fields)}",
            payload=payload,
        )

    open_time = _parse_timestamp(payload.get("open_time"))
    close_time = _parse_timestamp(payload.get("close_time"))
    ingest_time = _parse_timestamp(payload.get("ingest_time"))
    open_price = _parse_decimal(payload.get("open_price"))
    high_price = _parse_decimal(payload.get("high_price"))
    low_price = _parse_decimal(payload.get("low_price"))
    close_price = _parse_decimal(payload.get("close_price"))
    volume = _parse_decimal(payload.get("volume"))
    quote_asset_volume = _parse_decimal(payload.get("quote_asset_volume"))
    trade_count = _parse_int(payload.get("trade_count"))

    if None in (
        open_time,
        close_time,
        ingest_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_asset_volume,
        trade_count,
    ):
        return InvalidRecord(reason="Kline payload contains invalid typed values.", payload=payload)

    if close_time < open_time:
        return InvalidRecord(reason="Kline close_time is earlier than open_time.", payload=payload)
    if any(
        value < 0
        for value in (open_price, high_price, low_price, close_price, volume, quote_asset_volume)
    ):
        return InvalidRecord(reason="Kline payload contains negative values.", payload=payload)
    if high_price < max(open_price, close_price, low_price):
        return InvalidRecord(reason="Kline high_price is inconsistent with candle values.", payload=payload)
    if low_price > min(open_price, close_price, high_price):
        return InvalidRecord(reason="Kline low_price is inconsistent with candle values.", payload=payload)
    if trade_count < 0:
        return InvalidRecord(reason="Kline trade_count must be zero or greater.", payload=payload)

    return CleanKlineRecord(
        event_id=str(payload["event_id"]).strip(),
        source=str(payload["source"]).strip(),
        symbol=str(payload["symbol"]).strip().upper(),
        interval=str(payload["interval"]).strip(),
        open_time=str(payload["open_time"]).strip(),
        close_time=str(payload["close_time"]).strip(),
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        quote_asset_volume=quote_asset_volume,
        trade_count=trade_count,
        is_closed=bool(payload["is_closed"]),
        ingest_time=str(payload["ingest_time"]).strip(),
        raw_payload=payload,
    )


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _parse_decimal(value: Any) -> Decimal | None:
    if not _has_value(value):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    if not _has_value(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if not _has_value(value):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
