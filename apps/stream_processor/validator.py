from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from apps.stream_processor.models import CleanProductRecord, InvalidRecord


REQUIRED_FIELDS = ("product_id", "title", "currency", "source", "crawled_at")


def validate_and_clean(payload: dict[str, Any]) -> CleanProductRecord | InvalidRecord:
    missing_fields = [field for field in REQUIRED_FIELDS if not _has_value(payload.get(field))]
    if missing_fields:
        return InvalidRecord(
            reason=f"Missing required fields: {', '.join(missing_fields)}",
            payload=payload,
        )

    price = _clean_price(payload.get("price"))
    if payload.get("price") not in (None, "") and price is None:
        return InvalidRecord(reason="Invalid price value.", payload=payload)

    return CleanProductRecord(
        product_id=str(payload["product_id"]).strip(),
        title=_clean_text(payload["title"]),
        price=price,
        currency=_clean_text(payload["currency"]).upper(),
        product_url=_clean_optional_text(payload.get("product_url")),
        image_url=_clean_optional_text(payload.get("image_url")),
        source=_clean_text(payload["source"]),
        crawled_at=_clean_text(payload["crawled_at"]),
        raw_payload=payload,
    )


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _clean_text(value: Any) -> str:
    return " ".join(str(value).strip().split())


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = _clean_text(value)
    return cleaned or None


def _clean_price(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
