from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from apps.crawler.config import CrawlerConfig


@dataclass(frozen=True)
class ProductRecord:
    product_id: str
    title: str
    price: str | None
    currency: str
    product_url: str | None
    image_url: str | None
    source: str
    crawled_at: str
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_product(raw_item: dict[str, Any], config: CrawlerConfig) -> ProductRecord:
    product_id_value = (
        raw_item.get("id")
        or raw_item.get("product_id")
        or raw_item.get("sku")
        or raw_item.get("asin")
    )
    title_value = raw_item.get("title") or raw_item.get("name")

    if product_id_value is None or title_value is None:
        raise ValueError("Record is missing required identifier or title fields.")

    price = _normalize_price(raw_item.get("price"))
    currency_value = (
        raw_item.get("currency")
        or raw_item.get("currency_code")
        or config.default_currency
    )
    product_url = raw_item.get("url") or raw_item.get("product_url") or raw_item.get("link")
    image_url = _extract_image_url(raw_item)
    product_id = _require_non_empty_text(product_id_value, "product identifier")
    title = _require_non_empty_text(title_value, "title")
    currency = _require_non_empty_text(currency_value, "currency")
    source_name = _require_non_empty_text(config.source_name, "source name")

    return ProductRecord(
        product_id=product_id,
        title=title,
        price=price,
        currency=currency,
        product_url=str(product_url).strip() if product_url else None,
        image_url=image_url,
        source=source_name,
        crawled_at=datetime.now(timezone.utc).isoformat(),
        raw=raw_item,
    )


def _normalize_price(value: Any) -> str | None:
    if value is None or value == "":
        return None

    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def _extract_image_url(raw_item: dict[str, Any]) -> str | None:
    for key in ("image", "thumbnail", "image_url"):
        value = raw_item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    images = raw_item.get("images")
    if isinstance(images, list):
        for item in images:
            if isinstance(item, str) and item.strip():
                return item.strip()

    return None


def _require_non_empty_text(value: Any, field_name: str) -> str:
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"Record is missing a valid {field_name}.")
    return cleaned
