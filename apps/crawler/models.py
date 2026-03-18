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
    product_id = (
        raw_item.get("id")
        or raw_item.get("product_id")
        or raw_item.get("sku")
        or raw_item.get("asin")
    )
    title = raw_item.get("title") or raw_item.get("name")

    if product_id is None or not title:
        raise ValueError("Record is missing required identifier or title fields.")

    price = _normalize_price(raw_item.get("price"))
    currency = (
        raw_item.get("currency")
        or raw_item.get("currency_code")
        or config.default_currency
    )
    product_url = raw_item.get("url") or raw_item.get("product_url") or raw_item.get("link")
    image_url = _extract_image_url(raw_item)

    return ProductRecord(
        product_id=str(product_id),
        title=str(title).strip(),
        price=price,
        currency=str(currency).strip(),
        product_url=str(product_url).strip() if product_url else None,
        image_url=image_url,
        source=config.source_name,
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
