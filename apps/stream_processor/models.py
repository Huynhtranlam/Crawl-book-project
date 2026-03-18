from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class CleanProductRecord:
    product_id: str
    title: str
    price: Decimal | None
    currency: str
    product_url: str | None
    image_url: str | None
    source: str
    crawled_at: str
    raw_payload: dict[str, Any]

    def to_insert_tuple(self) -> tuple[Any, ...]:
        return (
            self.product_id,
            self.title,
            str(self.price) if self.price is not None else None,
            self.currency,
            self.product_url,
            self.image_url,
            self.source,
            self.crawled_at,
            self.raw_payload,
        )


@dataclass(frozen=True)
class InvalidRecord:
    reason: str
    payload: dict[str, Any]
