from __future__ import annotations

import unittest
from decimal import Decimal

from apps.stream_processor.models import CleanProductRecord, InvalidRecord
from apps.stream_processor.validator import validate_and_clean


class ValidateAndCleanTests(unittest.TestCase):
    def test_validate_and_clean_returns_clean_record(self) -> None:
        payload = {
            "product_id": "sku-1",
            "title": "  Sample   Product ",
            "price": "12.50",
            "currency": "usd",
            "product_url": " https://example.com/product/sku-1 ",
            "image_url": " https://example.com/product/sku-1.jpg ",
            "source": " dummyjson ",
            "crawled_at": "2026-03-18T00:00:00Z",
        }

        record = validate_and_clean(payload)

        self.assertIsInstance(record, CleanProductRecord)
        assert isinstance(record, CleanProductRecord)
        self.assertEqual("Sample Product", record.title)
        self.assertEqual(Decimal("12.50"), record.price)
        self.assertEqual("USD", record.currency)
        self.assertEqual("https://example.com/product/sku-1", record.product_url)

    def test_validate_and_clean_rejects_missing_required_fields(self) -> None:
        payload = {
            "product_id": "sku-1",
            "title": "",
            "currency": "USD",
            "source": "dummyjson",
            "crawled_at": "2026-03-18T00:00:00Z",
        }

        record = validate_and_clean(payload)

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertIn("title", record.reason)

    def test_validate_and_clean_rejects_invalid_price(self) -> None:
        payload = {
            "product_id": "sku-1",
            "title": "Sample Product",
            "price": "not-a-price",
            "currency": "USD",
            "source": "dummyjson",
            "crawled_at": "2026-03-18T00:00:00Z",
        }

        record = validate_and_clean(payload)

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertEqual("Invalid price value.", record.reason)


if __name__ == "__main__":
    unittest.main()
