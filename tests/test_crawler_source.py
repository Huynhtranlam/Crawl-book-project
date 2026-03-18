from __future__ import annotations

import unittest

from apps.crawler.config import CrawlerConfig
from apps.crawler.source import _extract_items


class ExtractItemsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = CrawlerConfig(
            source_url="https://example.com/products",
            source_items_key="products",
            source_name="example-source",
            batch_limit=10,
            http_timeout_seconds=30,
            default_currency="USD",
        )

    def test_extract_items_from_list_payload(self) -> None:
        payload = [{"id": "p-1"}]

        items = _extract_items(payload, self.config)

        self.assertEqual(payload, items)

    def test_extract_items_from_configured_dict_key(self) -> None:
        payload = {"products": [{"id": "p-1"}, {"id": "p-2"}]}

        items = _extract_items(payload, self.config)

        self.assertEqual(payload["products"], items)

    def test_raise_when_configured_items_key_is_not_a_list(self) -> None:
        payload = {"products": {"id": "p-1"}}

        with self.assertRaises(ValueError):
            _extract_items(payload, self.config)


if __name__ == "__main__":
    unittest.main()
