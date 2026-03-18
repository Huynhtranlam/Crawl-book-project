from __future__ import annotations

import unittest

from apps.crawler.config import MarketDataConfig, _parse_kline_intervals
from apps.crawler.models import normalize_klines, normalize_ticker


class NormalizeMarketDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MarketDataConfig(
            api_base_url="https://api.binance.com/api/v3",
            source_name="binance",
            symbol="BTCUSDT",
            event_type="ticker",
            kline_interval="5m",
            kline_intervals=("5m",),
            kline_limit=500,
            http_timeout_seconds=30,
            http_max_retries=3,
            http_backoff_seconds=2,
        )

    def test_normalize_ticker_builds_market_event(self) -> None:
        payload = {
            "symbol": "BTCUSDT",
            "closeTime": 1773820500000,
            "lastPrice": "73912.40",
            "priceChange": "120.50",
            "priceChangePercent": "0.16",
            "volume": "100.00",
            "quoteVolume": "7391240.00",
            "openPrice": "73791.90",
            "highPrice": "74000.00",
            "lowPrice": "73000.00",
            "count": 12345,
        }

        record = normalize_ticker(payload, self.config)

        self.assertEqual("ticker", record.event_type)
        self.assertEqual("BTCUSDT", record.symbol)
        self.assertEqual("73912.40", record.payload["last_price"])

    def test_normalize_ticker_rejects_missing_symbol(self) -> None:
        payload = {"closeTime": 1773820500000, "lastPrice": "73912.40"}

        with self.assertRaises(ValueError):
            normalize_ticker(payload, self.config)

    def test_normalize_klines_builds_market_events(self) -> None:
        payload = [
            [
                1773819900000,
                "73710.01",
                "73837.88",
                "73700.01",
                "73833.51",
                "25.78407000",
                1773820199999,
                "1901727.49515800",
                9529,
                "17.29198000",
                "1275421.23152540",
                "0",
            ]
        ]

        records = normalize_klines(payload, self.config, "5m")

        self.assertEqual(1, len(records))
        self.assertEqual("kline", records[0].event_type)
        self.assertEqual("5m", records[0].payload["interval"])

    def test_normalize_klines_rejects_invalid_shape(self) -> None:
        payload = [["not-enough-fields"]]

        with self.assertRaises(ValueError):
            normalize_klines(payload, self.config, "5m")

    def test_parse_kline_intervals_supports_multiple_values(self) -> None:
        intervals = _parse_kline_intervals("1m,5m,15m,1h,4h,1d,1w")

        self.assertEqual(("1m", "5m", "15m", "1h", "4h", "1d", "1w"), intervals)

    def test_parse_kline_intervals_rejects_unsupported_values(self) -> None:
        with self.assertRaises(ValueError):
            _parse_kline_intervals("2m,5m")


if __name__ == "__main__":
    unittest.main()
