from __future__ import annotations

import unittest
from decimal import Decimal

from apps.stream_processor.models import CleanKlineRecord, CleanTickerRecord, InvalidRecord
from apps.stream_processor.validator import validate_and_clean


class ValidateAndCleanTests(unittest.TestCase):
    def test_validate_and_clean_returns_clean_ticker_record(self) -> None:
        payload = {
            "event_type": "ticker",
            "event_id": "ticker-BTCUSDT-1",
            "source": "binance",
            "symbol": "BTCUSDT",
            "event_time": "2026-03-18T00:00:00Z",
            "ingest_time": "2026-03-18T00:00:05Z",
            "last_price": "73888.88",
            "price_change_24h": "-359.05",
            "price_change_pct_24h": "-0.484",
            "volume_24h": "100.50",
            "quote_volume_24h": "7391240.00",
            "open_price_24h": "74247.93",
            "high_price_24h": "74893.94",
            "low_price_24h": "73399.19",
            "trade_count_24h": 12345,
        }

        record = validate_and_clean(payload, "ticker")

        self.assertIsInstance(record, CleanTickerRecord)
        assert isinstance(record, CleanTickerRecord)
        self.assertEqual("BTCUSDT", record.symbol)
        self.assertEqual(Decimal("73888.88"), record.last_price)

    def test_validate_and_clean_returns_clean_kline_record(self) -> None:
        payload = {
            "event_type": "kline",
            "event_id": "kline-BTCUSDT-5m-1",
            "source": "binance",
            "symbol": "BTCUSDT",
            "interval": "5m",
            "open_time": "2026-03-18T00:00:00Z",
            "close_time": "2026-03-18T00:04:59Z",
            "open_price": "73800.00",
            "high_price": "73900.00",
            "low_price": "73750.00",
            "close_price": "73888.88",
            "volume": "10.50",
            "quote_asset_volume": "776833.24",
            "trade_count": 1200,
            "is_closed": True,
            "ingest_time": "2026-03-18T00:05:05Z",
        }

        record = validate_and_clean(payload, "kline")

        self.assertIsInstance(record, CleanKlineRecord)
        assert isinstance(record, CleanKlineRecord)
        self.assertEqual("5m", record.interval)
        self.assertEqual(Decimal("73888.88"), record.close_price)

    def test_validate_and_clean_rejects_missing_required_ticker_fields(self) -> None:
        payload = {
            "event_type": "ticker",
            "event_id": "ticker-BTCUSDT-1",
            "source": "binance",
            "symbol": "",
        }

        record = validate_and_clean(payload, "ticker")

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertIn("symbol", record.reason)

    def test_validate_and_clean_rejects_negative_ticker_price(self) -> None:
        payload = {
            "event_type": "ticker",
            "event_id": "ticker-BTCUSDT-1",
            "source": "binance",
            "symbol": "BTCUSDT",
            "event_time": "2026-03-18T00:00:00Z",
            "ingest_time": "2026-03-18T00:00:05Z",
            "last_price": "-1",
            "price_change_24h": "-359.05",
            "price_change_pct_24h": "-0.484",
            "volume_24h": "100.50",
            "quote_volume_24h": "7391240.00",
            "open_price_24h": "74247.93",
            "high_price_24h": "74893.94",
            "low_price_24h": "73399.19",
            "trade_count_24h": 12345,
        }

        record = validate_and_clean(payload, "ticker")

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertEqual("Ticker payload contains negative values.", record.reason)

    def test_validate_and_clean_rejects_invalid_event_type(self) -> None:
        payload = {
            "event_type": "ticker",
            "event_id": "ticker-BTCUSDT-1",
        }

        record = validate_and_clean(payload, "kline")

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertIn("Unexpected event_type", record.reason)

    def test_validate_and_clean_rejects_inconsistent_kline_values(self) -> None:
        payload = {
            "event_type": "kline",
            "event_id": "kline-BTCUSDT-5m-1",
            "source": "binance",
            "symbol": "BTCUSDT",
            "interval": "5m",
            "open_time": "2026-03-18T00:00:00Z",
            "close_time": "2026-03-18T00:04:59Z",
            "open_price": "73800.00",
            "high_price": "73700.00",
            "low_price": "73750.00",
            "close_price": "73888.88",
            "volume": "10.50",
            "quote_asset_volume": "776833.24",
            "trade_count": 1200,
            "is_closed": True,
            "ingest_time": "2026-03-18T00:05:05Z",
        }

        record = validate_and_clean(payload, "kline")

        self.assertIsInstance(record, InvalidRecord)
        assert isinstance(record, InvalidRecord)
        self.assertEqual("Kline high_price is inconsistent with candle values.", record.reason)


if __name__ == "__main__":
    unittest.main()
