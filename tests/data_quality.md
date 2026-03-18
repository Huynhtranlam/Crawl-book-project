# Data Quality Rules

## Coverage

This project validates data quality in three layers:

- Ingestion: market data normalization rejects malformed Binance ticker and kline payloads before publishing.
- Processing: stream processor rejects missing fields, negative values where impossible, invalid timestamps, and inconsistent OHLC candle values.
- Modeling: dbt enforces null, unique, accepted-value, freshness, and singular SQL checks on BTC ticker/kline source and mart tables.

## Local Commands

Run all Phase 9 quality checks locally:

```bash
python3 tests/run_data_quality.py
```

Run dbt checks directly if needed:

```bash
python3 -c "from dbt.cli.main import dbtRunner; print(dbtRunner().invoke(['source', 'freshness', '--project-dir', 'dbt', '--profiles-dir', 'dbt']).success)"
python3 -c "from dbt.cli.main import dbtRunner; print(dbtRunner().invoke(['test', '--project-dir', 'dbt', '--profiles-dir', 'dbt']).success)"
```
