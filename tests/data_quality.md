# Data Quality Rules

## Coverage

This project validates data quality in three layers:

- Ingestion: crawler normalization rejects blank identifiers, blank titles, blank currencies, and blank source names.
- Processing: stream processor rejects missing required fields, invalid prices, negative prices, and invalid `crawled_at` timestamps.
- Modeling: dbt enforces null, unique, accepted-value, freshness, and singular SQL checks on core source and mart tables.

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
