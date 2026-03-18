from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_batch

from apps.stream_processor.config import StreamProcessorConfig
from apps.stream_processor.models import CleanKlineRecord, CleanTickerRecord


class PostgresWriter(AbstractContextManager["PostgresWriter"]):
    def __init__(self, config: StreamProcessorConfig) -> None:
        self._config = config
        self._connection = psycopg2.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            dbname=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password,
        )

    def __enter__(self) -> "PostgresWriter":
        self.ensure_table()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._connection.close()

    def ensure_table(self) -> None:
        if self._config.market_event_type == "ticker":
            statement = f"""
                CREATE TABLE IF NOT EXISTS {self._config.postgres_table} (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_time TIMESTAMPTZ NOT NULL,
                    ingest_time TIMESTAMPTZ NOT NULL,
                    last_price NUMERIC NOT NULL,
                    price_change_24h NUMERIC NOT NULL,
                    price_change_pct_24h NUMERIC NOT NULL,
                    volume_24h NUMERIC NOT NULL,
                    quote_volume_24h NUMERIC NOT NULL,
                    open_price_24h NUMERIC NOT NULL,
                    high_price_24h NUMERIC NOT NULL,
                    low_price_24h NUMERIC NOT NULL,
                    trade_count_24h INTEGER NOT NULL,
                    raw_payload JSONB NOT NULL
                )
            """
        else:
            statement = f"""
                CREATE TABLE IF NOT EXISTS {self._config.postgres_table} (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time TIMESTAMPTZ NOT NULL,
                    close_time TIMESTAMPTZ NOT NULL,
                    open_price NUMERIC NOT NULL,
                    high_price NUMERIC NOT NULL,
                    low_price NUMERIC NOT NULL,
                    close_price NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    quote_asset_volume NUMERIC NOT NULL,
                    trade_count INTEGER NOT NULL,
                    is_closed BOOLEAN NOT NULL,
                    ingest_time TIMESTAMPTZ NOT NULL,
                    raw_payload JSONB NOT NULL
                )
            """

        with self._connection.cursor() as cursor:
            cursor.execute(statement)
        self._connection.commit()

    def write_batch(
        self, records: Iterable[CleanTickerRecord | CleanKlineRecord]
    ) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        if self._config.market_event_type == "ticker":
            rows = [
                (
                    record.event_id,
                    record.source,
                    record.symbol,
                    record.event_time,
                    record.ingest_time,
                    str(record.last_price),
                    str(record.price_change_24h),
                    str(record.price_change_pct_24h),
                    str(record.volume_24h),
                    str(record.quote_volume_24h),
                    str(record.open_price_24h),
                    str(record.high_price_24h),
                    str(record.low_price_24h),
                    record.trade_count_24h,
                    Json(record.raw_payload),
                )
                for record in record_list
                if isinstance(record, CleanTickerRecord)
            ]
            statement = f"""
                INSERT INTO {self._config.postgres_table} (
                    event_id,
                    source,
                    symbol,
                    event_time,
                    ingest_time,
                    last_price,
                    price_change_24h,
                    price_change_pct_24h,
                    volume_24h,
                    quote_volume_24h,
                    open_price_24h,
                    high_price_24h,
                    low_price_24h,
                    trade_count_24h,
                    raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    source = EXCLUDED.source,
                    symbol = EXCLUDED.symbol,
                    event_time = EXCLUDED.event_time,
                    ingest_time = EXCLUDED.ingest_time,
                    last_price = EXCLUDED.last_price,
                    price_change_24h = EXCLUDED.price_change_24h,
                    price_change_pct_24h = EXCLUDED.price_change_pct_24h,
                    volume_24h = EXCLUDED.volume_24h,
                    quote_volume_24h = EXCLUDED.quote_volume_24h,
                    open_price_24h = EXCLUDED.open_price_24h,
                    high_price_24h = EXCLUDED.high_price_24h,
                    low_price_24h = EXCLUDED.low_price_24h,
                    trade_count_24h = EXCLUDED.trade_count_24h,
                    raw_payload = EXCLUDED.raw_payload
            """
        else:
            rows = [
                (
                    record.event_id,
                    record.source,
                    record.symbol,
                    record.interval,
                    record.open_time,
                    record.close_time,
                    str(record.open_price),
                    str(record.high_price),
                    str(record.low_price),
                    str(record.close_price),
                    str(record.volume),
                    str(record.quote_asset_volume),
                    record.trade_count,
                    record.is_closed,
                    record.ingest_time,
                    Json(record.raw_payload),
                )
                for record in record_list
                if isinstance(record, CleanKlineRecord)
            ]
            statement = f"""
                INSERT INTO {self._config.postgres_table} (
                    event_id,
                    source,
                    symbol,
                    interval,
                    open_time,
                    close_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    quote_asset_volume,
                    trade_count,
                    is_closed,
                    ingest_time,
                    raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    source = EXCLUDED.source,
                    symbol = EXCLUDED.symbol,
                    interval = EXCLUDED.interval,
                    open_time = EXCLUDED.open_time,
                    close_time = EXCLUDED.close_time,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    quote_asset_volume = EXCLUDED.quote_asset_volume,
                    trade_count = EXCLUDED.trade_count,
                    is_closed = EXCLUDED.is_closed,
                    ingest_time = EXCLUDED.ingest_time,
                    raw_payload = EXCLUDED.raw_payload
            """

        with self._connection.cursor() as cursor:
            execute_batch(cursor, statement, rows)
        self._connection.commit()
        return len(rows)
