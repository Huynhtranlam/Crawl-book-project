from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_batch

from apps.stream_processor.config import StreamProcessorConfig
from apps.stream_processor.models import CleanProductRecord


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
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._config.postgres_table} (
                    product_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    price NUMERIC NULL,
                    currency TEXT NOT NULL,
                    product_url TEXT NULL,
                    image_url TEXT NULL,
                    source TEXT NOT NULL,
                    crawled_at TIMESTAMPTZ NOT NULL,
                    raw_payload JSONB NOT NULL
                )
                """
            )
        self._connection.commit()

    def write_batch(self, records: Iterable[CleanProductRecord]) -> int:
        rows = [
            (
                record.product_id,
                record.title,
                str(record.price) if record.price is not None else None,
                record.currency,
                record.product_url,
                record.image_url,
                record.source,
                record.crawled_at,
                Json(record.raw_payload),
            )
            for record in records
        ]

        if not rows:
            return 0

        with self._connection.cursor() as cursor:
            execute_batch(
                cursor,
                f"""
                INSERT INTO {self._config.postgres_table} (
                    product_id,
                    title,
                    price,
                    currency,
                    product_url,
                    image_url,
                    source,
                    crawled_at,
                    raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    currency = EXCLUDED.currency,
                    product_url = EXCLUDED.product_url,
                    image_url = EXCLUDED.image_url,
                    source = EXCLUDED.source,
                    crawled_at = EXCLUDED.crawled_at,
                    raw_payload = EXCLUDED.raw_payload
                """,
                rows,
            )
        self._connection.commit()
        return len(rows)
