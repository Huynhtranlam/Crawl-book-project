from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class StreamProcessorConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    kafka_auto_offset_reset: str
    kafka_poll_timeout_ms: int
    kafka_batch_size: int
    market_event_type: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_table: str
    error_output_dir: str

    @classmethod
    def from_env(cls) -> "StreamProcessorConfig":
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        kafka_topic = os.getenv("KAFKA_TOPIC", "").strip()
        postgres_host = os.getenv("POSTGRES_HOST", "localhost").strip()
        postgres_db = os.getenv("POSTGRES_DB", "").strip()
        postgres_user = os.getenv("POSTGRES_USER", "").strip()
        postgres_password = os.getenv("POSTGRES_PASSWORD", "").strip()
        market_event_type = os.getenv("MARKET_DATA_EVENT_TYPE", "ticker").strip().lower()

        if not kafka_bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required.")
        if not kafka_topic:
            raise ValueError("KAFKA_TOPIC is required.")
        if not postgres_db:
            raise ValueError("POSTGRES_DB is required.")
        if not postgres_user:
            raise ValueError("POSTGRES_USER is required.")
        if not postgres_password:
            raise ValueError("POSTGRES_PASSWORD is required.")
        if market_event_type not in {"ticker", "kline"}:
            raise ValueError("MARKET_DATA_EVENT_TYPE must be either 'ticker' or 'kline'.")

        return cls(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            kafka_group_id=os.getenv("STREAM_PROCESSOR_GROUP_ID", "stream-processor").strip()
            or "stream-processor",
            kafka_auto_offset_reset=os.getenv(
                "STREAM_PROCESSOR_AUTO_OFFSET_RESET", "earliest"
            ).strip()
            or "earliest",
            kafka_poll_timeout_ms=int(os.getenv("STREAM_PROCESSOR_POLL_TIMEOUT_MS", "10000")),
            kafka_batch_size=int(os.getenv("STREAM_PROCESSOR_BATCH_SIZE", "50")),
            market_event_type=market_event_type,
            postgres_host=postgres_host,
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=postgres_db,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_table=os.getenv(
                "STREAM_PROCESSOR_POSTGRES_TABLE",
                "raw_btc_ticker_events" if market_event_type == "ticker" else "raw_btc_kline_events",
            ).strip()
            or ("raw_btc_ticker_events" if market_event_type == "ticker" else "raw_btc_kline_events"),
            error_output_dir=os.getenv(
                "STREAM_PROCESSOR_ERROR_DIR", "apps/stream_processor/errors"
            ).strip()
            or "apps/stream_processor/errors",
        )
