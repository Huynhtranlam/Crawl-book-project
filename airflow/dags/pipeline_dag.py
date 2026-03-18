from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/pipeline"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("AIRFLOW_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PATH": f"/home/airflow/.local/bin:{env.get('PATH', '')}",
            "PYTHONPATH": PROJECT_DIR,
            "MARKET_DATA_API_BASE_URL": os.getenv(
                "AIRFLOW_MARKET_DATA_API_BASE_URL", "https://api.binance.com/api/v3"
            ),
            "MARKET_SOURCE_NAME": os.getenv("AIRFLOW_MARKET_SOURCE_NAME", "binance"),
            "MARKET_SYMBOL": os.getenv("AIRFLOW_MARKET_SYMBOL", "BTCUSDT"),
            "MARKET_KLINE_INTERVAL": os.getenv("AIRFLOW_MARKET_KLINE_INTERVAL", "5m"),
            "MARKET_KLINE_INTERVALS": os.getenv(
                "AIRFLOW_MARKET_KLINE_INTERVALS", "1m,5m,15m,1h,4h,1d,1w"
            ),
            "MARKET_KLINE_LIMIT": os.getenv("AIRFLOW_MARKET_KLINE_LIMIT", "500"),
            "MARKET_HTTP_TIMEOUT_SECONDS": os.getenv(
                "AIRFLOW_MARKET_HTTP_TIMEOUT_SECONDS", "30"
            ),
            "MARKET_HTTP_MAX_RETRIES": os.getenv("AIRFLOW_MARKET_HTTP_MAX_RETRIES", "3"),
            "MARKET_HTTP_BACKOFF_SECONDS": os.getenv(
                "AIRFLOW_MARKET_HTTP_BACKOFF_SECONDS", "2"
            ),
        }
    )
    return env


with DAG(
    dag_id="btc_market_data_pipeline",
    description="Run BTC ticker and kline ingestion with downstream processing.",
    start_date=datetime(2024, 1, 1),
    schedule=os.getenv("AIRFLOW_MARKET_PIPELINE_SCHEDULE", "*/5 * * * *"),
    catchup=False,
    tags=["market-data", "btc"],
) as dag:
    ingest_btc_ticker = BashOperator(
        task_id="ingest_btc_ticker",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.producer.main",
        env={
            **_base_env(),
            "MARKET_DATA_EVENT_TYPE": "ticker",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": os.getenv("AIRFLOW_MARKET_TICKER_TOPIC", "market.raw.ticker"),
            "KAFKA_CLIENT_ID": os.getenv("KAFKA_CLIENT_ID", "market-data-producer"),
            "KAFKA_REQUEST_TIMEOUT_MS": os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"),
        },
    )

    process_btc_ticker = BashOperator(
        task_id="process_btc_ticker",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.stream_processor.main",
        env={
            **_base_env(),
            "MARKET_DATA_EVENT_TYPE": "ticker",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": os.getenv("AIRFLOW_MARKET_TICKER_TOPIC", "market.raw.ticker"),
            "STREAM_PROCESSOR_GROUP_ID": os.getenv(
                "AIRFLOW_STREAM_PROCESSOR_GROUP_ID", "stream-processor-airflow"
            ),
            "STREAM_PROCESSOR_AUTO_OFFSET_RESET": os.getenv(
                "STREAM_PROCESSOR_AUTO_OFFSET_RESET", "earliest"
            ),
            "STREAM_PROCESSOR_POLL_TIMEOUT_MS": os.getenv(
                "STREAM_PROCESSOR_POLL_TIMEOUT_MS", "10000"
            ),
            "STREAM_PROCESSOR_BATCH_SIZE": os.getenv(
                "AIRFLOW_STREAM_PROCESSOR_BATCH_SIZE", "500"
            ),
            "STREAM_PROCESSOR_POSTGRES_TABLE": "raw_btc_ticker_events",
            "STREAM_PROCESSOR_ERROR_DIR": "/opt/airflow/logs/stream_processor_errors",
            "POSTGRES_HOST": os.getenv("AIRFLOW_POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("AIRFLOW_POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("AIRFLOW_POSTGRES_DB", "analytics"),
            "POSTGRES_USER": os.getenv("AIRFLOW_POSTGRES_USER", "analytics"),
            "POSTGRES_PASSWORD": os.getenv(
                "AIRFLOW_POSTGRES_PASSWORD", "analytics123"
            ),
        },
    )

    ingest_btc_klines = BashOperator(
        task_id="ingest_btc_klines",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.producer.main",
        env={
            **_base_env(),
            "MARKET_DATA_EVENT_TYPE": "kline",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": os.getenv("AIRFLOW_MARKET_KLINES_TOPIC", "market.raw.klines"),
            "KAFKA_CLIENT_ID": os.getenv("KAFKA_CLIENT_ID", "market-data-producer"),
            "KAFKA_REQUEST_TIMEOUT_MS": os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"),
        },
    )

    process_btc_klines = BashOperator(
        task_id="process_btc_klines",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.stream_processor.main",
        env={
            **_base_env(),
            "MARKET_DATA_EVENT_TYPE": "kline",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": os.getenv("AIRFLOW_MARKET_KLINES_TOPIC", "market.raw.klines"),
            "STREAM_PROCESSOR_GROUP_ID": os.getenv(
                "AIRFLOW_STREAM_PROCESSOR_GROUP_ID", "stream-processor-airflow"
            ),
            "STREAM_PROCESSOR_AUTO_OFFSET_RESET": os.getenv(
                "STREAM_PROCESSOR_AUTO_OFFSET_RESET", "earliest"
            ),
            "STREAM_PROCESSOR_POLL_TIMEOUT_MS": os.getenv(
                "STREAM_PROCESSOR_POLL_TIMEOUT_MS", "10000"
            ),
            "STREAM_PROCESSOR_BATCH_SIZE": os.getenv(
                "AIRFLOW_STREAM_PROCESSOR_BATCH_SIZE", "500"
            ),
            "STREAM_PROCESSOR_POSTGRES_TABLE": "raw_btc_kline_events",
            "STREAM_PROCESSOR_ERROR_DIR": "/opt/airflow/logs/stream_processor_errors",
            "POSTGRES_HOST": os.getenv("AIRFLOW_POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("AIRFLOW_POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("AIRFLOW_POSTGRES_DB", "analytics"),
            "POSTGRES_USER": os.getenv("AIRFLOW_POSTGRES_USER", "analytics"),
            "POSTGRES_PASSWORD": os.getenv(
                "AIRFLOW_POSTGRES_PASSWORD", "analytics123"
            ),
        },
    )

    build_btc_marts = BashOperator(
        task_id="build_btc_marts",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "dbt run --project-dir dbt --profiles-dir dbt "
            "--select mart_btc_ohlcv mart_btc_price_latest"
        ),
        env={
            **_base_env(),
            "POSTGRES_HOST": os.getenv("AIRFLOW_POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("AIRFLOW_POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("AIRFLOW_POSTGRES_DB", "analytics"),
            "POSTGRES_USER": os.getenv("AIRFLOW_POSTGRES_USER", "analytics"),
            "POSTGRES_PASSWORD": os.getenv("AIRFLOW_POSTGRES_PASSWORD", "analytics123"),
        },
    )

    ingest_btc_ticker >> process_btc_ticker >> build_btc_marts
    ingest_btc_klines >> process_btc_klines >> build_btc_marts
