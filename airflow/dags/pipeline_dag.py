from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/pipeline"
RAW_TOPIC = os.getenv("AIRFLOW_PIPELINE_TOPIC", "products.raw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("AIRFLOW_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": PROJECT_DIR,
            "CRAWLER_SOURCE_URL": os.getenv(
                "CRAWLER_SOURCE_URL", "https://dummyjson.com/products"
            ),
            "CRAWLER_SOURCE_ITEMS_KEY": os.getenv("CRAWLER_SOURCE_ITEMS_KEY", "products"),
            "CRAWLER_SOURCE_NAME": os.getenv("CRAWLER_SOURCE_NAME", "dummyjson"),
            "CRAWLER_BATCH_LIMIT": os.getenv("CRAWLER_BATCH_LIMIT", "10"),
            "CRAWLER_HTTP_TIMEOUT_SECONDS": os.getenv(
                "CRAWLER_HTTP_TIMEOUT_SECONDS", "30"
            ),
            "CRAWLER_DEFAULT_CURRENCY": os.getenv("CRAWLER_DEFAULT_CURRENCY", "USD"),
        }
    )
    return env


with DAG(
    dag_id="realtime_price_pipeline",
    description="Run ingestion and stream processing in sequence.",
    start_date=datetime(2024, 1, 1),
    schedule=os.getenv("AIRFLOW_PIPELINE_SCHEDULE", "@hourly"),
    catchup=False,
    tags=["pipeline", "phase-4"],
) as dag:
    ingest_raw_products = BashOperator(
        task_id="ingest_raw_products",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.producer.main",
        env={
            **_base_env(),
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": RAW_TOPIC,
            "KAFKA_CLIENT_ID": os.getenv("KAFKA_CLIENT_ID", "crawler-producer"),
            "KAFKA_REQUEST_TIMEOUT_MS": os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"),
        },
    )

    process_raw_products = BashOperator(
        task_id="process_raw_products",
        bash_command=f"cd {PROJECT_DIR} && python -m apps.stream_processor.main",
        env={
            **_base_env(),
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": RAW_TOPIC,
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
                "AIRFLOW_STREAM_PROCESSOR_BATCH_SIZE", "10"
            ),
            "STREAM_PROCESSOR_POSTGRES_TABLE": os.getenv(
                "AIRFLOW_STREAM_PROCESSOR_POSTGRES_TABLE", "products_clean"
            ),
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

    ingest_raw_products >> process_raw_products
