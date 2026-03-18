from __future__ import annotations

from apps.crawler.config import CrawlerConfig
from apps.crawler.source import fetch_normalized_batch
from apps.producer.config import ProducerConfig
from apps.producer.kafka_producer import ProductKafkaProducer


def main() -> int:
    crawler_config = CrawlerConfig.from_env()
    producer_config = ProducerConfig.from_env()
    records = fetch_normalized_batch(crawler_config)

    producer = ProductKafkaProducer(producer_config)
    try:
        published = producer.publish_batch(records)
    finally:
        producer.close()

    print(f"Published {published} records to topic '{producer_config.topic}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
