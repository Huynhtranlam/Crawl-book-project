from __future__ import annotations

from apps.crawler.config import MarketDataConfig
from apps.crawler.source import fetch_normalized_batch
from apps.producer.config import ProducerConfig
from apps.producer.kafka_producer import MarketDataKafkaProducer


def main() -> int:
    market_config = MarketDataConfig.from_env()
    producer_config = ProducerConfig.from_env()
    records = fetch_normalized_batch(market_config)

    producer = MarketDataKafkaProducer(producer_config)
    try:
        published = producer.publish_batch(records)
    finally:
        producer.close()

    print(
        f"Published {published} {market_config.event_type} records "
        f"for {market_config.symbol}"
        f"{' across ' + ','.join(market_config.kline_intervals) if market_config.event_type == 'kline' else ''} "
        f"to topic '{producer_config.topic}'."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
