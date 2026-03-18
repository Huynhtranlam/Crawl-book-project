from __future__ import annotations

import json
from typing import Iterable

from kafka import KafkaProducer

from apps.crawler.models import ProductRecord
from apps.producer.config import ProducerConfig


class ProductKafkaProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            client_id=config.client_id,
            request_timeout_ms=config.request_timeout_ms,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
        )

    def publish_batch(self, records: Iterable[ProductRecord]) -> int:
        published = 0
        for record in records:
            future = self._producer.send(
                self._config.topic,
                key=record.product_id,
                value=record.to_dict(),
            )
            future.get(timeout=self._config.request_timeout_ms / 1000)
            published += 1

        self._producer.flush()
        return published

    def close(self) -> None:
        self._producer.close()
