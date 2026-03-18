from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer

from apps.stream_processor.config import StreamProcessorConfig


class RawKafkaConsumer:
    def __init__(self, config: StreamProcessorConfig) -> None:
        self._config = config
        self._consumer = KafkaConsumer(
            config.kafka_topic,
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=config.kafka_group_id,
            auto_offset_reset=config.kafka_auto_offset_reset,
            enable_auto_commit=False,
            consumer_timeout_ms=config.kafka_poll_timeout_ms,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    def read_batch(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for message in self._consumer:
            if isinstance(message.value, dict):
                records.append(message.value)
            if len(records) >= self._config.kafka_batch_size:
                break

        return records

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()
