from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    client_id: str
    request_timeout_ms: int

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        topic = os.getenv("KAFKA_TOPIC", "").strip()

        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required.")
        if not topic:
            raise ValueError("KAFKA_TOPIC is required.")

        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            client_id=os.getenv("KAFKA_CLIENT_ID", "market-data-producer").strip()
            or "market-data-producer",
            request_timeout_ms=int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000")),
        )
