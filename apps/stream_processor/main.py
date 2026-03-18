from __future__ import annotations

from apps.stream_processor.config import StreamProcessorConfig
from apps.stream_processor.consumer import RawKafkaConsumer
from apps.stream_processor.error_handler import write_invalid_records
from apps.stream_processor.models import InvalidRecord
from apps.stream_processor.postgres import PostgresWriter
from apps.stream_processor.validator import validate_and_clean


def main() -> int:
    config = StreamProcessorConfig.from_env()
    consumer = RawKafkaConsumer(config)

    try:
        raw_batch = consumer.read_batch()
        valid_records = []
        invalid_records: list[InvalidRecord] = []

        for payload in raw_batch:
            result = validate_and_clean(payload, config.market_event_type)
            if isinstance(result, InvalidRecord):
                invalid_records.append(result)
            else:
                valid_records.append(result)

        with PostgresWriter(config) as writer:
            written_count = writer.write_batch(valid_records)

        error_output = write_invalid_records(invalid_records, config)
        consumer.commit()
    finally:
        consumer.close()

    print(
        "Processed "
        f"{len(raw_batch)} {config.market_event_type} records: "
        f"{written_count} written, {len(invalid_records)} invalid."
    )
    if error_output is not None:
        print(f"Invalid records saved to {error_output}.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
