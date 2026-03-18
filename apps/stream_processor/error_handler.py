from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from apps.stream_processor.config import StreamProcessorConfig
from apps.stream_processor.models import InvalidRecord


def write_invalid_records(
    records: Iterable[InvalidRecord], config: StreamProcessorConfig
) -> Path | None:
    invalid_records = list(records)
    if not invalid_records:
        return None

    output_dir = Path(config.error_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / _build_filename()

    with output_path.open("w", encoding="utf-8") as file_handle:
        json.dump(
            [
                {"reason": record.reason, "payload": record.payload}
                for record in invalid_records
            ],
            file_handle,
            ensure_ascii=True,
            indent=2,
        )

    return output_path


def _build_filename() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"invalid_records_{timestamp}.json"
