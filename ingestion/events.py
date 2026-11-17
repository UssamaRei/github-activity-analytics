"""Read JSONL without reserializing the original event bytes."""

import gzip
import json
from datetime import datetime
from pathlib import Path


def validate_event(raw: bytes) -> dict:
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("An event must be a JSON object")
    for field in ("id", "type", "created_at"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise ValueError(f"Missing or invalid field: {field}")
    for parent, child in (("repo", "name"), ("actor", "login")):
        if not isinstance(event.get(parent), dict) or not isinstance(event[parent].get(child), str) or not event[parent][child]:
            raise ValueError(f"Missing or invalid field: {parent}.{child}")
    if not isinstance(event.get("payload"), dict):
        raise ValueError("Missing or invalid field: payload")
    timestamp = datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("created_at must contain a timezone")
    return event


def iter_events(path: str | Path, limit: int | None = None):
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    count = 0
    with opener(path, "rb") as stream:
        for line_number, line in enumerate(stream, 1):
            raw = line.rstrip(b"\r\n")
            if not raw.strip():
                continue
            try:
                event = validate_event(raw)
            except (ValueError, UnicodeError, TypeError) as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
            yield event, raw
            count += 1
            if limit is not None and count >= limit:
                break
