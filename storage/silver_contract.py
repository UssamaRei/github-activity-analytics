"""Pure-Python validation and extraction for GH Archive events."""

import json
import hashlib
from datetime import datetime, timezone


SILVER_COLUMN_TYPES = {
    "event_id": "string",
    "event_type": "string",
    "actor_id": "bigint",
    "actor_login": "string",
    "repository_id": "bigint",
    "repo_name": "string",
    "created_at": "timestamp",
    "event_date": "date",
    "ingested_at": "timestamp",
    "payload_json": "string",
    "record_order": "string",
}


def _reject_json_constant(value):
    raise ValueError("non-standard numeric constant: " + value)


def _required_string(event, path, issues):
    value = event.get(path)
    if not isinstance(value, str) or not value.strip():
        issues.append("missing or invalid " + path)
        return None
    return value


def _parse_utc(value):
    """Parse an ISO-8601 timestamp and return a timezone-aware UTC datetime."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must contain a timezone")
    return parsed.astimezone(timezone.utc)


def record_order(timestamp, raw):
    """Stable lexical Hudi order: source update time, then raw-event digest."""
    normalized = _parse_utc(timestamp).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    return normalized + "|" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def deduplicate_events(records):
    """Choose the highest deterministic record_order for each event_id."""
    latest = {}
    for record in records:
        event_id = record["event_id"]
        if event_id not in latest or record["record_order"] > latest[event_id]["record_order"]:
            latest[event_id] = record
    return list(latest.values())


def _optional_id(parent, key):
    value = parent.get(key) if isinstance(parent, dict) else None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value if value <= 9223372036854775807 else None


def classify_event(raw):
    """Return normalized fields and a reason, or a validation reason for raw JSON."""
    try:
        event = json.loads(raw, parse_constant=_reject_json_constant)
    except (TypeError, ValueError) as exc:
        return {"reason": "invalid JSON: " + str(exc)}

    if not isinstance(event, dict):
        return {"reason": "event must be a JSON object"}

    issues = []
    event_id = _required_string(event, "id", issues)
    event_type = _required_string(event, "type", issues)

    actor = event.get("actor")
    actor_login = actor.get("login") if isinstance(actor, dict) else None
    if not isinstance(actor_login, str) or not actor_login.strip():
        issues.append("missing or invalid actor.login")
        actor_login = None

    repo = event.get("repo")
    repo_name = repo.get("name") if isinstance(repo, dict) else None
    if not isinstance(repo_name, str) or not repo_name.strip():
        issues.append("missing or invalid repo.name")
        repo_name = None

    created_at = _required_string(event, "created_at", issues)
    normalized_time = None
    if created_at is not None:
        try:
            normalized_time = _parse_utc(created_at).isoformat().replace(
                "+00:00", "Z"
            )
        except ValueError as exc:
            if "timezone" in str(exc):
                issues.append("created_at must contain a timezone")
            else:
                issues.append("created_at must be a valid ISO-8601 timestamp")

    payload = event.get("payload")
    if not isinstance(payload, dict):
        issues.append("payload must be a JSON object")

    # Retain the complete source line (including unknown fields) for downstream
    # consumers and deterministic tie-breaking.
    payload_json = raw
    order_timestamp = event.get("updated_at") or normalized_time
    if not isinstance(order_timestamp, str):
        order_timestamp = normalized_time
    try:
        order = record_order(order_timestamp, raw) if order_timestamp else None
    except ValueError:
        order = record_order(normalized_time, raw) if normalized_time else None

    return {
        "event_id": event_id,
        "event_type": event_type,
        "actor_id": _optional_id(actor, "id"),
        "actor_login": actor_login,
        "repository_id": _optional_id(repo, "id"),
        "repo_name": repo_name,
        "created_at": normalized_time,
        "payload_json": payload_json,
        "record_order": order,
        "reason": "; ".join(issues) if issues else None,
    }
