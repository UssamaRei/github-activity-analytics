"""TEST DATA ONLY: build Silver-shaped rows from the fictional sample events.

Part 2 owns the real Silver Hudi table (`github_events_silver`). Until it is
available, analytics jobs and tests use these rows. They follow the proposed
Silver columns in docs/data-contract.md and are deduplicated on `event_id`,
as Silver will be. Never present results computed from them as real activity.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ingestion.events import iter_events

SAMPLE_EVENTS = Path(__file__).resolve().parents[1] / "data/samples/github_events.jsonl"

SILVER_COLUMNS = (
    "event_id",
    "event_type",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "created_at",
    "event_date",
    "ingested_at",
    "payload_json",
)

# Fixed so fixture output is reproducible across runs.
FIXTURE_INGESTED_AT = datetime(2025, 1, 1, 16, 0, tzinfo=timezone.utc)


def to_silver_row(event: dict) -> dict:
    created_at = datetime.fromisoformat(event["created_at"].replace("Z", "+00:00")).astimezone(timezone.utc)
    return {
        "event_id": event["id"],
        "event_type": event["type"],
        "actor_id": event["actor"].get("id"),
        "actor_login": event["actor"]["login"],
        "repo_id": event["repo"].get("id"),
        "repo_name": event["repo"]["name"],
        "created_at": created_at,
        "event_date": created_at.date(),
        "ingested_at": FIXTURE_INGESTED_AT,
        "payload_json": json.dumps(event["payload"], separators=(",", ":"), sort_keys=True),
    }


def load_silver_rows(path: str | Path = SAMPLE_EVENTS) -> list[dict]:
    """Return one Silver-shaped row per event ID; the first occurrence wins."""
    rows: dict[str, dict] = {}
    for event, _raw in iter_events(path):
        rows.setdefault(event["id"], to_silver_row(event))
    return list(rows.values())


# FICTIONAL language metadata for tests. Not real GitHub data. beta is left out
# on purpose so the "unknown" path is exercised.
FICTIONAL_LANGUAGES = {
    "demo-org/alpha": "Python",
}

# TEST DATA ONLY: three fictional activity profiles for the clustering job.
# (event-type weights, events range, actor pool size)
CLUSTER_PROFILES = {
    "automation": ({"PushEvent": 1.0}, (40, 60), 1),
    "community": ({"PullRequestEvent": 0.4, "IssuesEvent": 0.4, "PushEvent": 0.2}, (30, 50), 14),
    "quiet": ({"PushEvent": 0.5, "IssuesEvent": 0.5}, (3, 6), 2),
}
REPOS_PER_PROFILE = 4


def cluster_fixture_rows(seed: int = 42) -> list[dict]:
    """Deterministic Silver-shaped rows: 12 fictional repos across two UTC days.

    Repo names are `fixture-<profile>/repo-N` so tests can check that KMeans
    recovers the profiles; the job itself never reads the name.
    """
    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    repo_id = 1000
    for profile, (weights, (low, high), actors) in CLUSTER_PROFILES.items():
        types, probabilities = list(weights), list(weights.values())
        for n in range(1, REPOS_PER_PROFILE + 1):
            repo_id += 1
            repo_name = f"fixture-{profile}/repo-{n}"
            for i in range(rng.randint(low, high)):
                created_at = start + timedelta(minutes=rng.randrange(2 * 24 * 60))
                actor = rng.randrange(actors)
                event = {
                    "id": f"fx-{repo_id}-{i}",
                    "type": rng.choices(types, probabilities)[0],
                    "actor": {"id": 5000 + repo_id * 100 + actor, "login": f"fx-{profile}-{n}-user{actor}"},
                    "repo": {"id": repo_id, "name": repo_name},
                    "created_at": created_at.isoformat().replace("+00:00", "Z"),
                    "payload": {},
                }
                rows.append(to_silver_row(event))
    return rows
