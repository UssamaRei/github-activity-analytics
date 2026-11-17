"""Publish source JSONL records unchanged, keyed by repository name."""

import argparse
import json
import logging
import os
import time
from collections import Counter

from ingestion.events import iter_events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="JSONL or .json.gz input")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum events per run")
    parser.add_argument("--interval", type=float, default=0, help="Seconds between events")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without Kafka")
    args = parser.parse_args()
    if args.limit < 1 or args.interval < 0:
        parser.error("limit must be positive and interval must be nonnegative")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    producer = None
    topic = os.getenv("KAFKA_TOPIC", "github_events")
    counts = Counter()
    try:
        if not args.dry_run:
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(","),
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=1,
                request_timeout_ms=30000,
            )
        for event, raw in iter_events(args.input, args.limit):
            if producer is not None:
                producer.send(topic, key=event["repo"]["name"].encode("utf-8"), value=raw).get(timeout=30)
            counts[event["type"]] += 1
            if producer is not None and args.interval:
                time.sleep(args.interval)
        print(json.dumps({"mode": "dry-run" if args.dry_run else "published", "topic": topic, "events": sum(counts.values()), "by_type": dict(counts)}, indent=2))
    except (ValueError, OSError) as exc:
        logging.error("%s", exc)
        raise SystemExit(1) from exc
    finally:
        if producer is not None:
            producer.close(timeout=10)


if __name__ == "__main__":
    main()
