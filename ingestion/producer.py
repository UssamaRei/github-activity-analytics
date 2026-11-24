"""Publish source JSONL records unchanged, keyed by repository name."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import tempfile
import time
import zlib
from collections import Counter
from pathlib import Path

from ingestion.download import normalize_hour
from ingestion.events import iter_events


def save_manifest(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, suffix=".part", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(record, stream, indent=2)
            stream.write("\n")
            stream.close()
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def publish(args, topic, brokers, make_producer=None, kafka_errors=()):
    counts = Counter()
    offsets = {}
    producer = None
    manifest = {
        "source_hour_utc": args.hour,
        "input_sha256": None,
        "limit": args.limit,
        "brokers": brokers,
        "topic": topic,
        "status": "running",
        "records_sent": 0,
        "offsets": offsets,
        "error": None,
    }
    owns_manifest = False
    try:
        if args.manifest is not None:
            previous = json.loads(args.manifest.read_text(encoding="utf-8")) if args.manifest.exists() else None
            # Preserve an existing record until its input and destination have been checked.
            owns_manifest = previous is None
            with args.input.open("rb") as stream:
                digest = hashlib.sha256()
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
                manifest["input_sha256"] = digest.hexdigest()
            if previous is not None:
                identity = ("source_hour_utc", "limit", "brokers", "topic")
                previous_digest = previous["input_sha256"]
                if (
                    any(previous[key] != manifest[key] for key in identity)
                    or (previous_digest is not None and previous_digest != manifest["input_sha256"])
                ):
                    raise ValueError("Manifest belongs to a different input, hour, limit or Kafka destination")
                if previous["status"] == "complete":
                    return previous
            owns_manifest = True
            save_manifest(args.manifest, manifest)

        if make_producer is not None:
            producer = make_producer(
                bootstrap_servers=brokers,
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=1,
                request_timeout_ms=30000,
            )

        for event, raw in iter_events(args.input, args.limit):
            if producer is not None:
                metadata = producer.send(topic, key=event["repo"]["name"].encode("utf-8"), value=raw).get(timeout=60)
                partition = str(metadata.partition)
                bounds = offsets.setdefault(partition, {
                    "first_offset": metadata.offset,
                    "last_offset": metadata.offset,
                    "records_sent": 0,
                })
                bounds["last_offset"] = metadata.offset
                bounds["records_sent"] += 1
                manifest["records_sent"] += 1
            counts[event["type"]] += 1
            if producer is not None and args.interval:
                time.sleep(args.interval)
        if args.manifest is not None and not counts:
            raise ValueError("Input contained no events")

        result = {
            "mode": "dry-run" if args.dry_run else "published",
            "topic": topic,
            "events": sum(counts.values()),
            "by_type": dict(counts),
        }
        if args.manifest is not None:
            manifest.update(result, status="complete")
            save_manifest(args.manifest, manifest)
            return manifest
        return result
    except (ValueError, OSError, EOFError, zlib.error, KeyboardInterrupt, *kafka_errors) as exc:
        if owns_manifest:
            manifest.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            save_manifest(args.manifest, manifest)
        raise
    finally:
        if producer is not None:
            producer.close(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSONL or .json.gz input")
    parser.add_argument("--limit", type=int, help="Maximum events (default: publish the entire input)")
    parser.add_argument("--interval", type=float, default=0, help="Seconds between events")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without Kafka")
    parser.add_argument("--hour", help="Source UTC hour in YYYY-MM-DD-HH form")
    parser.add_argument("--manifest", type=Path, help="Publication record for one source hour and input")
    args = parser.parse_args()
    if (args.limit is not None and args.limit < 1) or not 0 <= args.interval < float("inf"):
        parser.error("limit must be positive and interval must be finite and nonnegative")
    if bool(args.hour) != bool(args.manifest) or (args.dry_run and args.manifest is not None):
        parser.error("--hour and --manifest must be supplied together for publication")
    if args.hour:
        try:
            args.hour = normalize_hour(args.hour)
        except ValueError as exc:
            parser.error(str(exc))
        if args.input.resolve() == args.manifest.resolve():
            parser.error("input and manifest must have different paths")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    topic = os.getenv("KAFKA_TOPIC", "github_events")
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")
    make_producer = None
    kafka_errors = ()
    try:
        if not args.dry_run:
            from kafka import KafkaProducer
            from kafka.errors import KafkaError

            make_producer = KafkaProducer
            kafka_errors = (KafkaError,)
        print(json.dumps(publish(args, topic, brokers, make_producer, kafka_errors), indent=2))
    except (ValueError, OSError, EOFError, zlib.error, ImportError, *kafka_errors) as exc:
        logging.error("%s", exc)
        raise SystemExit(1) from exc
    except KeyboardInterrupt as exc:
        raise SystemExit(130) from exc


if __name__ == "__main__":
    main()
