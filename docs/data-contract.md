# Shared data contract - v0.1

These conventions describe how components connect. They do not assign work.

## Source and Kafka

- Source: public GH Archive hourly JSON gzip files; timestamps are UTC.
- Fixture: six fictional events in `data/samples/`, labeled separately from real data.
- Topic: `github_events` (required by IL-05); one partition for the local demo.
- Internal broker: `kafka:9092`; host broker: `localhost:29092` by default.
- Message key: UTF-8 `repo.name` so a repository's events share a partition.
- Message value: original UTF-8 JSON line, with its record-ending newline removed.
- No wrapper and no JSON reserialization. Extra fields are preserved.
- Validated fields: nonempty string `id`, `type`, `actor.login`, `repo.name`,
  timezone-aware `created_at`, and object `payload`.
- Other event types are retained, not silently filtered. Analytics can later select
  push, issue, and pull-request events.
- Delivery: at least once. Replaying a file repeats records. Bronze preserves
  duplicates; Silver deduplicates/upserts on the source event `id`.
- A producer run may publish a prefix before encountering a bad line. It exits
  with an error rather than claiming the whole file succeeded.

## HDFS locations

| Location | Current status |
| --- | --- |
| `hdfs://namenode:9000/lake/bronze/github_events` | Implemented: raw JSONL in Spark `part-*` text files |
| `hdfs://namenode:9000/checkpoints/github_events_bronze` | Implemented: durable streaming offsets and sink progress |
| `hdfs://namenode:9000/lake/silver/github_events` | Implemented: Hudi Copy-on-Write table `github_events_silver` |
| `hdfs://namenode:9000/lake/quarantine/github_events` | Implemented: invalid raw JSON and validation reasons as JSONL |
| `hdfs://namenode:9000/lake/gold/repository_activity_hourly` | Planned: Hudi repository aggregates |
| `hdfs://namenode:9000/lake/gold/language_activity_hourly` | Planned: Hudi language aggregates |
| `hdfs://namenode:9000/lake/gold/repository_clusters` | Planned: KMeans results |

The streaming query starts at earliest retained Kafka offsets only for its **first**
run. Subsequent runs resume the checkpoint. Kafka keeps approximately 24 hours of
data locally; if the job falls behind retention, it fails rather than hiding lost data.
Do not delete a checkpoint while retaining the associated sink and claim a clean replay.

## Silver columns

| Column | Type | Notes |
| --- | --- | --- |
| `event_id` | string | Source event ID and Hudi record key |
| `event_type` | string | Source event type |
| `actor_id` | bigint, nullable | Source actor ID when present |
| `actor_login` | string | Validated actor login |
| `repository_id` | bigint, nullable | Source repository ID when present |
| `repo_name` | string | Validated repository name |
| `created_at` | timestamp | Normalized to UTC |
| `event_date` | date | UTC calendar date derived from `created_at` |
| `ingested_at` | timestamp | UTC processing time |
| `payload_json` | string | Exact original event JSON line, including unknown fields |
| `record_order` | string | Deterministic source timestamp plus source-line SHA-256; Hudi precombine field |

Silver uses `event_id` as its Hudi record key and partitions by UTC `event_date`.
The global Bloom index keeps event IDs unique if a corrected record moves to
another date partition. Hudi's `record_order` precombine value is the source
`updated_at` timestamp when valid, otherwise `created_at`, normalized to UTC and
followed by a SHA-256 of the exact raw event line. Duplicate IDs therefore select
the latest source update/event time deterministically, with a stable hash tie-break.
`ingested_at` records processing time and is not used to choose a winner.
Run `docker compose run --rm spark-silver` to reprocess Bronze: valid rows are
validated and upserted, while invalid rows are appended to quarantine with the
original record and a human-readable reason. `payload_json` contains the complete
original event JSON line, preserving unknown source fields. Re-running can add
duplicate quarantine entries; it does not increase the current Silver record
count for the same event IDs.

If `/lake/silver/github_events` already contains a table created by the earlier
unpartitioned contract (with `repo_id` and `ingested_at` precombine), do not point
the partitioned writer at it and do not delete it. Hudi table partition settings
are established at table creation; preserve that table and perform a separately
reviewed Bronze-to-new-table migration before switching the canonical path.

The reusable Spark reader is `storage/hudi_io.py::read_silver(spark, path)`.
`analytics/read_silver.py` uses it to load the Hudi table, check the contract
columns, and run an event-type count. See `analytics/README.md` for its command.

## Analytics definitions to preserve

- `event_count`: number of deduplicated events, not number of commits.
- `active_contributors`: distinct actors observed in the selected event types;
  this includes accounts that might be bots unless explicitly filtered.
- Trends must compare matching UTC periods and state how growth is calculated.
- Language is **not a universal field on GH Archive events**. Language analytics
  needs repository metadata enrichment and caching (e.g. GitHub's repository API).
  Store unknown languages explicitly; do not infer a language from a repo name.
- KMeans should group repositories by documented activity features; it is not a
  prediction model. Features, scaling, K, and validation remain to be selected.
