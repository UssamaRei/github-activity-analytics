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

## Hourly ingestion handoff

- Download with `python -m ingestion.download --hour YYYY-MM-DD-HH --output-dir data/downloads`
  and repeat `--hour` to select more hours. The default is a sample of up to 1,000
  events per hour. Supply `--limit N` for another sample size or `--full-hour`
  to read the entire archive. Output names are `<hour>-sample-<N>.jsonl` or
  `<hour>-full.jsonl`. Files appear at these paths after a successful download.
  Keep the directory on persistent storage so retries reuse completed files.
  Temporary network and gzip read failures get up to three attempts.
- Publish each downloaded file with `python -m ingestion.producer --input FILE --hour YYYY-MM-DD-HH --manifest MANIFEST.json`.
  Use the file's archive hour and a distinct manifest path per input selection
  and Kafka destination. Run one publisher at a time for each manifest and
  keep the input immutable during a run. The producer reads the entire input
  by default. An explicit producer `--limit N` applies to that publication only.
- Both commands return 0 on success (including an already completed selection),
  1 on a processing failure and 2 for invalid CLI arguments. The producer
  returns 130 on keyboard interruption.
- The manifest records `source_hour_utc`, `input_sha256`, `limit`, `brokers`,
  `topic`, `status`, `records_sent`, `offsets`, and `error`. The source hour
  comes from the archive selection. The payload and original event ID are
  unchanged.
- Use only a manifest with `status: complete` to drive Bronze completion.
  Its `offsets` object is keyed by Kafka partition. Each entry contains the
  inclusive `first_offset` and `last_offset` and the acknowledged
  `records_sent` for that partition. Other producers may have written within
  the range. A Bronze checkpoint must commit a next offset at least
  `last_offset + 1` in every listed partition.
- On a reported failure, `status: failed` records known acknowledgments for
  that attempt and an error. Abrupt termination can leave `status: running`
  with only the initial counters. Kafka may have accepted an event whose
  acknowledgment was lost. Rerun with the same input and manifest to replay
  from the start. Bronze can contain duplicates and Silver deduplicates by
  event ID. A successful retry records that attempt's acknowledgments.
  Keep completed manifests with the Kafka data they describe. If the topic
  or cluster is recreated, use fresh manifests before republishing.

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
| `hdfs://namenode:9000/lake/gold/contributor_activity_hourly` | Planned: contributor activity for the dashboard |
| `hdfs://namenode:9000/lake/silver/github_events` | Planned: Hudi table `github_events_silver` |
| `hdfs://namenode:9000/lake/gold/repository_activity_hourly` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/contributor_rankings_daily` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/language_activity_hourly` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/repository_clusters` | Job ready (`analytics/`); Hudi run pending Silver |

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

## Part 4 handoffs and SQL serving

The runner checks committed Kafka offsets before downstream jobs start. Gold
schemas, executable-job adapters and SQL publication are defined in
[part4-integration.md](part4-integration.md). PostgreSQL serves checked Gold exports
to Superset. A separate `demo` schema contains a labeled fixture; real `gold` starts
empty until the actual Hudi outputs are provided by Parts 2/3.
- KMeans groups repositories by documented activity features; it is not a
  prediction model. Selected: features `event_count`, `active_contributors`,
  `push_share`, `pr_share`, `issue_share` over a UTC period; `log1p` on the two
  counts, then `StandardScaler` (mean 0, std 1); K chosen by best silhouette in
  [2, 6] unless forced; seed 42. Settings and cluster explanations are saved as JSON.

## Gold tables

Produced by `analytics/` (see [analytics/README.md](../analytics/README.md) for
commands). All tables live under `hdfs://namenode:9000/lake/gold/<table>`, are Hudi
upserts keyed as below with `computed_at` as precombine column, and are partitioned
by the listed date column (Hive-style). Example exports from **test fixtures**:
`data/samples/gold/`.

| Table | Record key | Partition |
| --- | --- | --- |
| `repository_activity_hourly` | `hour_utc`, `repo_name` | `event_date` |
| `contributor_rankings_daily` | `event_date`, `actor_login` | `event_date` |
| `language_activity_hourly` | `hour_utc`, `language` | `event_date` |
| `repository_clusters` | `period_end`, `repo_name` | `period_end` |

**repository_activity_hourly:** `hour_utc` (timestamp, UTC hour start), `repo_name`,
`event_count`, `push_count`, `pr_count`, `issue_count`, `active_contributors` (long),
`event_date` (date), `previous_event_count` (long, nullable), `growth_pct` (double,
nullable), `computed_at` (timestamp).

**contributor_rankings_daily:** `event_date`, `actor_login`, `event_count`,
`repo_count` (distinct repositories), `rank` (1 = most events that UTC day; ties share
a rank), `computed_at`.

**language_activity_hourly:** `hour_utc`, `language` (GitHub primary language or
`unknown`), `event_count`, `repo_count`, `active_contributors` (distinct actors across
that language's repositories), `event_date`, `previous_event_count`, `growth_pct`,
`computed_at`.

**repository_clusters:** `period_start`, `period_end` (dates), `repo_name`,
`cluster_id`, `cluster_label` (readable description), `event_count`,
`active_contributors`, `push_share`, `pr_share`, `issue_share` (raw, unscaled),
`k`, `silhouette`, `computed_at`.

**Growth:** `growth_pct = (event_count - previous_event_count) / previous_event_count * 100`,
rounded to 2 decimals, where `previous_event_count` is the value for the **same key
and same UTC hour one day earlier**. Null when that earlier row does not exist (the
hour may not have been ingested) or is 0.

**Language enrichment:** `GET https://api.github.com/repos/{repo_name}` -> `language`,
cached in `data/cache/repo_languages.json` (git-ignored). 404/451 or a null language
are cached as unknown; temporary failures (rate limit, network, 5xx) are not cached
and are retried on the next run.
