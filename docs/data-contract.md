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
  duplicates; Silver must deduplicate/upsert on the source event `id`.
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
| `hdfs://namenode:9000/lake/silver/github_events` | Planned: Hudi table `github_events_silver` |
| `hdfs://namenode:9000/lake/gold/repository_activity_hourly` | Planned: Hudi repository aggregates |
| `hdfs://namenode:9000/lake/gold/language_activity_hourly` | Planned: Hudi language aggregates |
| `hdfs://namenode:9000/lake/gold/repository_clusters` | Planned: KMeans results |
| `hdfs://namenode:9000/lake/gold/contributor_activity_hourly` | Planned: contributor activity for the dashboard |

The streaming query starts at earliest retained Kafka offsets only for its **first**
run. Subsequent runs resume the checkpoint. Kafka keeps approximately 24 hours of
data locally; if the job falls behind retention, it fails rather than hiding lost data.
Do not delete a checkpoint while retaining the associated sink and claim a clean replay.

## Proposed Silver columns - not implemented yet

`event_id`, `event_type`, `actor_id`, `actor_login`, `repo_id`, `repo_name`,
`created_at` (UTC timestamp), `event_date`, `ingested_at`, `payload_json`.

Use `event_id` as the Hudi record key. Decide the precombine column and partition
strategy when implementing Hudi, then record the decision here.

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
