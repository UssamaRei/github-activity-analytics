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

## HDFS locations

| Location | Current status |
| --- | --- |
| `hdfs://namenode:9000/lake/bronze/github_events` | Implemented: raw JSONL in Spark `part-*` text files |
| `hdfs://namenode:9000/checkpoints/github_events_bronze` | Implemented: durable streaming offsets and sink progress |
| `hdfs://namenode:9000/lake/silver/github_events` | Planned: Hudi table `github_events_silver` |
| `hdfs://namenode:9000/lake/gold/repository_activity_hourly` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/contributor_rankings_daily` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/language_activity_hourly` | Job ready (`analytics/`); Hudi run pending Silver |
| `hdfs://namenode:9000/lake/gold/repository_clusters` | Job ready (`analytics/`); Hudi run pending Silver |

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
