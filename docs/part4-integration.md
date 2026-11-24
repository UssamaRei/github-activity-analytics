# Part 4 integration contract

The integrated branch connects all four parts. Coordinate interface changes here
and in `docs/data-contract.md` before changing jobs or published schemas.

## Kafka -> durable Bronze

The runner records each partition's acknowledged `offset + 1`. Airflow waits until
a **committed** Spark batch reaches every required offset. It reads the commit
and matching offset files under `/checkpoints/github_events_bronze` through WebHDFS.
Uncommitted offsets or old raw records do not establish completion.

## Commands from Parts 2/3

`orchestration/job_config.json` maps fixed names to scripts and argument lists.
The mapped scripts now exist and are used by both complete-pipeline DAGs.
Jobs run under Spark 3.5.1/Hudi 0.15 with a UTC `--hour YYYY-MM-DD-HH`, must finish,
and must exit nonzero on errors or unavailable required inputs. Repository metrics
also produce contributor rankings. Clustering must finish before publication.

## Hudi Gold -> PostgreSQL

Paths are `/lake/gold/<table>` under `hdfs://namenode:9000`. These are requirements
for the exporter and the integrated analytics jobs.

| Table | Columns | Unique key |
| --- | --- | --- |
| `repository_activity_hourly` | `hour_utc`, `repo_name`, `event_count`, `push_count`, `pr_count`, `issue_count`, `active_contributors` | hour + repo |
| `language_activity_hourly` | `hour_utc`, `language`, `event_count`, `active_repositories` | hour + language |
| `contributor_activity_hourly` | `hour_utc`, `actor_login`, `event_count`, `repo_count` | hour + actor |
| `repository_clusters` | `snapshot_utc`, `repo_name`, `cluster_id`, `event_count`, `active_contributors` | snapshot + repo |

- Timestamps explicitly use UTC; `hour_utc` is the start of an hour.
- Names are nonempty; counts and cluster IDs are nonnegative signed-BIGINT integers.
- Event counts refer to deduplicated events, not commits.
- Event-type subtotals and contributor counts cannot exceed event totals.
- Repository, language and contributor event totals reconcile per UTC hour.
- Missing language is `unknown`; retain those events in totals.
- All four tables must be nonempty, with no duplicate keys.

The exporter projects these columns and creates a version-1 `hudi_gold` manifest
with source paths, row counts and SHA-256 sums. Files are staged before the final
export directory is renamed. The SQL loader checks all tables before changing any
published data, then replaces the snapshot and provenance in one transaction.
Gold jobs must retain/upsert the desired history: publication replaces a complete
SQL snapshot, not just one hour's delta. Equivalent UTC timestamp spellings share
the same key. Exports are capped at 100,000 rows per table for this laptop setup.

`synthetic_demo` data can write only `demo`; real `hudi_gold` data writes only `gold`.
Invalid real data preserves the preceding publication. Check `publication.loaded_at`
for freshness, rather than assuming that displayed old data is current.

## Shared deployment

All platform services start by default, including `part4-init`, `serving-db`,
`pipeline-runner`, `airflow` and `superset`. UI ports bind to loopback.
The runner exposes only fixed reviewed jobs, with no host Docker socket.
Secrets stay in `part4-secrets`; `pipeline-runtime` contains downloads, run manifests,
exports and logs. Neither volume belongs in Git.
