# Analytics

`read_silver.py` is the Part 3 Spark/Hudi integration check. It loads the Silver
table, verifies the documented schema, and reports a count by event type. Run it
after `docker compose run --rm spark-silver`:

```sh
docker compose run --rm --entrypoint /opt/spark/bin/spark-submit spark-silver \
  --master local[2] \
  --driver-memory 768m \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.jars.ivySettings=/opt/project/storage/ivysettings.xml \
  --conf spark.driver.host=spark-silver \
  --py-files /opt/project/storage/silver_contract.py,/opt/project/storage/hudi_io.py \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  /opt/project/analytics/read_silver.py
```

Gold Hudi aggregates, repository feature engineering, and MLlib KMeans remain
planned. See `docs/data-contract.md` for the shared schema and analytics
definitions.

## Handoff contract

- Hudi table: `github_events_silver`
- HDFS base path: `hdfs://namenode:9000/lake/silver/github_events`
- Record key: `event_id`; partitions: UTC `event_date`
- Current columns: `event_id` (string), `event_type` (string), `actor_id`
  (bigint, nullable), `actor_login` (string), `repository_id` (bigint, nullable),
  `repo_name` (string), `created_at` (UTC timestamp), `event_date` (date),
  `ingested_at` (timestamp), `payload_json` (exact original event JSON), and
  `record_order` (string used for deterministic Hudi precombine).

The first sample Silver row has `event_id=demo-001`, `event_type=PushEvent`,
`actor_login=demo-alice`, `repository_id=201`, `repo_name=demo-org/alpha`, and
`created_at=2025-01-01 15:00:00 UTC`. Its `event_date` is `2025-01-01`; its
`payload_json` preserves the full corresponding line in the sample fixture.
`ingested_at` reflects when that run executes.

For a Spark application launched with Hudi and `hudi_io.py` on `--py-files`:

```python
from hudi_io import read_silver

events = read_silver(
    spark,
    "hdfs://namenode:9000/lake/silver/github_events",
)
events.select("event_id", "event_type", "repo_name", "created_at").show()
```

`analytics/read_silver.py` is the completion check: it verifies required column
names and Spark types, rejects null or duplicate event IDs, then prints the
current row count and counts by event type. Replaying the same source IDs updates
the current Hudi rows instead of adding another current row per ID.
# Analytics (Part 3): Silver -> Gold

Spark SQL aggregates, language enrichment and MLlib KMeans. Each job reads the
Silver table, computes one or more Gold tables, and writes them with a record key
so a rerun **upserts** instead of appending (reprocessing never inflates results).

| Job | Gold table(s) | Module |
| --- | --- | --- |
| Repository metrics | `repository_activity_hourly`, `contributor_rankings_daily` | `analytics/repository_metrics.py` |
| Language metrics | `language_activity_hourly` | `analytics/language_metrics.py` |
| Clustering | `repository_clusters` (+ settings JSON) | `analytics/cluster_repositories.py` |
| Shared I/O | Silver reader, Gold writer, table keys | `analytics/gold_io.py` |
| Test data | six-event fixture, 12-repo clustering fixture, fictional languages | `analytics/fixtures.py` |

Schemas and metric definitions: [docs/data-contract.md](../docs/data-contract.md#gold-tables).
Example exports produced from the **test fixtures** (not real activity):
[data/samples/gold/](../data/samples/gold/).

## What this part expects (inputs)

- **From Part 2:** Hudi table `github_events_silver` at
  `hdfs://namenode:9000/lake/silver/github_events`, one row per `event_id`, with the
  Silver columns in the data contract (`created_at` UTC timestamp, `event_date` date).
  If Part 2 provides `storage/hudi_io.py` with `read_silver(spark, path)`, the jobs use
  it automatically (`--source hudi`). The jobs still deduplicate on `event_id` defensively.
- **Hudi bundle:** the same Hudi version Part 2 selects for Spark 3.5.1.
- **Network (language job only):** `api.github.com`. Optional `GITHUB_TOKEN`
  environment variable for a higher rate limit. Never commit a token.

## Quick local run (fixtures, no Docker, no HDFS)

Requires Python 3.11+, Java 17+ and `pip install pyspark==3.5.1`.

```bash
python -m unittest tests.test_analytics -v
python -m analytics.repository_metrics --source fixture --output tmp/gold --format local-csv
python -m analytics.language_metrics --source fixture --languages fixture --output tmp/gold --format local-csv
python -m analytics.cluster_repositories --source cluster-fixture --output tmp/gold --format local-csv --settings tmp/gold/kmeans_settings.json
```

Expected for the six-event fixture: `demo-org/alpha` 4 events / 3 contributors,
`demo-org/beta` 2 / 2. Clustering fixture: K=3 chosen, one cluster per profile.

## Commands for Part 4 (Airflow)

All jobs are **finite batch jobs**: exit code `0` on success, non-zero on any error
(a Python exception fails the Spark driver). Run them **after** the Silver job for
the same hour has finished.

| Task | Command (relative to the project mount) | Schedule |
| --- | --- | --- |
| Repository metrics | `analytics/repository_metrics.py --source hudi --format hudi` | hourly, after Silver |
| Language metrics | `analytics/language_metrics.py --source hudi --format hudi` | hourly, after Silver |
| KMeans retraining | `analytics/cluster_repositories.py --source hudi --format hudi --start <YYYY-MM-DD> --end <YYYY-MM-DD> --settings /opt/project/data/models/kmeans_settings.json` | daily or weekly |

Defaults: `--silver hdfs://namenode:9000/lake/silver/github_events`,
`--output hdfs://namenode:9000/lake/gold`. Language job options: `--offline` (cache
only), `--max-requests 500` (API calls per run), `--cache data/cache/repo_languages.json`.
Clustering options: `--k N` forces K, `--max-k 6`, `--model-path hdfs://...` saves the
fitted pipeline.

Suggested `spark-submit` inside the existing `apache/spark:3.5.1` image (mount the
repository at `/opt/project`, working directory `/opt/project`, reuse the Ivy cache):

```bash
/opt/spark/bin/spark-submit --master local[2] --driver-memory 1g \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.jars.ivy=/tmp/ivy \
  --py-files /opt/project/analytics.zip \
  /opt/project/analytics/repository_metrics.py --source hudi --format hudi
```

`analytics.zip` is `analytics/` zipped (`python -m zipfile -c analytics.zip analytics`)
so imports such as `from analytics.gold_io import ...` resolve on the driver.
Align the Hudi bundle version with Part 2 before merging; the Compose service is
for Part 4 to add, so edits to the shared `docker-compose.yml` stay coordinated.

## Design decisions

- **Growth** compares the same UTC hour one day earlier (self-join, not `lag()`,
  because quiet hours have no row). Null when there is no earlier row or it was 0.
- **Rankings** use `rank()`: ties share a rank (1, 1, 3).
- **Language** comes only from GitHub metadata; missing -> `unknown`, never guessed.
  Temporary API failures are not cached so they are retried.
- **KMeans features** are `log1p` counts + event-type shares, standardized; K chosen
  by silhouette (seed 42). Labels are generated from each cluster's mean features
  relative to the overall mean, and saved in the settings JSON.
