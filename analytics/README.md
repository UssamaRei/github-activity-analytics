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
  Temporary API failures are not cached so they are retried. Lookups go to the
  busiest repositories first and stop at a rate limit; each run prints language
  coverage (share of events with a known language). Set `GITHUB_TOKEN` (5,000
  lookups/hour instead of 60) and a larger `--max-requests` for real data.
- **KMeans features** are `log1p` counts + event-type shares, standardized; K chosen
  by silhouette (seed 42). Labels are generated from each cluster's mean features
  relative to the overall mean, and saved in the settings JSON.
