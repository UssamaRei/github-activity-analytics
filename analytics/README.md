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
