# IL-05 - GitHub Open Source Activity Analytics

GH Archive -> Kafka -> Spark streaming -> HDFS Bronze -> Hudi Silver ->
Spark SQL / MLlib KMeans -> Hudi Gold -> PostgreSQL -> Superset.
Airflow coordinates processing. Docker Compose starts the platform.

## Start

Docker Desktop must run Linux containers. Initial builds require internet access
and several GB of disk space. Service memory limits total about 8.1 GiB; leave
headroom for Windows, Docker and batch processing.

```powershell
docker compose up --build -d
docker compose ps -a
docker compose exec airflow python /opt/project/orchestration/show_credentials.py
```

All services now start by default, including Airflow, PostgreSQL and Superset.
Wait for health checks. The last command shows the generated password for `student`.

- Airflow: http://localhost:18082
- Real Gold: http://localhost:8088/superset/dashboard/il05-gold/
- Synthetic demo: http://localhost:8088/superset/dashboard/il05-demo/
- HDFS: http://localhost:9870

Credentials and data persist in named volumes. Optional overrides are in
`.env.example`; the defaults work without a local `.env`.

## Run the complete pipeline

```powershell
docker compose exec -T airflow python -m orchestration.verify_pipeline --hour 2025-01-01-15 --limit 100
```

This triggers `il05_pipeline_manual` through the actual scheduler and waits for
all eleven tasks: download, Kafka publication, committed Bronze verification,
Silver, quality checks, analytics, KMeans, Hudi export and SQL publication.
View the output in the real Gold dashboard.

In Airflow, the same DAG accepts `source_hour`, `event_limit` and `full_hour`.
Sample mode is the default; `full_hour=true` requests an entire archive. A small
sample run does not establish full-hour capacity. Growth compares the same hour
one day earlier: ingest matching hours such as `2025-01-01-15` and `2025-01-02-15`.
Missing or zero baselines display unavailable growth, not artificial zero growth.

`il05_hourly_pipeline` has the same processing steps and an hourly UTC schedule.
It starts paused to avoid unexpected downloads; unpause it for automatic ingestion.
All tasks and retries retain the same source hour.

`il05_sample_ingestion` publishes six fictional fixture events.
`il05_refresh_demo_dashboard` refreshes only the separate 85-event SQL demo.
Known fictional sample IDs remain inspectable in Silver and are excluded from
real analytics. Fixture languages and cluster labels are not real results.

## Processing and replay

Silver validates and quarantines input, then upserts by event ID. Kafka replay can
add raw Bronze rows, but must not add unique Silver events or inflate Gold counts.
Gold jobs recompute the full current Silver snapshot. Hudi snapshot replacement
also removes obsolete aggregation keys after language enrichment changes.
Daily contributor rankings supplement the hourly dashboard table.

Language metadata is cached in the runtime volume; failed/missing lookups remain
`unknown`. KMeans saves settings and its fitted model. It requires at least three
repositories with more than one distinct activity profile.

The default language job attempts at most 30 API requests per run. For better
coverage, provide `GITHUB_TOKEN` in an uncommitted local `.env` and raise
`--max-requests` in `orchestration/job_config.json`. Unknown categories remain
in activity totals but do not count as named growing languages.

Superset uses a read-only PostgreSQL copy of four validated Hudi Gold tables.
Publication is one transaction; failure preserves the previous published snapshot.
The dashboard shows its source and publication time.

## Checks

```powershell
python -m unittest discover -s tests -v
docker compose config --quiet
docker compose exec -T pipeline-runner python3 -m orchestration.verify_serving
docker compose exec -T superset python /opt/project/dashboard/verify_superset.py --expected-real-events 100
```

Adjust the expected total to all unique real events currently in the lake.
Host tests skip Spark tests if PySpark is absent. Run them in Docker:

```powershell
docker compose exec -T pipeline-runner env 'PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/project' 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 768m pyspark-shell' python3 -m unittest discover -s tests -p test_analytics.py -v
```

Do not run another Spark batch in the runner while a pipeline task is active.
Observed results and limits are in [validation](docs/validation.md).

## Files and contracts

- [ingestion](ingestion/README.md): multi-hour downloads and Kafka publication.
- [storage](storage/README.md): streaming, Silver, quarantine and quality checks.
- [analytics](analytics/README.md): SQL metrics, growth and KMeans.
- [orchestration](orchestration/README.md): DAGs and pipeline verification.
- [dashboard](dashboard/README.md): SQL publication and reproducible charts.
- [Data contract](docs/data-contract.md) and [integration details](docs/part4-integration.md).

Services connect using Docker names (`kafka`, `namenode`, `serving-db`).
`localhost` inside a container refers to that container.

## Stop, resume and troubleshoot

```powershell
docker compose down
docker compose up -d
docker compose logs --tail 100 airflow pipeline-runner spark-bronze superset
```

Ordinary shutdown preserves volumes and credentials. Keep checkpoints with their
sinks. Initializers and the sample producer finish with exit 0; other services
stay running. Exit 137 usually indicates memory pressure. Spark logs are in
`pipeline-runtime/runs/<run-hash>/`. Streaming metrics are described in storage/.

This is a local academic deployment with loopback-only host ports, one Kafka
broker and one HDFS DataNode. It is not configured for public hosting.

Repository: https://github.com/UssamaRei/github-activity-analytics
