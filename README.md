# IL-05 - GitHub Open Source Activity Analytics

**v0.1: shared starter and working ingestion-to-Silver pipeline.**

Université Ibn Zohr - Faculté des Sciences Agadir
Master Excellence Ingénierie Logicielle - Module Big Data
Professor: Yassine Oukdach - Academic year: 2026-2027

**Responsibilities are not assigned yet.**

The final project analyzes public GitHub events to show repository, contributor,
and language activity trends and cluster repositories with MLlib KMeans.

## What this version does

```text
Six fictional sample events (or a real GH Archive sample)
    -> Python producer
    -> Apache Kafka topic github_events
    -> Spark Structured Streaming
    -> HDFS Bronze (original JSON records)
    -> rerunnable Spark validation job
       -> Hudi Silver (deduplicated current events)
       -> HDFS quarantine (invalid raw records and reasons)
```

Implemented: bounded GH Archive downloader, raw-event Kafka producer, HDFS Bronze,
checkpointed Spark streaming, rerunnable Hudi Silver upserts, invalid-event
quarantine, a Part 3 Hudi reader, a synthetic fixture, tests, CI, and conventions.

**Not implemented yet:** Hudi Gold, Spark analytical jobs and KMeans,
language metadata enrichment, Airflow, Superset, and its SQL serving connection.
Gold directories are reserved paths only. This is not the finished academic
submission. See [roadmap](docs/roadmap.md).

## Quick start (PowerShell or a terminal)

Requirements: Docker Desktop running Linux containers, Docker Compose, and an
internet connection for the initial images, pip package, Spark connector jars,
and Hudi package.
Check free disk space first. Initial downloads can require several GB on a clean
machine. The running demo is configured for roughly 4 GB of container memory limits;
leave headroom for Docker and Windows. Ports 29092 and 9870 must be available.

From this directory:

```powershell
docker compose up --build -d
docker compose ps -a
docker compose logs -f spark-bronze
```

The first startup may take several minutes. The `producer`, `kafka-init`,
`hdfs-init`, and `spark-cache-init` containers should finish with **exit code 0**;
they are one-shot tasks.
Kafka, NameNode, DataNode, and the streaming consumer keep running.
The producer publishes the six fictional sample events automatically.

### Monitor Bronze streaming

Each query lifecycle/status update and completed micro-batch is logged as a
timestamped `STREAMING_METRIC` JSON record. The records include input counts,
rates, trigger execution duration, and Kafka offset ranges. To summarize recent
batches:

```sh
docker compose logs --since 30m --no-color spark-bronze | python3 scripts/streaming_report.py
```

Inspect live records with `docker compose logs -f --no-color spark-bronze`.
Check container resource use with
`docker stats --no-stream spark-bronze kafka namenode datanode` and Docker's
overall allocation with `docker info`. Monitoring is log-only; it does not alter
the Bronze checkpoint or the existing 5-second trigger.

Build Silver and verify that replayed IDs do not increase its current row count:

```powershell
docker compose run --rm spark-silver
docker compose run --rm --entrypoint /opt/spark/bin/spark-submit spark-silver --master 'local[2]' --driver-memory 768m --conf spark.jars.ivy=/tmp/ivy --conf spark.jars.ivySettings=/opt/project/storage/ivysettings.xml --conf spark.driver.host=spark-silver --py-files /opt/project/storage/silver_contract.py,/opt/project/storage/hudi_io.py --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 /opt/project/analytics/read_silver.py
docker compose exec namenode hdfs dfs -ls -R /lake/silver/github_events
```

The Part 3 reader checks the Silver schema, rejects null or duplicate event IDs,
and reports the current row count and event-type counts. To verify replay safety,
replay the six sample records, rerun `spark-silver`, and run the reader again; the
fixture produces six current event IDs regardless of Bronze replay copies. See
[analytics/README.md](analytics/README.md) for the reader interface and schema.

### Part 2 manual verification sequence

```sh
docker compose up --build -d
# hdfs-init runs automatically; this confirms the directories are present.
docker compose exec namenode hdfs dfs -ls /lake/bronze /lake/silver /lake/quarantine
python3 scripts/verify_bronze.py
docker compose exec namenode hdfs dfs -cat '/lake/bronze/github_events/part-*'
docker compose run --rm spark-silver
docker compose exec namenode hdfs dfs -ls -R /lake/silver/github_events
# Run the Spark/Hudi reader command shown above.
docker compose run --rm producer
docker compose run --rm spark-silver
# Run the Spark/Hudi reader again; current Silver should still report 6 sample IDs.
docker compose restart spark-bronze
docker compose run --rm producer
python3 scripts/verify_bronze.py
```

The final restart/replay check confirms the stream resumes from its HDFS
checkpoint and consumes new offsets without replaying prior offsets. Bronze
contains additional raw rows after the explicit producer runs; Silver remains
one current row per event ID.

With Python 3.11+ installed on the host, verify the result:

```powershell
python scripts/verify_bronze.py
```

Expected: `PASS: 6 expected raw records found unchanged in Bronze (6 unique IDs).`
The script waits up to 180 seconds. It checks actual raw records in HDFS, not just
whether containers started. To inspect manually:

```powershell
docker compose exec namenode hdfs dfs -ls /lake/bronze/github_events
docker compose exec namenode hdfs dfs -cat '/lake/bronze/github_events/part-*'
```

HDFS NameNode interface: http://localhost:9870 (file browser and storage information;
this is an infrastructure interface, not the future analytics dashboard).

## Replay the sample

```powershell
docker compose run --rm producer
python scripts/verify_bronze.py
```

A replay intentionally creates more raw records with the same event IDs in Bronze.
Re-run `docker compose run --rm spark-silver` to upsert the repeated IDs without
increasing the number of current Silver records. Do not present duplicate raw
events as additional unique GitHub activity.

## Try a small real GH Archive sample

The downloader stops after the requested number of events instead of saving an
entire hourly archive. The following uses a fixed historical hour in UTC:

```powershell
docker compose run --rm --no-deps producer python -m ingestion.download --hour 2025-01-01-15 --limit 100 --output data/downloads/2025-01-01-15-sample.jsonl
docker compose run --rm producer python -m ingestion.producer --input data/downloads/2025-01-01-15-sample.jsonl --limit 100
python scripts/verify_bronze.py --expected data/downloads/2025-01-01-15-sample.jsonl
```

Choose another filename if the output already exists. All downloaded data is ignored
by Git. GH Archive contains public activity across repositories; it is independent
of the GitHub repository used to collaborate on this project.

## Fast local checks (no Docker services or pip packages required)

```powershell
python -m unittest discover -s tests -v
python -m ingestion.producer --input data/samples/github_events.jsonl --dry-run
docker compose config --quiet
```

Expected dry-run counts: 6 events; 3 pushes, 2 issue events, 1 pull request event.
GitHub Actions is configured to run these lightweight checks on pushes and pull requests.
CI does not run the container integration test.
Local integration results are recorded in [validation](docs/validation.md).

## Stop and resume

```powershell
docker compose down
docker compose up -d
```

Named volumes preserve Kafka, HDFS, and Spark dependency downloads. On subsequent
startup, the sample producer can run again, so raw replay duplicates are normal.
The Spark job resumes its HDFS checkpoint. Keep output and checkpoints consistent.
For a deliberate **complete reset of this project's data only**, use
`docker compose down --volumes`. That command deletes this Compose project's
Kafka records, HDFS files, checkpoints, and cached connector jars.

## Structure and shared conventions

- `ingestion/`: downloader, JSONL reader, and Kafka producer.
- `storage/`: Bronze streaming and the Silver Hudi upsert/quarantine job.
- `analytics/`: Part 3 Silver reader; Gold analytics remain future work.
- `orchestration/`, `dashboard/`: documented next steps.
- `docs/`: [architecture](docs/architecture.md), [data contract](docs/data-contract.md), [roadmap](docs/roadmap.md).
- `tests/`: meaningful ingestion and bounded-download checks.

The Compose network uses service names such as `kafka` and `namenode`.
`localhost` inside a container points to that container, not another service.
Optional port/image overrides are documented in `.env.example`; copy it to `.env`
only if needed. The default setup works without an `.env` file.

## Troubleshooting

- `docker compose logs kafka` / `namenode` / `datanode` / `spark-bronze` show the failing layer.
- A first Spark startup downloads the matching Kafka connector and its dependencies.
  If Maven is unreachable, fix connectivity and run `docker compose restart spark-bronze`.
- If a service exits with code 137, check Docker/host memory and close unnecessary apps.
- If the NameNode starts but writes fail, check the DataNode and free disk space.
- If ports are occupied, set `KAFKA_PORT` or `HDFS_UI_PORT` in `.env`.
- This is an isolated local academic demo: single broker, one DataNode, no service
  authentication, no encryption, and a legacy Hadoop image. Host ports bind to
  loopback. It is not configured for public deployment.

## GitHub handoff

Repository: [UssamaRei/github-activity-analytics](https://github.com/UssamaRei/github-activity-analytics).
Read [CONTRIBUTING.md](CONTRIBUTING.md) before making changes. Each member should
clone the repository and use their own Git account. Collaborator invitations and
the division of responsibilities are to be decided by the group.

```powershell
git clone https://github.com/UssamaRei/github-activity-analytics.git
cd github-activity-analytics
docker compose up --build -d
```

Technical references are collected in [docs/architecture.md](docs/architecture.md).
