# IL-05 - GitHub Open Source Activity Analytics

**v0.1: shared starter and working ingestion-to-Bronze pipeline.**

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
```

Implemented: multi-hour GH Archive downloader, checkpointed Kafka publication, HDFS storage,
checkpointed Spark streaming, a synthetic fixture, tests, CI, and connection conventions.

**Not implemented yet:** Hudi Silver/Gold, Spark analytical jobs and KMeans,
language metadata enrichment, Airflow, Superset, and its SQL serving connection.
Empty Silver/Gold directories are reserved paths only. This is not the finished
academic submission. See [roadmap](docs/roadmap.md).

## Quick start (PowerShell or a terminal)

Requirements: Docker Desktop running Linux containers, Docker Compose, and an
internet connection for the initial images, pip package, and Spark connector jars.
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
Silver will deduplicate/upsert them in the next iteration. Do not present duplicate
raw events as additional unique GitHub activity.

## Download and publish selected GH Archive hours

Use UTC hours in `YYYY-MM-DD-HH` format. This downloads up to 100 events
per hour, then publishes and verifies the first hour.

```powershell
docker compose run --rm --no-deps producer python -m ingestion.download --hour 2025-01-01-15 --hour 2025-01-01-16 --limit 100 --output-dir data/downloads
docker compose run --rm producer python -m ingestion.producer --input data/downloads/2025-01-01-15-sample-100.jsonl --hour 2025-01-01-15 --manifest data/downloads/2025-01-01-15-sample-100.publish.json
python scripts/verify_bronze.py --expected data/downloads/2025-01-01-15-sample-100.jsonl
```

Repeat the publish command with the second hour and its corresponding paths.
For a whole archive, use `--full-hour` instead of the download limit, replace
`sample-100` with `full` in both publication paths, and leave off the producer
limit. With `--output-dir`, rerunning the same download command reuses files
already completed for that hour and volume. A single hour can still use
`--output PATH` for a custom filename.

The publish manifest records progress for the selected hour. If publication
fails, rerun the same command to replay from the first event. A successful
manifest lets the retry skip the completed publication. The [data contract](docs/data-contract.md)
describes the scheduler interface, Kafka offsets, and replay behavior. See the
[ingestion README](ingestion/README.md) for verification results.

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
- `storage/`: working Bronze streaming job; Hudi layers to follow.
- `analytics/`, `orchestration/`, `dashboard/`: documented next steps.
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
