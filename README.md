# IL-05 - GitHub Open Source Activity Analytics

**v0.2: ingestion-to-Bronze plus Part 4 Airflow/Superset integration.**

Université Ibn Zohr - Faculté des Sciences Agadir
Master Excellence Ingénierie Logicielle - Module Big Data
Professor: Yassine Oukdach - Academic year: 2026-2027

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

Implemented: bounded GH Archive downloader, raw-event Kafka producer, HDFS storage,
checkpointed Spark streaming, a synthetic fixture, tests, CI, and connection conventions.

**Not implemented yet:** Hudi Silver/Gold, Spark analytical jobs and KMeans,
and language metadata enrichment. Airflow, Superset and PostgreSQL serving are
available through the `part4` Compose profile. Real Gold stays empty until the
upstream jobs are connected.
Empty Silver/Gold directories are reserved paths only. This is not the finished
academic submission. See [roadmap](docs/roadmap.md).

## Part 4: automation and dashboard

```powershell
docker compose --profile part4 up --build -d
docker compose --profile part4 exec airflow python /opt/project/orchestration/show_credentials.py
```

The second command displays generated local credentials for username `student`.

- Airflow: http://localhost:18082
- Superset demo: http://localhost:8088/superset/dashboard/il05-demo/
- Superset real Gold: http://localhost:8088/superset/dashboard/il05-gold/

Trigger `il05_sample_ingestion` first. It publishes six events and checks their
committed Bronze offsets. The full hourly DAG stays paused until the actual
Silver/analytics jobs exist and reports missing dependencies explicitly.
The demo has fictional data (85 events); real Gold starts empty. Read the
[Airflow instructions](orchestration/README.md), [dashboard instructions](dashboard/README.md)
and [integration contract](docs/part4-integration.md).

The profile adds downloads and memory use. Base ingestion below works alone.
Set `COMPOSE_PROFILES=part4` in a local `.env` if ordinary `docker compose up`
should include the full local platform.

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
- `storage/`: working Bronze streaming job; Hudi layers to follow.
- `analytics/`: interfaces for the upcoming Gold jobs.
- `orchestration/`: Airflow workflows, private job runner and integration checks.
- `dashboard/`: Gold export, transactional PostgreSQL publication and Superset dashboards.
- `docs/`: [architecture](docs/architecture.md), [data contract](docs/data-contract.md), [roadmap](docs/roadmap.md).
- `tests/`: ingestion, download, publication-contract and pipeline-handoff checks.

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
each member's responsibilities are agreed within the group.

```powershell
git clone https://github.com/UssamaRei/github-activity-analytics.git
cd github-activity-analytics
docker compose up --build -d
```

Technical references are collected in [docs/architecture.md](docs/architecture.md).
