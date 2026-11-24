# Part 4 - Airflow and pipeline coordination

Start from the repository root:

```powershell
docker compose --profile part4 up --build -d
docker compose --profile part4 ps -a
docker compose --profile part4 exec airflow python /opt/project/orchestration/show_credentials.py
```

Open **http://localhost:18082**. The last command displays the local `student`
password used by both Airflow and Superset. Credentials are generated once in a
Docker volume; they are not committed to Git. Override the port with `AIRFLOW_PORT`.

## What to run first

Unpause `il05_sample_ingestion` in Airflow, then press Trigger. Its real tasks are:

1. `publish_six_sample_events`: publish the existing fixture and remember the acknowledged Kafka offsets.
2. `verify_committed_bronze`: wait until Spark's **committed** HDFS checkpoint reaches those offsets. Old sample IDs are not sufficient.

Retrying within the same run reuses its completed publication manifest. A new DAG
run deliberately replays the fixture. A crash before saving the manifest can replay
a partial batch: Bronze is at least once; Silver must deduplicate event IDs.

`il05_refresh_demo_dashboard` refreshes only the synthetic SQL schema. It never
publishes synthetic rows into real `gold`.

## Complete real-data pipeline

All services start with `docker compose up --build -d`; the old explicit
`--profile part4` commands remain compatible. Run the eleven steps manually:

```powershell
docker compose exec -T airflow python -m orchestration.verify_pipeline --hour 2025-01-01-15 --limit 100
```

`il05_pipeline_manual` accepts `source_hour`, `event_limit` and `full_hour` and
uses exactly the same task chain as the hourly DAG. It avoids starting automatic
hourly downloads while testing a selected historical hour.

`il05_hourly_pipeline` is **paused by default**:

```text
check integrations -> download UTC hour -> Kafka -> committed Bronze
    -> Silver -> Silver quality -> repository/contributor Gold
    -> language Gold -> KMeans -> validated Gold export -> SQL publication
```

Preflight checks the integrated scripts. Jobs fail with nonzero exit codes;
placeholders and demo charts cannot substitute for real pipeline results.
`job_config.json` maps the actual commands:

| Job | Script | Argument |
| --- | --- | --- |
| Silver | `storage/silver_job.py` | `--hour YYYY-MM-DD-HH` |
| Silver quality | `storage/check_silver.py` | same |
| Repository and contributor metrics | `analytics/repository_metrics.py` | same |
| Language metrics | `analytics/language_metrics.py` | same |
| Repository clustering | `analytics/cluster_repositories.py` | same |

These scripts are integrated and return nonzero on failure. The runner uses
argument lists, not shell strings, and needs no Docker socket. It supplies Spark
3.5.1 and the matching Hudi bundle; teammates can import their shared Hudi utilities.

Unpause the hourly DAG when automatic ingestion is wanted. Its UTC interval is the preceding
completed hour, catchup is disabled, and only one run is active at a time. The
manual-run fallback uses the completed UTC hour before the run's timestamp; all
tasks and retries keep that same hour. The current
default is a **bounded sample**, 1,000 events, exposed as `event_limit`.
Set `full_hour=true` to use Part 1's complete-archive downloader. Full-hour
throughput needs a separate capacity test. Silver and Gold scan current history;
Gold replaces full Hudi snapshots so outdated aggregate keys cannot linger.
KMeans initially retrains in each integrated run; split its schedule later if the
model lifetime or volume requires it.

## Files and recovery

- `dags/il05_workflows.py`: schedules and dependencies.
- `runner.py`: fixed ingestion, Spark and publication jobs.
- `webhdfs.py`: committed HDFS checkpoint verification.
- `job_config.json`: adapter to Parts 2/3.
- `init_secrets.py`, `connection_uri.py`: local credentials and DB URLs.

Airflow 3.4 runs in local standalone mode with PostgreSQL metadata and low
parallelism. The Bronze stream remains a separate long-running Compose service.

```powershell
docker compose --profile part4 logs --tail 100 airflow pipeline-runner
docker compose --profile part4 exec airflow airflow dags list-import-errors --output json
```

Spark job logs and manifests are in the `pipeline-runtime` volume under
`runs/<run-hash>/`. Ordinary `down` preserves state. Deleting the project volumes
also deletes credentials, metadata, Kafka and HDFS data.

## Repeat the Part 4 checks

Wait until Airflow, Superset and the runner show `healthy` in Compose, then run:

```powershell
docker compose --profile part4 exec -T airflow python -m orchestration.verify_airflow
docker compose --profile part4 exec -T pipeline-runner python3 -m orchestration.verify_serving
docker compose --profile part4 exec -T superset python /opt/project/dashboard/verify_superset.py --expected-real-events 100
```

The first command triggers two actual DAG runs. It publishes six additional raw
sample events, refreshes the synthetic SQL snapshot, restores the sample/demo
DAGs' previous pause settings, and confirms the hourly DAG is paused. Retries of
the same publication reuse its manifest; separate verification runs deliberately
replay the fixture. It also checks the now-integrated scripts through preflight.

The SQL check injects a failure midway through publication and checks rollback,
provenance preservation and the read-only account. The Superset check authenticates
without displaying the password and verifies totals, rankings and UTC filtering.
Adjust the expected real-event total to all unique real events in your lake.

For startup problems, run:

```powershell
docker compose --profile part4 logs --tail 100 airflow superset pipeline-runner
```

Initialization can take several minutes on a laptop.
All running project containers currently use roughly 3.3 GiB at idle; Spark batch
jobs and startup need additional memory. The combined service memory limits are
about 8.1 GiB, excluding one-shot initializers. Leave Windows and Docker headroom.

[Airflow reference](https://airflow.apache.org/docs/apache-airflow/3.4.0/start.html).
