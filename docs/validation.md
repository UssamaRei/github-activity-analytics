# Verification history

## Integrated v0.3 - 24 November 2026

Merged the Part 1, Part 2, analytics/ML and Part 4 branches into
`codex/integrated-pipeline`, retaining their commit histories. Fixed the actual
integration gaps: Python 3.8 compatibility, ML dependencies, the missing Silver
quality gate, executable arguments, hourly contributor output, language/cluster
columns, complete Gold snapshots, synthetic-data isolation and dashboard growth.

| Check | Observed result |
| --- | --- |
| Real Airflow run `integration-20261124-first` | All 11 tasks succeeded; 100 real events from UTC hour `2025-01-01-15` reached SQL Gold |
| Real Airflow run `integration-20261124-second-day` | All 11 tasks succeeded; another 100 real events from `2025-01-02-15` published successfully |
| Raw preservation | All 200 real source JSON lines found byte-for-byte unchanged in Bronze |
| Hudi Silver upserts | First run: 168 raw -> 106 unique events; second: 268 raw -> 206 unique events, including the six explicit fixtures |
| Real Gold totals | 200 events, split 100/100 across the two selected UTC hours; language and contributor totals reconcile |
| Synthetic isolation | The six Bronze/Silver demo IDs are excluded from real analytics; SQL demo remains 85 events |
| Real KMeans | 183 repositories; K=2 selected by silhouette 0.9558 in the second run; fitted pipeline saved on HDFS |
| Growth | Same-hour previous-day views validated; example TypeScript 1 -> 4 sampled events gives 300% growth |
| SQL reliability | Invalid publications and injected mid-transaction failures preserve data/provenance; dashboard reader cannot write |
| Superset API/browser | Real total 200; native UTC filter gives 100; demo gives 85; 27 browser chart requests succeeded without query errors |
| Unit/functional tests | 49 non-Spark tests passed on the host; all eight Spark analytics tests passed in Docker, including KMeans and hourly contributors; the five ingestion tests also passed under Docker Python 3.8 |
| Deployment | `docker compose up --build -d` starts the complete platform by default; configuration is valid |
| Streaming observations | Structured progress logs report committed offsets, batch counts, rates and query status |

The optional standalone Silver service remains available, but complete processing
uses the runner. All automatic hourly downloads remain paused until explicitly
enabled; the full manual DAG exercises exactly the same eleven tasks.

### What is still needed for the final submission

- Test complete hourly archives and a larger multi-hour dataset. The verified
  live inputs here were two 100-event samples, not complete archives. Current
  serving export limits are 100,000 rows per table; batch jobs use laptop memory.
- Improve language coverage. 158 of 200 sampled events (79%) remain `unknown`
  because metadata is missing or not yet fetched. The default API budget is 30
  lookups per run; optional `GITHUB_TOKEN` forwarding and cache backfill are
  supported. More recent repositories can also improve metadata availability.
- Verify/finalize the cahier des charges PDF, write the final results-based report
  and slides, and rehearse startup, automatic scheduling and each member's demo.
  These are submission requirements from the original workbook (Guide general,
  deliverables section). This code integration does not create those documents.

Small-sample model scores and observed batch rates do not establish large-volume
capacity or representative GitHub-wide trends. Known missing values stay visible.

## v0.1 baseline verification (17 November 2026)

Historical Bronze-only verification on 17 November 2026 using Docker Desktop Linux
containers. These are small functional checks, not performance or
production-readiness tests.

| Check | Observed result |
| --- | --- |
| Python ingestion and download tests | 7 tests passed |
| Compose configuration | Valid |
| Fresh project volumes and Compose startup | All initializers and the sample producer exited 0; Kafka/HDFS healthy; Spark streaming running |
| Synthetic sample | All 6 original JSON records present unchanged in Bronze |
| Bounded real GH Archive download, UTC hour `2025-01-01-15` | 20 events downloaded and published; all 20 present unchanged in Bronze |
| Spark restart plus deliberate sample replay | Raw count increased from 26 to exactly 32; previous offsets were not reprocessed |
| Git contents | Downloaded data and local environment files excluded |

## Historical Silver/Hudi QA (19 November 2026)

Run against the actual Docker Compose stack after `docker compose up -d --build`.
The validation job is rerunnable: Bronze retains replay duplicates and Hudi keeps
one current row per `event_id`.

| Requirement | Command/evidence | Result |
| --- | --- | --- |
| Original JSON preserved in Bronze | `python3 scripts/verify_bronze.py --timeout 60` | 6 sample lines found byte-for-byte; 6 unique IDs |
| Checkpoint recovery | `docker compose restart spark-bronze`; verify Bronze remained at 6 rows; `docker compose run --rm producer`; HDFS count rose to 12 | Existing offsets were not replayed; 6 new offsets were consumed |
| Sample valid Silver records | `docker compose run --rm spark-silver`; Part 3 reader | 6 unique Hudi rows, with counts 3 Push, 2 Issues, 1 PullRequest |
| Replay is idempotent in current Silver | Replay sample with `docker compose run --rm producer`, run `docker compose run --rm spark-silver`, then Part 3 reader | Bronze rose to 26 rows; Silver remained at 6 unique records |
| Invalid-event quarantine | Publish `not-json` and an event with a naive timestamp to `github_events`, then run `spark-silver`; inspect `/lake/quarantine/github_events` | Both raw values quarantined with `invalid JSON...` and `created_at must contain a timezone` reasons |
| Hudi upserts | Re-run `docker compose run --rm spark-silver` on the replayed Bronze input | Hudi completed a Copy-on-Write upsert keyed by `event_id`; Part 3 still read 6 current rows |
| Silver schema | Part 3 reader validates column names and Spark types against `docs/data-contract.md` | Passed |
| Part 3 read | `docker compose run --rm --entrypoint /opt/spark/bin/spark-submit spark-silver --master 'local[2]' --driver-memory 768m --conf spark.jars.ivy=/tmp/ivy --conf spark.jars.ivySettings=/opt/project/storage/ivysettings.xml --conf spark.driver.host=spark-silver --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 /opt/project/analytics/read_silver.py` | `PASS: Part 3 read 6 unique Silver Hudi records.` |
| Python tests and Compose configuration | `python3 -m unittest discover -s tests -v`; `docker compose config --quiet` | 10 tests passed; Compose valid |

Quarantine is append-only. The QA run repeated Silver processing while correcting
an Hudi/Hadoop compatibility issue, so the final quarantine files contain
repeated instances of the two test messages; their reasons remain explicit.
This verification describes the earlier Silver configuration and is functional
evidence only, not a performance or production-readiness result. The current
contract uses `event_date` partitions and deterministic `record_order`; its
container-level Hudi behavior still needs an integration run.

## Historical Part 2 verification (22 November 2026)

| Check | Result |
| --- | --- |
| Python unit suite, including Silver validation, deterministic deduplication, Hudi writer options, and replay model | 24 tests passed with `python3 -m unittest discover -s tests -v` |
| Compose configuration | Valid with `docker compose config --quiet` |
| Python syntax compilation | Passed with `python3 -m compileall -q storage analytics tests` |
| Whitespace validation | Passed with `git diff --check` |
| Live HDFS/Spark/Hudi verification of the current partitioned table | Not run: Docker daemon was unavailable (`Cannot connect to the Docker daemon`) |

The Python replay test verifies deterministic current-record selection and the
fixture's six IDs. It does not substitute for a real Hudi/HDFS replay test. Run
the manual sequence in the root README after starting Docker to verify the
current `event_date` partition and global-index upsert behavior.

To repeat the main checks, run the commands in the root README. GitHub Actions is
configured for unit tests, dry-run ingestion, and Compose configuration validation.
Its remote status is available in the repository's Actions tab after a push;
the local results above do not establish hosted CI success.

## v0.2 Part 4 verification - 23 November 2026

Verified locally on the existing Docker volumes after a service restart.

| Check | Observed result |
| --- | --- |
| Python tests | 21 passed, including equivalent UTC keys and BIGINT bounds |
| Compose | Base and `part4` profile configurations valid |
| Airflow DAG imports | Three DAGs; no import errors |
| Actual sample DAG run | Both tasks succeeded on their first attempt; the new Kafka offsets reached a committed Bronze batch |
| Actual demo-refresh DAG run | Succeeded on its first attempt; synthetic SQL snapshot refreshed |
| Integration preflight | Listed all five missing Parts 2/3 jobs; hourly DAG remained paused |
| UTC hour selection | Scheduled runs preserve their interval; manual fallback selects the completed hour before the run timestamp, consistently across tasks/retries |
| SQL publication | Invalid totals rejected; deliberate failure after the first table insert rolled back data and provenance |
| SQL permissions | Dashboard reader can SELECT and cannot DELETE |
| Superset authenticated API | Demo total 85; repository/language/contributor totals correct; one-hour total 35; synthetic provenance visible |
| Browser dashboard check | Seven charts loaded without chart-query errors; native UTC filter produced 35 events while provenance remained visible |
| Real Gold dashboard | Empty results and `No data`, with an explicit explanation; no synthetic rows published into Gold |
| Repeatable Superset setup | Bootstrap rerun updated the existing datasets, charts and dashboards |

Repeat the service checks with the commands in
[orchestration/README.md](../orchestration/README.md#repeat-the-part-4-checks).
The browser check used local headless Chromium. Superset's optional service-worker
asset returned 404; ordinary page loading and all chart queries still succeeded.

**At the time of v0.2, pending upstream integration:** reading actual Part 3 Hudi Gold tables with the
Spark exporter, executing the complete hourly pipeline, and validating real KMeans
results. These require the Silver/analytics jobs; demo data does not establish
that they work. The current downloader remains a bounded hourly sample. No
performance, complete-archive or production deployment claim is made.
