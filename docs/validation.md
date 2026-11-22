# v0.1 baseline verification (17 November 2026)

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

## Current Part 2 verification (22 November 2026)

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
