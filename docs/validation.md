# v0.1 verification

Verified locally on 17 November 2026 using Docker Desktop Linux containers.
These are small functional checks, not performance or production-readiness tests.

| Check | Observed result |
| --- | --- |
| Python ingestion and download tests | 7 tests passed |
| Compose configuration | Valid |
| Fresh project volumes and Compose startup | All initializers and the sample producer exited 0; Kafka/HDFS healthy; Spark streaming running |
| Synthetic sample | All 6 original JSON records present unchanged in Bronze |
| Bounded real GH Archive download, UTC hour `2025-01-01-15` | 20 events downloaded and published; all 20 present unchanged in Bronze |
| Spark restart plus deliberate sample replay | Raw count increased from 26 to exactly 32; previous offsets were not reprocessed |
| Git contents | Downloaded data and local environment files excluded |

The replay leaves duplicate sample IDs in raw Bronze intentionally. Deduplication
belongs to the future Silver Hudi layer. No deduplication or Hudi behavior has been
claimed as tested in this version.

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

**Pending upstream integration:** reading actual Part 3 Hudi Gold tables with the
Spark exporter, executing the complete hourly pipeline, and validating real KMeans
results. These require the Silver/analytics jobs; demo data does not establish
that they work. The current downloader remains a bounded hourly sample. No
performance, complete-archive or production deployment claim is made.
