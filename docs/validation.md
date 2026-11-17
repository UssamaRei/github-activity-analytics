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
