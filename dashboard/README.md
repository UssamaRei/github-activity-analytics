# Part 4 - Superset and SQL serving

Start with `docker compose --profile part4 up --build -d`, then open
**http://localhost:8088**. Sign in as `student` using the generated password from
the credentials command in `orchestration/README.md`.

Startup creates the database connection, fourteen datasets, eighteen charts and two
dashboards. Restarting updates the same objects; no manual chart setup is required.

## Two separate dashboards

- **DEMO:** http://localhost:8088/superset/dashboard/il05-demo/
  Reads `demo`. Events, languages and cluster labels are fictional and explicitly
  labeled. The fixture totals 85 events over two hours. The labels are not ML results.
- **Gold:** http://localhost:8088/superset/dashboard/il05-gold/
  Reads `gold`, populated by the integrated Airflow pipeline's validated Hudi export.
  Empty charts do not mean healthy zero activity; no publication row exists until
  the first real snapshot is loaded.

Charts show event totals, hourly activity, repository rankings, languages,
contributors, groups, repository/language growth and publication provenance.
Growth compares the same hour one day earlier; missing/zero baselines yield NULL.
A UTC time filter is included;
the provenance table stays unfiltered. Repository groups include their snapshot
time so historical clustering runs can be distinguished.

## The SQL serving connection

```text
Hudi Gold in HDFS -> Spark export -> validation -> PostgreSQL snapshot -> Superset
```

Hudi remains authoritative; PostgreSQL is a serving copy. Superset uses a read-only
database role. The writer publishes all four tables and their provenance in one
transaction. Failed validation or insertion preserves the previous snapshot and
publication time.

## Part 3 connection

The exact fields and types are defined in `dashboard/contracts.py` and
[the integration contract](../docs/part4-integration.md). Integrated Part 3 produces:

1. `/lake/gold/repository_activity_hourly`
2. `/lake/gold/language_activity_hourly`
3. `/lake/gold/contributor_activity_hourly`
4. `/lake/gold/repository_clusters`

Airflow runs `export_gold.py` with Spark after the real jobs succeed. It reads Hudi,
writes JSONL and a checksummed manifest, and checks the result. `serving.py`
validates again before publishing. Synthetic mode is restricted to `demo`.

The SQL publisher replaces a **complete snapshot**, not an hourly delta. Gold jobs
replace complete Hudi snapshots; the exporter reads their full current contents.
Exports are capped at 100,000 rows per table for the laptop. Empty tables are
rejected; explicitly revise the policy if a genuinely empty reporting snapshot is needed.

## Files

- `sample_gold.json`: labeled offline fixture with known totals.
- `contracts.py`: schema, uniqueness, timestamp, count and reconciliation checks.
- `export_gold.py`: Hudi-to-JSONL Spark adapter.
- `serving.py`: transactional SQL publication.
- `bootstrap_superset.py`: repeatable datasets, charts and layout.
- `superset_config.py`: metadata connection and generated secret key.
- `start_superset.sh`: migration, permissions, bootstrap and web server startup.
- `verify_superset.py`: authenticated chart-query and UTC-filter checks.

Verification commands and the upstream integration limits are documented in
[the Airflow instructions](../orchestration/README.md#repeat-the-part-4-checks)
and [validation results](../docs/validation.md).

The bootstrap definitions are the version-controlled dashboard source. If editing
charts in the UI, export the changes and update the definitions so teammates can
reproduce them. Restarting reapplies the code definitions.

[Superset connection reference](https://superset.apache.org/docs/databases/).
