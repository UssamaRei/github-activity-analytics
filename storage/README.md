# Storage

`bronze_stream.py` uses Spark Structured Streaming to read Kafka message values
and write the original JSON records as text files in HDFS. Checkpoints are also
stored in HDFS. Keep checkpoints and the output directory together when moving
or restoring the platform.

The Bronze query emits timestamped `STREAMING_METRIC` JSON records to its
container log: query lifecycle/status and, for each completed batch, the batch
ID, input rows, input/processed rows per second, trigger execution duration,
and Kafka start/end offsets. This listener only observes progress; it does not
write to, rename, or reset the streaming checkpoint. The 5-second trigger and
existing offset/recovery configuration are unchanged.

Inspect recent batches and a compact summary with:

```sh
docker compose logs --since 30m --no-color spark-bronze | python3 scripts/streaming_report.py
```

For live structured records, use `docker compose logs -f --no-color spark-bronze`
and look for `STREAMING_METRIC`. Check container CPU/memory with
`docker stats --no-stream spark-bronze kafka namenode datanode`; inspect Docker's
overall CPU/memory allocation with `docker info`.

`silver_job.py` reads Bronze JSONL, validates the shared contract, writes invalid
records with reasons to `/lake/quarantine/github_events`, and upserts valid events
to the Hudi Copy-on-Write table at `/lake/silver/github_events`, partitioned by
UTC `event_date`. Run it with `docker compose run --rm spark-silver`. The job is
deliberately rerunnable; Hudi uses `event_id` as its global record key and the
stable `record_order` value (source update/event timestamp plus raw-line digest)
as precombine. `hudi_io.py` exposes reusable Silver read and write functions;
`analytics/read_silver.py` is the Part 3 Spark reader for this table.

Gold Hudi tables remain planned. Silver quarantine is append-only, so replayed
invalid events can produce repeated quarantine records.
