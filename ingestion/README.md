# Part 1 — Data collection and Kafka

Use completed UTC hours in `YYYY-MM-DD-HH` form. GH Archive publishes the
source files as compressed hourly JSON. The downloader writes JSONL files
under the ignored `data/downloads/` directory. Within Compose, the producer
connects to `kafka:9092` and publishes to `github_events`.

## Try a small real sample

From the repository root, start the shared services, download up to 100
events for the selected hour, and publish the downloaded file.

```powershell
docker compose up --build -d
docker compose run --rm --no-deps producer python -m ingestion.download --hour 2025-01-01-15 --limit 100 --output-dir data/downloads
docker compose run --rm producer python -m ingestion.producer --input data/downloads/2025-01-01-15-sample-100.jsonl --hour 2025-01-01-15 --manifest data/downloads/2025-01-01-15-sample-100.publish.json
python scripts/verify_bronze.py --expected data/downloads/2025-01-01-15-sample-100.jsonl
```

Repeat `--hour` on the download command to request multiple hours. Use
`--full-hour` instead of `--limit 100` to download complete hours. The
resulting filename ends in `-full.jsonl`. Publish one file at a time with its
own manifest and omit the producer's `--limit` to send the whole file.

The Kafka key is the UTF-8 repository name and the value is the original JSON
line. Invalid records fail validation with an error. Archive requests retry
temporary failures up to three total attempts. The manifest marks success
after Kafka acknowledges every record. It records the source hour, delivery
count, offsets per partition, and any reported error. Keep the manifest
with its Kafka destination. Rerunning with a complete manifest skips the
file. An incomplete publication replays from the beginning, so Bronze can
contain duplicate event IDs. See the [data contract](../docs/data-contract.md)
for the manifest fields, exit codes, and the Bronze offset check.

## Verification

- `python -m unittest discover -s tests -v` covers validation, bounded and full
  downloads, retryable and permanent failures, partial publication, and resume.
- The fictional fixture in `data/samples/github_events.jsonl` produced six
  events in dry-run mode. A separate real sample from `2025-01-01-15` UTC
  produced 100 Kafka acknowledgements. Rerunning the completed real sample
  left the end offset at 106. All 106 fixture and real records were compared
  byte for byte against HDFS Bronze.
- The local startup required a direct Kafka readiness check because the
  Compose health probe exceeded its 10-second timeout. HDFS verification
  used a longer command timeout on that machine.
- The acknowledged offsets were verified against Kafka. Compatibility with
  the Part 4 runner is pending availability of its source and integration
  contract in the shared repository.
