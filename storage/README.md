# Storage

`bronze_stream.py` uses Spark Structured Streaming to read Kafka message values
and write the original JSON records as text files in HDFS. Checkpoints are also
stored in HDFS. Keep checkpoints and the output directory together when moving
or restoring the platform.

Silver Hudi upserts, event-ID deduplication, quality checks, and Gold Hudi tables
are planned for the next iteration. The Silver/Gold directories created at startup
are empty reserved paths, not implemented lakehouse tables.
