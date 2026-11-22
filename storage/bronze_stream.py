"""Kafka -> immutable raw JSON lines in HDFS using Structured Streaming."""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener


LOG_PREFIX = "STREAMING_METRIC "


def emit_log(record):
    record["logged_at"] = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    print(LOG_PREFIX + json.dumps(record, sort_keys=True), flush=True)


def parse_offset(offset):
    if offset is None:
        return None
    try:
        return json.loads(offset)
    except (TypeError, ValueError):
        return offset


class BronzeProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        emit_log(
            {
                "event": "query_started",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "query_name": event.name,
                "query_status": "running",
            }
        )

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        durations = progress.get("durationMs") or {}
        sources = []
        for source_id, source in enumerate(progress.get("sources") or []):
            sources.append(
                {
                    "source_id": source_id,
                    "description": source.get("description"),
                    "start_offset": parse_offset(source.get("startOffset")),
                    "end_offset": parse_offset(source.get("endOffset")),
                }
            )
        emit_log(
            {
                "event": "batch_progress",
                "query_id": progress.get("id"),
                "run_id": progress.get("runId"),
                "query_name": progress.get("name"),
                "query_status": "running",
                "batch_id": progress.get("batchId"),
                "batch_timestamp": progress.get("timestamp"),
                "num_input_rows": progress.get("numInputRows"),
                "input_rows_per_second": progress.get("inputRowsPerSecond"),
                "processed_rows_per_second": progress.get(
                    "processedRowsPerSecond"
                ),
                "batch_duration_ms": durations.get("triggerExecution"),
                "duration_ms": durations,
                "sources": sources,
            }
        )

    def onQueryTerminated(self, event):
        emit_log(
            {
                "event": "query_terminated",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "query_status": "terminated",
                "exception": event.exception,
            }
        )


def log_query_status(query):
    emit_log(
        {
            "event": "query_status",
            "query_id": str(query.id),
            "run_id": str(query.runId),
            "query_status": "running" if query.isActive else "inactive",
            "is_active": query.isActive,
            "status": query.status,
        }
    )


def main():
    spark = (
        SparkSession.builder.appName("IL05-Kafka-to-Bronze")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    listener = BronzeProgressListener()
    spark.streams.addListener(listener)
    try:
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .option("subscribe", os.getenv("KAFKA_TOPIC", "github_events"))
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", "1000")
            .load()
        )
        # A text sink preserves the source JSON. A JSON sink would wrap/escape it.
        query = (
            source.selectExpr("CAST(value AS STRING) AS value")
            .writeStream.format("text")
            .outputMode("append")
            .option("path", os.getenv("BRONZE_PATH", "hdfs://namenode:9000/lake/bronze/github_events"))
            .option("checkpointLocation", os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/github_events_bronze"))
            .trigger(processingTime="5 seconds")
            .start()
        )
        emit_log(
            {
                "event": "stream_started",
                "query_id": str(query.id),
                "run_id": str(query.runId),
                "query_status": "running",
                "trigger_interval": "5 seconds",
                "checkpoint_path": os.getenv(
                    "CHECKPOINT_PATH",
                    "hdfs://namenode:9000/checkpoints/github_events_bronze",
                ),
            }
        )
        while query.isActive:
            log_query_status(query)
            query.awaitTermination(30)
        query.awaitTermination()
    finally:
        spark.streams.removeListener(listener)
        spark.stop()


if __name__ == "__main__":
    main()
