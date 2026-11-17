"""Kafka -> immutable raw JSON lines in HDFS using Structured Streaming."""

import os

from pyspark.sql import SparkSession


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
    print("Bronze stream started: Kafka -> HDFS", flush=True)
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
