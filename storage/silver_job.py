"""Validate Bronze JSONL, quarantine invalid events, and upsert valid Hudi records."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, row_number, to_date, to_timestamp, udf
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from silver_contract import classify_event


SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("actor_id", LongType(), True),
        StructField("actor_login", StringType(), True),
        StructField("repository_id", LongType(), True),
        StructField("repo_name", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("payload_json", StringType(), True),
        StructField("record_order", StringType(), True),
        StructField("reason", StringType(), True),
    ]
)


def main():
    bronze_path = os.getenv(
        "BRONZE_PATH", "hdfs://namenode:9000/lake/bronze/github_events"
    )
    silver_path = os.getenv(
        "SILVER_PATH", "hdfs://namenode:9000/lake/silver/github_events"
    )
    quarantine_path = os.getenv(
        "QUARANTINE_PATH", "hdfs://namenode:9000/lake/quarantine/github_events"
    )

    spark = (
        SparkSession.builder.appName("IL05-Bronze-to-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    classify_udf = udf(classify_event, SILVER_SCHEMA)
    parsed = spark.read.text(bronze_path).withColumn(
        "event", classify_udf(col("value"))
    )

    try:
        invalid = parsed.filter(col("event.reason").isNotNull())
        invalid_count = invalid.count()
        if invalid_count:
            (
                invalid.select(
                    col("value").alias("raw_event"),
                    col("event.reason").alias("reason"),
                    current_timestamp().alias("quarantined_at"),
                )
                .write.mode("append")
                .json(quarantine_path)
            )

        valid = parsed.filter(col("event.reason").isNull()).select(
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            col("event.actor_id").alias("actor_id"),
            col("event.actor_login").alias("actor_login"),
            col("event.repository_id").alias("repository_id"),
            col("event.repo_name").alias("repo_name"),
            to_timestamp(col("event.created_at")).alias("created_at"),
            to_date(to_timestamp(col("event.created_at"))).alias("event_date"),
            current_timestamp().alias("ingested_at"),
            col("event.payload_json").alias("payload_json"),
            col("event.record_order").alias("record_order"),
        )
        # Hudi's precombine field is stable across replays. created_at/update time
        # wins first; the source-line digest resolves ties deterministically.
        valid_count = valid.count()
        winner_window = Window.partitionBy("event_id").orderBy(
            col("record_order").desc()
        )
        winners = (
            valid.withColumn("_winner", row_number().over(winner_window))
            .filter(col("_winner") == 1)
            .drop("_winner")
        )
        if valid_count:
            from hudi_io import write_silver

            write_silver(winners, silver_path)

        print(
            "Silver batch complete: {} valid source rows, {} invalid rows; "
            "valid records upserted by event_id.".format(valid_count, invalid_count),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
