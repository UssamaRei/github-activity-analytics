"""Part 3: read and validate the Hudi Silver event table."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hudi_io import read_silver
from silver_contract import SILVER_COLUMN_TYPES


def main():
    silver_path = os.getenv(
        "SILVER_PATH", "hdfs://namenode:9000/lake/silver/github_events"
    )
    spark = (
        SparkSession.builder.appName("IL05-Part3-Read-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    try:
        silver = read_silver(spark, silver_path)
        actual_types = {
            field.name: field.dataType.simpleString() for field in silver.schema.fields
        }
        missing = set(SILVER_COLUMN_TYPES) - set(actual_types)
        if missing:
            raise ValueError(
                "Silver table is missing contract columns: "
                + ", ".join(sorted(missing))
            )
        mismatches = [
            "{}: expected {}, got {}".format(name, expected, actual_types[name])
            for name, expected in SILVER_COLUMN_TYPES.items()
            if actual_types[name] != expected
        ]
        if mismatches:
            raise ValueError("Silver schema mismatch: " + "; ".join(mismatches))
        count = silver.count()
        if silver.filter(col("event_id").isNull()).limit(1).count():
            raise ValueError("Silver table contains an event with a null event_id")
        if (
            silver.groupBy("event_id")
            .count()
            .filter(col("count") > 1)
            .limit(1)
            .count()
        ):
            raise ValueError("Silver table contains duplicate event_id values")
        silver.groupBy("event_type").count().orderBy("event_type").show(
            truncate=False
        )
        print(
            "PASS: Part 3 read {} unique Silver Hudi records.".format(count),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
