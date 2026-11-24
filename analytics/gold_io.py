"""Shared Spark session, Silver reader and Gold writer for analytics jobs.

Uses Part 2's shared Silver reader. Gold has its own table-specific keys and
publishes complete snapshots through Hudi, including removal of stale groups.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "hdfs://namenode:9000/lake/silver/github_events"
GOLD_PATH = "hdfs://namenode:9000/lake/gold"

# Gold table -> (record key columns, partition column). Keys make reprocessing
# an upsert instead of an append, so rerunning a job never inflates results.
GOLD_TABLES = {
    "repository_activity_hourly": (["hour_utc", "repo_name"], "event_date"),
    "language_activity_hourly": (["hour_utc", "language"], "event_date"),
    "contributor_rankings_daily": (["event_date", "actor_login"], "event_date"),
    'contributor_activity_hourly': (['hour_utc', 'actor_login'], 'event_date'),
    "repository_clusters": (["snapshot_utc", "repo_name"], "period_end"),
}


def build_spark(app_name: str) -> SparkSession:
    # Python workers must match the driver's interpreter (Windows often has several).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_silver(spark: SparkSession, source: str, silver_path: str = SILVER_PATH) -> DataFrame:
    """source: 'fixture' (six sample events), 'cluster-fixture' (larger TEST set),
    'hudi' (Part 2's github_events_silver) or 'parquet'."""
    if source in ("fixture", "cluster-fixture"):
        from analytics.fixtures import SILVER_COLUMNS, cluster_fixture_rows, load_silver_rows

        rows = load_silver_rows() if source == "fixture" else cluster_fixture_rows()
        return spark.createDataFrame([tuple(row[c] for c in SILVER_COLUMNS) for row in rows], list(SILVER_COLUMNS))
    if source == "hudi":
        try:  # Prefer Part 2's shared reader when it is available.
            from storage.hudi_io import read_silver  # type: ignore

            result = read_silver(spark, silver_path)
        except ImportError:
            result = spark.read.format("hudi").load(silver_path)
        # The shared Bronze/Silver lake also retains the six explicit demo events.
        # Real Gold must never count those fictional records as GitHub activity.
        from analytics.fixtures import load_silver_rows
        fixture_ids = [row['event_id'] for row in load_silver_rows()]
        return result.filter(~F.col('event_id').isin(fixture_ids))
    if source == "parquet":
        return spark.read.parquet(silver_path)
    raise ValueError(f"Unknown Silver source: {source}")


def write_gold(df: DataFrame, table: str, base_path: str, fmt: str = "parquet") -> str:
    keys, partition = GOLD_TABLES[table]
    path = f"{base_path.rstrip('/')}/{table}"
    df = df.withColumn("computed_at", F.current_timestamp())
    if fmt == "hudi":
        _write_hudi(df, table, path, keys, partition)
    elif fmt == "parquet":
        # Local preview only: overwrite keeps reruns idempotent.
        df.write.mode("overwrite").partitionBy(partition).parquet(path)
    elif fmt == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    elif fmt == "local-csv":
        # Small example exports only: collects to the driver, no Hadoop needed
        # (works on Windows without winutils). Writes <base>/<table>.csv.
        path = f"{path}.csv"
        _write_local_csv(df.drop("computed_at").orderBy(*keys), path)
    else:
        raise ValueError(f"Unknown Gold format: {fmt}")
    return path


def _write_local_csv(df: DataFrame, path: str) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(df.columns)
        for row in df.collect():
            writer.writerow(["" if value is None else value for value in row])


def _write_hudi(df: DataFrame, table: str, path: str, keys: list, partition: str) -> None:
    options = {
        "hoodie.table.name": table,
        "hoodie.datasource.write.recordkey.field": ",".join(keys),
        "hoodie.datasource.write.precombine.field": "computed_at",
        "hoodie.datasource.write.partitionpath.field": partition,
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        # Every job recomputes the full Silver snapshot. Replace the table in one
        # Hudi commit so changed language groups or removed keys cannot linger.
        "hoodie.datasource.write.operation": "insert_overwrite_table",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        'hoodie.metadata.enable': 'false',
        'hoodie.insert.shuffle.parallelism': '2',
        'hoodie.upsert.shuffle.parallelism': '2',
    }
    df.write.format("hudi").options(**options).mode("append").save(path)
