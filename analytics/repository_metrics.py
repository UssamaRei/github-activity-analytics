"""Silver events -> Gold repository activity, contributor rankings and growth.

Metric definitions (also in docs/data-contract.md):
- event_count: deduplicated events, not commits.
- active_contributors: distinct actor_login values, bots included.
- growth_pct: change versus the SAME UTC hour one day earlier (see add_growth).
- All hours are UTC (spark.sql.session.timeZone = UTC).

Run on the labeled six-event TEST fixture (no HDFS needed):
    python -m analytics.repository_metrics --source fixture --output tmp/gold
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from analytics.gold_io import GOLD_PATH, SILVER_PATH, build_spark, load_silver, write_gold

PUSH = "PushEvent"
PULL_REQUEST = "PullRequestEvent"
ISSUES = "IssuesEvent"


def deduplicate(silver: DataFrame) -> DataFrame:
    """Silver is already keyed on event_id; this guards against replayed input."""
    return silver.dropDuplicates(["event_id"])


def count_type(event_type: str):
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).cast("long")


def repository_activity_hourly(silver: DataFrame) -> DataFrame:
    """One row per (hour_utc, repo_name) with event-type counts."""
    return (
        deduplicate(silver)
        .withColumn("hour_utc", F.date_trunc("hour", F.col("created_at")))
        .groupBy("hour_utc", "repo_name")
        .agg(
            F.count("*").alias("event_count"),
            count_type(PUSH).alias("push_count"),
            count_type(PULL_REQUEST).alias("pr_count"),
            count_type(ISSUES).alias("issue_count"),
            F.countDistinct("actor_login").alias("active_contributors"),
        )
        .withColumn("event_date", F.to_date("hour_utc"))
    )


def contributor_rankings(silver: DataFrame) -> DataFrame:
    """Per UTC day: events per contributor, ranked (1 = most active)."""
    daily = (
        deduplicate(silver)
        .groupBy("event_date", "actor_login")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("repo_name").alias("repo_count"),
        )
    )
    # Ties share a rank (1, 1, 3). Ordering by actor_login here would break ties.
    window = Window.partitionBy("event_date").orderBy(F.desc("event_count"))
    return daily.withColumn("rank", F.rank().over(window))


def add_growth(hourly: DataFrame, keys=("repo_name",)) -> DataFrame:
    """Add previous_event_count and growth_pct, comparing each row with the SAME
    UTC hour one day earlier for the same key (matching periods, so a quiet
    3am is never compared with a busy 3pm).

    - previous_event_count is null when no row exists for that earlier hour
      (the hour may not have been ingested, so we do not assume zero).
    - growth_pct = (event_count - previous) / previous * 100, rounded to 2
      decimals; null when previous is null or 0 (no meaningful percentage).
    A self-join is used instead of lag() because hours with no activity have
    no row, so "the previous row" is not necessarily "the previous day".
    """
    previous = hourly.select(
        *keys,
        (F.col("hour_utc") + F.expr("INTERVAL 1 DAY")).alias("hour_utc"),
        F.col("event_count").alias("previous_event_count"),
    )
    joined = hourly.join(previous, on=[*keys, "hour_utc"], how="left")
    growth = (F.col("event_count") - F.col("previous_event_count")) / F.col("previous_event_count") * 100
    return joined.withColumn(
        "growth_pct",
        F.when(F.col("previous_event_count") > 0, F.round(growth, 2)).otherwise(F.lit(None).cast("double")),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description="Silver -> Gold repository metrics")
    parser.add_argument("--source", default="hudi", choices=["hudi", "parquet", "fixture", "cluster-fixture"])
    parser.add_argument("--silver", default=SILVER_PATH)
    parser.add_argument("--output", default=GOLD_PATH, help="Gold base directory")
    parser.add_argument("--format", default="hudi", choices=["hudi", "parquet", "csv", "local-csv"])
    args = parser.parse_args(argv)

    spark = build_spark("IL05-Repository-Metrics")
    try:
        silver = load_silver(spark, args.source, args.silver)
        hourly = add_growth(repository_activity_hourly(silver))
        rankings = contributor_rankings(silver)
        for table, df in (("repository_activity_hourly", hourly), ("contributor_rankings_daily", rankings)):
            print(f"wrote {table}: {df.count()} rows -> {write_gold(df, table, args.output, args.format)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
