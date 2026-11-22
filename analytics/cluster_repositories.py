"""Group repositories with similar activity using Spark MLlib KMeans.

KMeans is descriptive, not a prediction model: it finds repositories whose
activity profiles look alike. Steps:
1. Features per repository over a UTC period [start, end):
   event_count, active_contributors, push_share, pr_share, issue_share.
2. Transform: log1p on the two counts (activity is heavy-tailed: one huge repo
   should not flatten everything else), then StandardScaler (mean 0, std 1) so
   counts and shares weigh the same.
3. K: try K in [2, max_k] and keep the best silhouette score (or force --k).
4. Explain: each cluster's mean raw features are compared with the overall
   mean and turned into a readable label, e.g. "high activity, single
   contributor, mostly pushes".

Run on the labeled 12-repository TEST fixture:
    python -m analytics.cluster_repositories --source cluster-fixture --output tmp/gold --format local-csv
"""

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from analytics.repository_metrics import ISSUES, PULL_REQUEST, PUSH, count_type, deduplicate

FEATURES = ["event_count", "active_contributors", "push_share", "pr_share", "issue_share"]
LOG_FEATURES = ["event_count", "active_contributors"]
SEED = 42
DEFAULT_SETTINGS = Path(__file__).resolve().parents[1] / "data/models/kmeans_settings.json"


def repository_features(silver: DataFrame, start: date | None = None, end: date | None = None) -> DataFrame:
    """One row per repository for the period; start inclusive, end exclusive."""
    events = deduplicate(silver)
    if start:
        events = events.where(F.col("event_date") >= F.lit(start))
    if end:
        events = events.where(F.col("event_date") < F.lit(end))
    counts = events.groupBy("repo_name").agg(
        F.count("*").alias("event_count"),
        F.countDistinct("actor_login").alias("active_contributors"),
        count_type(PUSH).alias("push"),
        count_type(PULL_REQUEST).alias("pr"),
        count_type(ISSUES).alias("issue"),
    )
    for name in ("push", "pr", "issue"):
        counts = counts.withColumn(f"{name}_share", F.col(name) / F.col("event_count"))
    return counts.drop("push", "pr", "issue")


def _pipeline(k: int) -> Pipeline:
    return Pipeline(stages=[
        VectorAssembler(inputCols=[f"model_{c}" for c in FEATURES], outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        KMeans(k=k, seed=SEED, featuresCol="features", predictionCol="cluster_id"),
    ])


def _model_input(features: DataFrame) -> DataFrame:
    for column in FEATURES:
        value = F.log1p(F.col(column)) if column in LOG_FEATURES else F.col(column)
        features = features.withColumn(f"model_{column}", value.cast("double"))
    return features


def fit_clusters(features: DataFrame, k: int | None = None, max_k: int = 6):
    """Return (clustered DataFrame, chosen k, {k: silhouette}, fitted pipeline)."""
    data = _model_input(features).cache()
    n = data.count()
    if n < 3:
        raise ValueError(f"Need at least 3 repositories to cluster, got {n}")
    candidates = [k] if k else list(range(2, min(max_k, n - 1) + 1))
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster_id")
    scores, models = {}, {}
    for candidate in candidates:
        model = _pipeline(candidate).fit(data)
        scores[candidate] = round(evaluator.evaluate(model.transform(data)), 4)
        models[candidate] = model
    best = max(scores, key=scores.get)  # ties -> smallest k (dict keeps order)
    clustered = models[best].transform(data).drop("raw_features", "features", *[f"model_{c}" for c in FEATURES])
    return clustered, best, scores, models[best]


def describe_cluster(means: dict, overall: dict) -> str:
    """Turn mean raw features into a label relative to the whole population."""
    ratio = means["event_count"] / overall["event_count"] if overall["event_count"] else 1
    activity = "high activity" if ratio >= 1.5 else "low activity" if ratio <= 0.5 else "moderate activity"
    if means["active_contributors"] < 1.5:
        people = "single contributor"
    elif means["active_contributors"] >= 1.5 * overall["active_contributors"]:
        people = "many contributors"
    else:
        people = "few contributors"
    shares = {"pushes": means["push_share"], "pull requests": means["pr_share"], "issues": means["issue_share"]}
    top, value = max(shares.items(), key=lambda item: item[1])
    mix = f"mostly {top}" if value > 0.5 else "mixed event types"
    return f"{activity}, {people}, {mix}"


def explain_clusters(clustered: DataFrame) -> list[dict]:
    overall = clustered.agg(*[F.avg(c).alias(c) for c in FEATURES]).first().asDict()
    rows = clustered.groupBy("cluster_id").agg(
        F.count("*").alias("size"), *[F.avg(c).alias(c) for c in FEATURES]
    ).orderBy("cluster_id").collect()
    clusters = []
    for row in rows:
        means = {c: round(row[c], 3) for c in FEATURES}
        clusters.append({"cluster_id": row.cluster_id, "size": row["size"], "label": describe_cluster(means, overall), "means": means})
    return clusters


def main(argv=None):
    from analytics.gold_io import GOLD_PATH, SILVER_PATH, build_spark, load_silver, write_gold

    parser = argparse.ArgumentParser(description="Repository KMeans clustering -> Gold repository_clusters")
    parser.add_argument("--source", default="hudi", choices=["hudi", "parquet", "fixture", "cluster-fixture"])
    parser.add_argument("--silver", default=SILVER_PATH)
    parser.add_argument("--output", default=GOLD_PATH)
    parser.add_argument("--format", default="hudi", choices=["hudi", "parquet", "csv", "local-csv"])
    parser.add_argument("--start", type=date.fromisoformat, help="UTC date, inclusive")
    parser.add_argument("--end", type=date.fromisoformat, help="UTC date, exclusive")
    parser.add_argument("--k", type=int, help="force K instead of choosing by silhouette")
    parser.add_argument("--max-k", type=int, default=6)
    parser.add_argument("--settings", default=str(DEFAULT_SETTINGS), help="where to save the chosen settings (JSON)")
    parser.add_argument("--model-path", help="optional: save the fitted pipeline (e.g. on HDFS)")
    args = parser.parse_args(argv)

    spark = build_spark("IL05-Repository-Clusters")
    try:
        silver = load_silver(spark, args.source, args.silver)
        features = repository_features(silver, args.start, args.end)
        clustered, k, scores, model = fit_clusters(features, args.k, args.max_k)
        clusters = explain_clusters(clustered)
        bounds = silver.agg(F.min("event_date").alias("lo"), F.max("event_date").alias("hi")).first()
        period_start = args.start or bounds.lo
        period_end = args.end or bounds.hi  # inclusive last day when no --end is given
        labels = spark.createDataFrame([(c["cluster_id"], c["label"]) for c in clusters], "cluster_id int, cluster_label string")
        result = (
            clustered.join(labels, "cluster_id")
            .withColumn("period_start", F.lit(period_start))
            .withColumn("period_end", F.lit(period_end))
            .withColumn("k", F.lit(k))
            .withColumn("silhouette", F.lit(scores[k]))
        )
        settings = {
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "period": {"start": str(period_start), "end": str(period_end), "end_inclusive": args.end is None},
            "features": FEATURES, "log1p": LOG_FEATURES, "scaler": "StandardScaler(withMean, withStd)",
            "seed": SEED, "k": k, "k_selection": "forced" if args.k else "best silhouette", "silhouette_by_k": scores,
            "clusters": clusters,
        }
        Path(args.settings).parent.mkdir(parents=True, exist_ok=True)
        Path(args.settings).write_text(json.dumps(settings, indent=2), encoding="utf-8")
        if args.model_path:
            model.write().overwrite().save(args.model_path)
        path = write_gold(result, "repository_clusters", args.output, args.format)
        print(f"K={k} silhouette={scores[k]} (tried {scores})")
        for c in clusters:
            print(f"  cluster {c['cluster_id']} ({c['size']} repos): {c['label']}")
        print(f"wrote repository_clusters -> {path}; settings -> {args.settings}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
