"""Silver events + repository language metadata -> Gold language_activity_hourly.

GH Archive events do not carry a language. We look it up with the GitHub
repository API (`GET /repos/{owner}/{repo}` -> "language") and keep a local JSON
cache so each repository is requested once. Rules (docs/data-contract.md):
- Missing, null or not-found language -> "unknown". Never guessed from names.
- A temporary API failure is NOT cached: the repo is "unknown" for this run
  and retried next run. Not-found (404) and null languages are cached.
- Set GITHUB_TOKEN to raise the API rate limit; it is never written anywhere.

Run on the six-event TEST fixture with FICTIONAL languages (no network):
    python -m analytics.language_metrics --source fixture --languages fixture --output tmp/gold
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

UNKNOWN = "unknown"
DEFAULT_CACHE = Path(__file__).resolve().parents[1] / "data/cache/repo_languages.json"
API = "https://api.github.com/repos/"


class TemporaryLookupError(Exception):
    """Rate limit, network or server error: retry on a later run."""


def fetch_github_language(repo_name: str, token: str | None = None, timeout: float = 10) -> str | None:
    """Return the repository's primary language, or None if GitHub has none/404."""
    request = urllib.request.Request(API + repo_name, headers={"Accept": "application/vnd.github+json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response).get("language")
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 451):  # deleted, renamed away, or blocked
            return None
        raise TemporaryLookupError(f"{repo_name}: HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise TemporaryLookupError(f"{repo_name}: {exc}") from exc


class LanguageCache:
    def __init__(self, path: str | Path = DEFAULT_CACHE, fetch=fetch_github_language):
        self.path = Path(path)
        self.fetch = fetch
        self.entries = json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}
        self.failures: list[str] = []

    def resolve(self, repo_names, allow_fetch: bool = True, max_requests: int | None = None) -> dict:
        """Return {repo_name: language or "unknown"} for every requested repo."""
        token = os.getenv("GITHUB_TOKEN")
        requests = 0
        result = {}
        for repo in sorted(set(repo_names)):
            if repo not in self.entries and allow_fetch and (max_requests is None or requests < max_requests):
                requests += 1
                try:
                    language = self.fetch(repo, token)
                    self.entries[repo] = {"language": language, "fetched_at": datetime.now(timezone.utc).isoformat()}
                except TemporaryLookupError as exc:
                    self.failures.append(str(exc))
            language = self.entries.get(repo, {}).get("language")
            result[repo] = language or UNKNOWN
        return result

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.entries, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)  # atomic: a crash never leaves a half-written cache


def language_activity_hourly(silver, languages: dict):
    """One row per (hour_utc, language). Repos missing from `languages` -> unknown."""
    from pyspark.sql import functions as F

    from analytics.repository_metrics import deduplicate

    spark = silver.sparkSession
    mapping = spark.createDataFrame(list(languages.items()), "repo_name string, language string")
    return (
        deduplicate(silver)
        .join(F.broadcast(mapping), "repo_name", "left")
        .withColumn("language", F.coalesce(F.col("language"), F.lit(UNKNOWN)))
        .withColumn("hour_utc", F.date_trunc("hour", F.col("created_at")))
        .groupBy("hour_utc", "language")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("repo_name").alias("repo_count"),
            F.countDistinct("repo_name").alias("active_repositories"),
            F.countDistinct("actor_login").alias("active_contributors"),
        )
        .withColumn("event_date", F.to_date("hour_utc"))
    )


def main(argv=None):
    from analytics.gold_io import GOLD_PATH, SILVER_PATH, build_spark, load_silver, write_gold
    from analytics.repository_metrics import add_growth

    parser = argparse.ArgumentParser(description="Silver -> Gold language activity")
    parser.add_argument('--hour', help='UTC source-hour identity; recompute the complete Silver snapshot')
    parser.add_argument("--source", default="hudi", choices=["hudi", "parquet", "fixture", "cluster-fixture"])
    parser.add_argument("--silver", default=SILVER_PATH)
    parser.add_argument("--output", default=GOLD_PATH)
    parser.add_argument("--format", default="hudi", choices=["hudi", "parquet", "csv", "local-csv"])
    parser.add_argument("--languages", default="cache", choices=["cache", "fixture"],
                        help="'fixture' = FICTIONAL test mapping, no network")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE))
    parser.add_argument("--offline", action="store_true", help="use the cache only; no API calls")
    parser.add_argument("--max-requests", type=int, default=500, help="API calls per run (rate limit)")
    args = parser.parse_args(argv)

    spark = build_spark("IL05-Language-Metrics")
    try:
        silver = load_silver(spark, args.source, args.silver)
        repos = [row.repo_name for row in silver.select("repo_name").distinct().collect()]
        if args.languages == "fixture":
            from analytics.fixtures import FICTIONAL_LANGUAGES

            languages = {repo: FICTIONAL_LANGUAGES.get(repo, UNKNOWN) for repo in repos}
        else:
            cache = LanguageCache(args.cache)
            languages = cache.resolve(repos, allow_fetch=not args.offline, max_requests=args.max_requests)
            cache.save()
            if cache.failures:
                print(f"WARNING: {len(cache.failures)} temporary lookup failures; retried next run")
        result = add_growth(language_activity_hourly(silver, languages), keys=("language",))
        path = write_gold(result, "language_activity_hourly", args.output, args.format)
        print(f"wrote language_activity_hourly: {result.count()} rows -> {path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
