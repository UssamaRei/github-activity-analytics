import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from analytics.fixtures import CLUSTER_PROFILES, REPOS_PER_PROFILE, SILVER_COLUMNS, cluster_fixture_rows, load_silver_rows
from analytics.language_metrics import UNKNOWN, LanguageCache, TemporaryLookupError

HAS_SPARK = importlib.util.find_spec("pyspark") is not None


class FixtureTests(unittest.TestCase):
    """Run in CI without pyspark."""

    def test_fixture_has_silver_columns_and_six_ids(self):
        rows = load_silver_rows()
        self.assertEqual(len(rows), 6)
        self.assertEqual({row["event_id"] for row in rows}, {f"demo-00{i}" for i in range(1, 7)})
        self.assertTrue(all(tuple(row) == SILVER_COLUMNS for row in rows))

    def test_created_at_is_utc(self):
        row = load_silver_rows()[0]
        self.assertEqual(row["created_at"], datetime(2025, 1, 1, 15, 0, tzinfo=timezone.utc))
        self.assertEqual(str(row["event_date"]), "2025-01-01")

    def test_cluster_fixture_is_deterministic_and_unique(self):
        first, second = cluster_fixture_rows(), cluster_fixture_rows()
        self.assertEqual(first, second)
        self.assertEqual(len({row["event_id"] for row in first}), len(first))
        self.assertEqual(len({row["repo_name"] for row in first}), len(CLUSTER_PROFILES) * REPOS_PER_PROFILE)


class LanguageCacheTests(unittest.TestCase):
    """No network: the GitHub API call is replaced by a fake."""

    def make_cache(self, folder, answers):
        calls = []

        def fake_fetch(repo, token):
            calls.append(repo)
            answer = answers[repo]
            if isinstance(answer, Exception):
                raise answer
            return answer

        return LanguageCache(Path(folder) / "cache.json", fetch=fake_fetch), calls

    def test_known_missing_and_failed_languages(self):
        with tempfile.TemporaryDirectory() as folder:
            cache, calls = self.make_cache(folder, {
                "o/py": "Python", "o/none": None, "o/flaky": TemporaryLookupError("HTTP 503"),
            })
            result = cache.resolve(["o/py", "o/none", "o/flaky", "o/py"])
            cache.save()
            self.assertEqual(result, {"o/py": "Python", "o/none": UNKNOWN, "o/flaky": UNKNOWN})
            self.assertEqual(sorted(calls), ["o/flaky", "o/none", "o/py"])
            saved = json.loads((Path(folder) / "cache.json").read_text(encoding="utf-8"))
            # Temporary failures are not cached, so they are retried next run.
            self.assertEqual(set(saved), {"o/py", "o/none"})

    def test_cache_hit_makes_no_request_and_offline_mode(self):
        with tempfile.TemporaryDirectory() as folder:
            cache, calls = self.make_cache(folder, {"o/py": "Python", "o/new": "Go"})
            cache.resolve(["o/py"])
            cache.save()
            again, calls_again = self.make_cache(folder, {})
            self.assertEqual(again.resolve(["o/py"]), {"o/py": "Python"})
            self.assertEqual(again.resolve(["o/new"], allow_fetch=False), {"o/new": UNKNOWN})
            self.assertEqual(calls_again, [])

    def test_max_requests_limits_api_calls(self):
        with tempfile.TemporaryDirectory() as folder:
            cache, calls = self.make_cache(folder, {"a/1": "C", "a/2": "C", "a/3": "C"})
            result = cache.resolve(["a/1", "a/2", "a/3"], max_requests=2)
            self.assertEqual(len(calls), 2)
            self.assertEqual(list(result.values()).count(UNKNOWN), 1)


@unittest.skipUnless(HAS_SPARK, "pyspark not installed")
class SparkAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from analytics import cluster_repositories, language_metrics, repository_metrics
        from analytics.gold_io import build_spark, load_silver

        cls.metrics = repository_metrics
        cls.languages = language_metrics
        cls.clusters = cluster_repositories
        cls.spark = build_spark("IL05-analytics-tests")
        cls.silver = load_silver(cls.spark, "fixture")
        cls.cluster_silver = load_silver(cls.spark, "cluster-fixture")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def hourly_by_repo(self, silver):
        rows = self.metrics.repository_activity_hourly(silver).collect()
        return {row.repo_name: row for row in rows}

    # --- repository_activity_hourly -------------------------------------
    def test_known_fixture_totals(self):
        result = self.hourly_by_repo(self.silver)
        self.assertEqual(set(result), {"demo-org/alpha", "demo-org/beta"})
        alpha, beta = result["demo-org/alpha"], result["demo-org/beta"]
        self.assertEqual((alpha.event_count, alpha.active_contributors), (4, 3))
        self.assertEqual((beta.event_count, beta.active_contributors), (2, 2))
        self.assertEqual((alpha.push_count, alpha.pr_count, alpha.issue_count), (2, 0, 2))
        self.assertEqual((beta.push_count, beta.pr_count, beta.issue_count), (1, 1, 0))
        self.assertEqual(str(alpha.hour_utc), "2025-01-01 15:00:00")

    def test_replay_does_not_inflate_counts(self):
        replayed = self.silver.unionByName(self.silver)
        self.assertEqual(self.hourly_by_repo(replayed)["demo-org/alpha"].event_count, 4)

    # --- contributor_rankings_daily -------------------------------------
    def test_contributor_rankings(self):
        rows = {r.actor_login: r for r in self.metrics.contributor_rankings(self.silver).collect()}
        # alice 2, bob 2, charlie 2: all tie at rank 1.
        self.assertEqual({login: r.event_count for login, r in rows.items()},
                         {"demo-alice": 2, "demo-bob": 2, "demo-charlie": 2})
        self.assertEqual({r.rank for r in rows.values()}, {1})
        self.assertEqual(rows["demo-alice"].repo_count, 2)

    def test_hourly_contributor_contract_and_reconciliation(self):
        rows = self.metrics.contributor_activity_hourly(self.silver).collect()
        self.assertEqual(sum(row.event_count for row in rows), 6)
        self.assertEqual({row.actor_login for row in rows}, {'demo-alice', 'demo-bob', 'demo-charlie'})
        self.assertEqual({str(row.hour_utc) for row in rows}, {'2025-01-01 15:00:00'})
        self.assertEqual({row.actor_login: row.repo_count for row in rows},
                         {'demo-alice': 2, 'demo-bob': 1, 'demo-charlie': 2})

    # --- growth -----------------------------------------------------------
    def test_growth_compares_same_hour_previous_day(self):
        hourly = self.spark.createDataFrame(
            [
                ("r", datetime(2025, 1, 1, 15), 4),
                ("r", datetime(2025, 1, 2, 14), 100),  # different hour: must be ignored
                ("r", datetime(2025, 1, 2, 15), 6),
                ("r", datetime(2025, 1, 3, 9), 1),     # no row a day earlier
            ],
            "repo_name string, hour_utc timestamp, event_count long",
        )
        rows = {row.hour_utc.day * 100 + row.hour_utc.hour: row for row in self.metrics.add_growth(hourly).collect()}
        self.assertEqual((rows[215].previous_event_count, rows[215].growth_pct), (4, 50.0))
        self.assertIsNone(rows[115].previous_event_count)
        self.assertIsNone(rows[309].growth_pct)

    # --- language_activity_hourly ---------------------------------------
    def test_language_activity_uses_unknown_for_missing(self):
        from analytics.fixtures import FICTIONAL_LANGUAGES

        rows = {r.language: r for r in self.languages.language_activity_hourly(self.silver, FICTIONAL_LANGUAGES).collect()}
        self.assertEqual(set(rows), {"Python", UNKNOWN})
        self.assertEqual((rows["Python"].event_count, rows["Python"].active_contributors), (4, 3))
        self.assertEqual((rows[UNKNOWN].event_count, rows[UNKNOWN].repo_count), (2, 1))

    # --- repository_clusters --------------------------------------------
    def test_features_are_shares_of_events(self):
        rows = {r.repo_name: r for r in self.clusters.repository_features(self.silver).collect()}
        alpha = rows["demo-org/alpha"]
        self.assertEqual((alpha.event_count, alpha.push_share, alpha.issue_share, alpha.pr_share), (4, 0.5, 0.5, 0.0))

    def test_kmeans_recovers_fixture_profiles_and_explains_them(self):
        features = self.clusters.repository_features(self.cluster_silver)
        clustered, k, scores, _model = self.clusters.fit_clusters(features)
        self.assertEqual(k, 3, scores)
        by_profile = {}
        for row in clustered.collect():
            profile = row.repo_name.split("/")[0].replace("fixture-", "", 1)
            by_profile.setdefault(profile, set()).add(row.cluster_id)
        self.assertTrue(all(len(ids) == 1 for ids in by_profile.values()), by_profile)
        self.assertEqual(len({next(iter(ids)) for ids in by_profile.values()}), 3)
        labels = {c["cluster_id"]: c["label"] for c in self.clusters.explain_clusters(clustered)}
        label = {p: labels[next(iter(ids))] for p, ids in by_profile.items()}
        self.assertIn("single contributor", label["automation"])
        self.assertIn("mostly pushes", label["automation"])
        self.assertIn("many contributors", label["community"])
        self.assertIn("low activity", label["quiet"])


if __name__ == "__main__":
    unittest.main()
