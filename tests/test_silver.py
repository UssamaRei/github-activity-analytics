import json
import unittest
from pathlib import Path
from unittest.mock import Mock

from storage.hudi_io import HUDI_WRITE_OPTIONS, TABLE_NAME, write_silver
from storage.silver_contract import (
    SILVER_COLUMN_TYPES,
    classify_event,
    deduplicate_events,
)


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data/samples/github_events.jsonl"


def sample_events():
    return [line for line in SAMPLE.read_text(encoding="utf-8").splitlines() if line]


class SilverTests(unittest.TestCase):
    def test_six_sample_events_parse_into_expected_types(self):
        raw_events = sample_events()
        parsed = [classify_event(raw) for raw in raw_events]
        self.assertEqual(len(parsed), 6)
        self.assertTrue(all(record["reason"] is None for record in parsed))
        self.assertEqual(
            [record["event_type"] for record in parsed].count("PushEvent"), 3
        )
        self.assertEqual(
            [record["event_type"] for record in parsed].count("IssuesEvent"), 2
        )
        self.assertEqual(
            [record["event_type"] for record in parsed].count("PullRequestEvent"), 1
        )
        self.assertEqual({record["event_id"] for record in parsed}, {
            "demo-001", "demo-002", "demo-003", "demo-004", "demo-005", "demo-006"
        })
        self.assertEqual(parsed[0]["payload_json"], raw_events[0])
        self.assertEqual(parsed[0]["repository_id"], 201)
        self.assertEqual(parsed[0]["actor_id"], 101)

    def test_missing_event_id_is_invalid(self):
        event = json.loads(sample_events()[0])
        del event["id"]
        result = classify_event(json.dumps(event))
        self.assertIn("missing or invalid id", result["reason"])

    def test_missing_repository_information_is_invalid(self):
        event = json.loads(sample_events()[0])
        del event["repo"]
        result = classify_event(json.dumps(event))
        self.assertIn("missing or invalid repo.name", result["reason"])
        self.assertIsNone(result["repository_id"])

    def test_invalid_timestamp_is_quarantined(self):
        event = json.loads(sample_events()[0])
        event["created_at"] = "yesterday"
        self.assertIn("valid ISO-8601", classify_event(json.dumps(event))["reason"])

    def test_unknown_event_type_is_preserved(self):
        event = json.loads(sample_events()[0])
        event["type"] = "FutureEventType"
        result = classify_event(json.dumps(event))
        self.assertIsNone(result["reason"])
        self.assertEqual(result["event_type"], "FutureEventType")

    def test_duplicate_ids_choose_latest_and_break_ties_deterministically(self):
        first = json.loads(sample_events()[0])
        newer = dict(first, created_at="2025-01-02T00:00:00Z")
        tie = dict(newer, payload={"ref": "stable tie breaker"})
        raws = [json.dumps(first), json.dumps(newer), json.dumps(tie)]
        candidates = [classify_event(raw) for raw in raws]
        winners = deduplicate_events(candidates)
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["created_at"], "2025-01-02T00:00:00Z")
        self.assertEqual(
            winners[0]["record_order"], max(row["record_order"] for row in candidates)
        )

    def test_utc_conversion_and_utc_event_date(self):
        event = json.loads(sample_events()[0])
        event["created_at"] = "2025-01-01T10:30:00-05:00"
        result = classify_event(json.dumps(event))
        self.assertEqual(result["created_at"], "2025-01-01T15:30:00Z")
        self.assertEqual(result["created_at"][:10], "2025-01-01")

    def test_silver_schema_contract(self):
        self.assertEqual(SILVER_COLUMN_TYPES["event_id"], "string")
        self.assertEqual(SILVER_COLUMN_TYPES["repository_id"], "bigint")
        self.assertEqual(SILVER_COLUMN_TYPES["actor_id"], "bigint")
        self.assertEqual(SILVER_COLUMN_TYPES["created_at"], "timestamp")
        self.assertEqual(SILVER_COLUMN_TYPES["event_date"], "date")
        self.assertEqual(SILVER_COLUMN_TYPES["ingested_at"], "timestamp")
        self.assertEqual(SILVER_COLUMN_TYPES["payload_json"], "string")

    def test_hudi_writer_uses_partitioned_upserts_and_global_event_keys(self):
        writer = Mock()
        dataframe = Mock(write=writer)
        writer.format.return_value = writer
        writer.options.return_value = writer
        writer.mode.return_value = writer

        write_silver(dataframe, "hdfs://namenode:9000/lake/silver/github_events")

        writer.format.assert_called_once_with("hudi")
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.table.name"], TABLE_NAME)
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.datasource.write.operation"], "upsert")
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.datasource.write.recordkey.field"], "event_id")
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.datasource.write.partitionpath.field"], "event_date")
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.datasource.write.precombine.field"], "record_order")
        self.assertEqual(HUDI_WRITE_OPTIONS["hoodie.index.type"], "GLOBAL_BLOOM")
        writer.save.assert_called_once_with("hdfs://namenode:9000/lake/silver/github_events")

    def test_replay_merge_remains_six_current_event_ids(self):
        parsed = [classify_event(raw) for raw in sample_events()]
        snapshot = {row["event_id"]: row for row in deduplicate_events(parsed)}
        for row in deduplicate_events(parsed + parsed):
            current = snapshot.get(row["event_id"])
            if current is None or row["record_order"] > current["record_order"]:
                snapshot[row["event_id"]] = row
        self.assertEqual(len(snapshot), 6)
        self.assertEqual(set(snapshot), {"demo-001", "demo-002", "demo-003", "demo-004", "demo-005", "demo-006"})


if __name__ == "__main__":
    unittest.main()
