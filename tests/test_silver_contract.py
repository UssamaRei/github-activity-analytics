import json
import unittest
from pathlib import Path

from storage.silver_contract import classify_event


SAMPLE = Path(__file__).resolve().parents[1] / "data/samples/github_events.jsonl"


class SilverContractTests(unittest.TestCase):
    def test_sample_events_are_valid_and_keep_contract_fields(self):
        events = [json.loads(line) for line in SAMPLE.read_text().splitlines()]
        parsed = [classify_event(json.dumps(event)) for event in events]
        self.assertEqual(len(parsed), 6)
        self.assertTrue(all(record["reason"] is None for record in parsed))
        self.assertEqual(
            {record["event_id"] for record in parsed},
            {event["id"] for event in events},
        )
        self.assertTrue(all(record["payload_json"] for record in parsed))

    def test_invalid_json_has_clear_reason(self):
        result = classify_event("{")
        self.assertIn("invalid JSON", result["reason"])

    def test_nonstandard_json_numbers_are_rejected(self):
        result = classify_event(
            '{"id":"bad","type":"IssuesEvent","actor":{"login":"qa"},'
            '"repo":{"name":"qa/repo"},"created_at":"2025-01-01T15:00:00Z",'
            '"payload":{"value":NaN}}'
        )
        self.assertIn("non-standard numeric constant: NaN", result["reason"])

    def test_missing_fields_and_naive_timestamp_are_quarantinable(self):
        result = classify_event('{"id":"bad","created_at":"2025-01-01T15:00:00"}')
        self.assertIn("missing or invalid type", result["reason"])
        self.assertIn("missing or invalid actor.login", result["reason"])
        self.assertIn("missing or invalid repo.name", result["reason"])
        self.assertIn("created_at must contain a timezone", result["reason"])
        self.assertIn("payload must be a JSON object", result["reason"])


if __name__ == "__main__":
    unittest.main()
