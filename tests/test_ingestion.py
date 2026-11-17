import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.download import archive_url, download_sample
from ingestion.events import iter_events, validate_event


SAMPLE = Path(__file__).resolve().parents[1] / "data/samples/github_events.jsonl"


class IngestionTests(unittest.TestCase):
    def test_sample_records_are_preserved_byte_for_byte(self):
        original = [line for line in SAMPLE.read_bytes().splitlines() if line.strip()]
        events = list(iter_events(SAMPLE))
        self.assertEqual([raw for _, raw in events], original)
        self.assertEqual(len({event["id"] for event, _ in events}), 6)

    def test_gzip_limit_and_unknown_fields_preserved(self):
        event = json.loads(SAMPLE.read_bytes().splitlines()[0])
        event["future_field"] = {"unicode": "développement"}
        raw = json.dumps(event, ensure_ascii=False).encode("utf-8")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.json.gz"
            with gzip.open(path, "wb") as stream:
                stream.write(raw + b"\n" + raw + b"\n")
            records = list(iter_events(path, limit=1))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][1], raw)

    def test_invalid_event_reports_line(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bad.jsonl"
            path.write_text('\n{"id":"broken"}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, r"bad.jsonl:2"):
                list(iter_events(path))

    def test_naive_timestamp_rejected(self):
        event = json.loads(SAMPLE.read_bytes().splitlines()[0])
        event["created_at"] = "2025-01-01T15:00:00"
        with self.assertRaisesRegex(ValueError, "timezone"):
            validate_event(json.dumps(event).encode())

    def test_hour_url_and_invalid_hour(self):
        self.assertEqual(archive_url("2025-01-01-03"), "https://data.gharchive.org/2025-01-01-3.json.gz")
        with self.assertRaises(ValueError):
            archive_url("2025-01-01-25")

    def test_download_is_bounded_and_does_not_overwrite(self):
        content = gzip.compress(SAMPLE.read_bytes())
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "sample.jsonl"
            with patch("ingestion.download.urlopen", return_value=io.BytesIO(content)):
                self.assertEqual(download_sample("2025-01-01-15", output, limit=2), 2)
            self.assertEqual(len(output.read_bytes().splitlines()), 2)
            with self.assertRaises(FileExistsError):
                download_sample("2025-01-01-15", output)

    def test_failed_download_does_not_leave_partial_output(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "sample.jsonl"
            with patch("ingestion.download.urlopen", return_value=io.BytesIO(gzip.compress(b"invalid JSON\n"))):
                with self.assertRaises(ValueError):
                    download_sample("2025-01-01-15", output)
            self.assertFalse(output.exists())
            self.assertFalse(output.with_name(output.name + ".part").exists())


if __name__ == "__main__":
    unittest.main()
