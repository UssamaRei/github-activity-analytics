import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from ingestion.download import download_sample, main as download_main
from ingestion.producer import publish


SAMPLE = Path(__file__).resolve().parents[1] / "data/samples/github_events.jsonl"
HOUR = "2025-01-01-15"


class HourlyDownloadTests(unittest.TestCase):
    def test_full_download_retries_a_truncated_archive(self):
        original = SAMPLE.read_bytes()
        compressed = gzip.compress(original)
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "hour.jsonl"
            with patch("ingestion.download.urlopen", side_effect=[
                    io.BytesIO(compressed[:-8]), io.BytesIO(compressed)
                ]) as fetch, patch("ingestion.download.time.sleep") as sleep:
                self.assertEqual(download_sample(HOUR, output, limit=None), 6)
            self.assertEqual(fetch.call_count, 2)
            sleep.assert_called_once_with(1)
            self.assertEqual(output.read_bytes().splitlines(), original.splitlines())
            self.assertEqual(list(output.parent.glob("*.part")), [])

    def test_permanent_http_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "hour.jsonl"
            error = HTTPError("https://data.gharchive.org", 404, "Not found", {}, None)
            with patch("ingestion.download.urlopen", side_effect=error) as fetch:
                with self.assertRaises(HTTPError):
                    download_sample(HOUR, output)
            fetch.assert_called_once()
            self.assertFalse(output.exists())

    def test_batch_resumes_completed_downloads_and_keeps_modes_separate(self):
        with tempfile.TemporaryDirectory() as folder:
            args = ["download", "--hour", HOUR, "--hour", "2025-01-01-16", "--output-dir", folder]
            compressed = gzip.compress(SAMPLE.read_bytes())
            with patch("sys.argv", args + ["--limit", "2"]), \
                patch("ingestion.download.urlopen", side_effect=lambda *a, **kw: io.BytesIO(compressed)) as fetch:
                download_main()
                download_main()
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(len(list(Path(folder).glob("*-sample-2.jsonl"))), 2)
            with patch("sys.argv", args + ["--full-hour"]), \
                patch("ingestion.download.urlopen", side_effect=lambda *a, **kw: io.BytesIO(compressed)):
                download_main()
            self.assertEqual(len(list(Path(folder).glob("*-full.jsonl"))), 2)


class PublicationTests(unittest.TestCase):
    def test_partial_failure_replays_from_start_then_completed_hour_is_skipped(self):
        records = SAMPLE.read_bytes().splitlines()
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(
                hour=HOUR, input=SAMPLE, manifest=Path(folder) / "hour.json",
                limit=None, dry_run=False, interval=0,
            )
            first = Mock()
            first.send.return_value.get.side_effect = [
                SimpleNamespace(partition=0, offset=4), OSError("lost connection")
            ]
            with self.assertRaisesRegex(OSError, "lost connection"):
                publish(args, "github_events", ["kafka:9092"], Mock(return_value=first))
            partial = json.loads(args.manifest.read_text())
            self.assertEqual(partial["status"], "failed")
            self.assertEqual(partial["source_hour_utc"], HOUR)
            self.assertEqual(partial["records_sent"], 1)
            self.assertEqual(partial["offsets"]["0"]["last_offset"], 4)
            self.assertIn("lost connection", partial["error"])

            retry = Mock()
            retry.send.return_value.get.side_effect = [
                SimpleNamespace(partition=index % 2, offset=10 + index)
                for index in range(len(records))
            ]
            factory = Mock(return_value=retry)
            complete = publish(args, "github_events", ["kafka:9092"], factory)
            self.assertEqual(complete["status"], "complete")
            self.assertIsNone(complete["error"])
            self.assertEqual(complete["records_sent"], len(records))
            self.assertEqual(complete["offsets"]["0"], {
                "first_offset": 10, "last_offset": 14, "records_sent": 3
            })
            self.assertEqual(complete["offsets"]["1"]["last_offset"], 15)
            self.assertEqual(len(retry.send.call_args_list), len(records))
            for call, raw in zip(retry.send.call_args_list, records):
                self.assertEqual(call.args, ("github_events",))
                self.assertEqual(call.kwargs, {
                    "key": json.loads(raw)["repo"]["name"].encode(), "value": raw
                })
            self.assertEqual(publish(args, "github_events", ["kafka:9092"], factory), complete)
            factory.assert_called_once()

            args.limit = 2
            with self.assertRaisesRegex(ValueError, "different input"):
                publish(args, "github_events", ["kafka:9092"], factory)
            self.assertEqual(json.loads(args.manifest.read_text()), complete)

    def test_default_limits_do_not_truncate_full_input(self):
        raw = SAMPLE.read_bytes().splitlines()[0]
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "full.jsonl"
            source.write_bytes((raw + b"\n") * 1001)
            args = SimpleNamespace(hour=None, input=source, manifest=None, limit=None, dry_run=True)
            result = publish(args, "github_events", ["kafka:9092"])
        self.assertEqual(result["events"], 1001)
