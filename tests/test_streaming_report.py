import io
import json
import unittest

from scripts.streaming_report import read_records, write_report


class StreamingReportTests(unittest.TestCase):
    def test_reads_docker_prefixed_structured_logs_and_reports_batches(self):
        lines = [
            "spark-bronze | Spark starting\n",
            'spark-bronze | STREAMING_METRIC {"event":"query_status","query_status":"running","logged_at":"2026-11-19T16:00:00Z"}\n',
            (
                'spark-bronze | STREAMING_METRIC '
                '{"event":"batch_progress","batch_id":7,"num_input_rows":6,'
                '"input_rows_per_second":1.5,"processed_rows_per_second":2.0,'
                '"batch_duration_ms":3000,"sources":[{"start_offset":{"t":{"0":3}},'
                '"end_offset":{"t":{"0":9}}}]}\n'
            ),
        ]
        records = read_records(lines)
        output = io.StringIO()

        write_report(records, output)

        report = output.getvalue()
        self.assertIn("batch_id  rows  input_rows/s", report)
        self.assertIn("7  6  1.50  2.00  3000", report)
        self.assertIn('"0":9', report)
        self.assertIn("1 batches, 6 input rows", report)
        self.assertIn("Latest query status: running", report)

    def test_rejects_malformed_structured_record(self):
        with self.assertRaisesRegex(ValueError, "Malformed streaming metric on line 1"):
            read_records(["STREAMING_METRIC {invalid json}\n"])

    def test_empty_logs_are_reported(self):
        output = io.StringIO()
        write_report(
            [
                {
                    "event": "query_status",
                    "query_status": "running",
                    "logged_at": "2026-11-19T16:00:00Z",
                }
            ],
            output,
        )
        self.assertEqual(
            output.getvalue(),
            "No batch progress records found.\n"
            "Latest query status: running (2026-11-19T16:00:00Z)\n",
        )


if __name__ == "__main__":
    unittest.main()
