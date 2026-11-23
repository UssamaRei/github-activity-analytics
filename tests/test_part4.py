import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dashboard.contracts import TABLES, validate_bundle
from dashboard.serving import load_export
from orchestration import runner
from orchestration.webhdfs import committed_offsets, offsets_reached, parse_offsets


ROOT = Path(__file__).resolve().parents[1]


def fixture():
    return json.loads((ROOT / 'dashboard/sample_gold.json').read_text())['tables']


class GoldContractTests(unittest.TestCase):
    def test_demo_reconciles_to_85_events(self):
        tables = validate_bundle(fixture())
        self.assertEqual(sum(row['event_count'] for row in tables['repository_activity_hourly']), 85)

    def test_mismatched_hourly_totals_rejected(self):
        tables = fixture()
        tables['language_activity_hourly'][0]['event_count'] += 1
        with self.assertRaisesRegex(ValueError, 'totals disagree'):
            validate_bundle(tables)

    def test_duplicate_repository_hour_rejected(self):
        tables = fixture()
        tables['repository_activity_hourly'].append(copy.deepcopy(tables['repository_activity_hourly'][0]))
        with self.assertRaisesRegex(ValueError, 'Duplicate key'):
            validate_bundle(tables)

    def test_equivalent_utc_timestamp_keys_are_duplicates(self):
        tables = fixture()
        duplicate = copy.deepcopy(tables['repository_activity_hourly'][0])
        duplicate['hour_utc'] = duplicate['hour_utc'].replace('Z', '+00:00')
        tables['repository_activity_hourly'].append(duplicate)
        with self.assertRaisesRegex(ValueError, 'Duplicate key'):
            validate_bundle(tables)

    def test_invalid_counts_and_timestamps_rejected(self):
        for field, value in [('event_count', -1), ('event_count', True), ('event_count', 2**63), ('hour_utc', '2025-01-01T10:15:00Z'),
                             ('hour_utc', '2025-01-01T10:00:00'), ('hour_utc', '2025-01-01T10:00:00+01:00')]:
            tables = fixture()
            tables['repository_activity_hourly'][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate_bundle(tables)

    def test_empty_snapshot_cannot_replace_existing_results(self):
        tables = fixture()
        tables['repository_clusters'] = []
        with self.assertRaisesRegex(ValueError, 'no rows'):
            validate_bundle(tables)

    def make_export(self, root):
        manifest = {'format_version': 1, 'source_mode': 'hudi_gold', 'tables': {}}
        for name, rows in fixture().items():
            raw = '\n'.join(json.dumps(row) for row in rows).encode() + b'\n'
            (root / (name + '.jsonl')).write_bytes(raw)
            manifest['tables'][name] = {'file': name + '.jsonl', 'hudi_path': '/lake/gold/' + name,
                                       'rows': len(rows), 'sha256': hashlib.sha256(raw).hexdigest()}
        (root / 'manifest.json').write_text(json.dumps(manifest))

    def test_completed_export_checksums_and_rows_verified(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_export(root)
            tables, digest = load_export(root)
            self.assertEqual(len(tables), 4)
            self.assertEqual(len(digest), 64)
            with (root / 'repository_clusters.jsonl').open('ab') as stream:
                stream.write(b'{}\n')
            with self.assertRaisesRegex(ValueError, 'Checksum'):
                load_export(root)

    def test_synthetic_manifest_is_not_real_gold(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_export(root)
            path = root / 'manifest.json'
            manifest = json.loads(path.read_text())
            manifest['source_mode'] = 'synthetic_demo'
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'Hudi Gold'):
                load_export(root)


class BronzeHandoffTests(unittest.TestCase):
    def test_existing_old_data_does_not_satisfy_new_offsets(self):
        self.assertFalse(offsets_reached({'0': 6}, {'0': 12}))
        self.assertFalse(offsets_reached({'0': 12}, {'0': 12, '1': 3}))
        self.assertTrue(offsets_reached({'0': 12, '1': 4}, {'0': 12, '1': 3}))
        self.assertFalse(offsets_reached({}, {}))

    def test_only_committed_batch_is_used(self):
        listing = {'FileStatuses': {'FileStatus': [{'pathSuffix': '3'}, {'pathSuffix': '.3.crc'}, {'pathSuffix': '2'}]}}
        with patch('orchestration.webhdfs.read', side_effect=[json.dumps(listing).encode(),
                    b'v1\n{"batchTimestampMs":1}\n{"github_events":{"0":10}}']) as mocked:
            self.assertEqual(committed_offsets('github_events'), {'0': 10})
            self.assertTrue(mocked.call_args_list[1].args[0].endswith('/offsets/3'))

    def test_offset_parser_handles_spark_metadata(self):
        raw = 'v1\n{"batchWatermarkMs":0}\n{"github_events":{"0":42,"1":11}}'
        self.assertEqual(parse_offsets(raw, 'github_events'), {'0': 42, '1': 11})

    def test_retry_of_same_run_does_not_publish_twice(self):
        published = []
        class Producer:
            def __init__(self, **kwargs):
                pass
            def send(self, topic, key, value):
                published.append(value)
                return SimpleNamespace(get=lambda timeout: SimpleNamespace(partition=0, offset=11 + len(published)))
            def close(self, timeout):
                pass
        with tempfile.TemporaryDirectory() as folder, patch.object(runner, 'ROOT', ROOT), \
             patch.object(runner, 'RUNTIME', Path(folder)), \
             patch.dict('sys.modules', {'kafka': SimpleNamespace(KafkaProducer=Producer)}):
            first = runner.publish_events({'run_id': 'same-run'}, sample=True)
            second = runner.publish_events({'run_id': 'same-run'}, sample=True)
        self.assertEqual(first, second)
        self.assertEqual(first['required_offsets'], {'0': 18})
        self.assertEqual(len(published), 6)

    def test_missing_integrations_are_reported_as_missing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'orchestration').mkdir()
            (root / 'orchestration/job_config.json').write_text(json.dumps({'silver': {'script': 'storage/not_ready.py'}}))
            with patch.object(runner, 'ROOT', root), self.assertRaisesRegex(ValueError, 'Parts 2/3'):
                runner.preflight()

    def test_paths_outside_project_are_rejected(self):
        with patch.object(runner, 'integration_config', return_value={'bad': {'script': '../outside.py'}}):
            with self.assertRaisesRegex(ValueError, 'inside the project'):
                runner.preflight()


if __name__ == '__main__':
    unittest.main()
