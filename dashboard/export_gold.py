"""Run with spark-submit: export Hudi Gold tables plus a checked manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path

from dashboard.contracts import TABLES


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if (args.output / 'manifest.json').exists():
        print('A completed export already exists for this run.')
        return
    staging = args.output.with_name(args.output.name + '.pending')
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    spark = SparkSession.builder.appName('IL05-Gold-export').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
    manifest = {'format_version': 1, 'source_mode': 'hudi_gold', 'tables': {}}
    max_rows = int(os.getenv('MAX_EXPORT_ROWS', '100000'))
    try:
        for name, spec in TABLES.items():
            source = '/lake/gold/' + name
            frame = spark.read.format('hudi').load('hdfs://namenode:9000' + source)
            frame = frame.select(*spec['columns'])
            filename = name + '.jsonl'
            count = 0
            digest = hashlib.sha256()
            # Stream rows rather than collecting a full Gold dataset in Python memory.
            with (staging / filename).open('wb') as target:
                for text in frame.toJSON().toLocalIterator():
                    count += 1
                    if count > max_rows:
                        raise ValueError('Export exceeds MAX_EXPORT_ROWS for ' + name)
                    raw = text.encode('utf-8') + b'\n'
                    target.write(raw)
                    digest.update(raw)
            manifest['tables'][name] = {'file': filename, 'hudi_path': source, 'rows': count, 'sha256': digest.hexdigest()}
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        from dashboard.serving import load_export
        load_export(staging)  # Includes schema and cross-table quality checks.
        if args.output.exists():
            raise ValueError('Unexpected incomplete destination: ' + str(args.output))
        staging.rename(args.output)
        print('Gold export committed: ' + str(args.output))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
