"""Atomically publish a validated snapshot to demo or gold PostgreSQL schemas."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from dashboard.contracts import TABLES, TEXT_COLUMNS, validate_bundle


def load_export(folder):
    folder = Path(folder)
    manifest = json.loads((folder / 'manifest.json').read_text())
    if manifest.get('format_version') != 1 or manifest.get('source_mode') != 'hudi_gold':
        raise ValueError('Real publication requires a version-1 Hudi Gold export manifest')
    if set(manifest.get('tables', {})) != set(TABLES):
        raise ValueError('Incomplete Gold export')
    tables = {}
    for name in TABLES:
        info = manifest['tables'][name]
        if info.get('file') != name + '.jsonl' or info.get('hudi_path') != '/lake/gold/' + name:
            raise ValueError('Unexpected export path for ' + name)
        content = (folder / info['file']).read_bytes()
        if hashlib.sha256(content).hexdigest() != info.get('sha256'):
            raise ValueError('Checksum mismatch for ' + name)
        tables[name] = [json.loads(line) for line in content.splitlines() if line.strip()]
        if len(tables[name]) != info.get('rows'):
            raise ValueError('Row-count mismatch for ' + name)
    validate_bundle(tables)
    return tables, hashlib.sha256((folder / 'manifest.json').read_bytes()).hexdigest()


def connect():
    import psycopg2
    secret = Path(os.getenv('IL05_SECRETS_DIR', '/run/il05-secrets'))
    return psycopg2.connect(host='serving-db', port=5432, dbname='analytics', user='il05',
                            password=(secret / 'postgres_password').read_text().strip(), connect_timeout=10)


def ensure_tables(connection, schema):
    from psycopg2 import sql
    if schema not in ('demo', 'gold'):
        raise ValueError('Unknown serving schema')
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
        for name, spec in TABLES.items():
            fields = []
            for column in spec['columns']:
                kind = 'TIMESTAMPTZ' if column == spec['time'] else ('TEXT' if column in TEXT_COLUMNS else 'BIGINT')
                fields.append(sql.SQL('{} {} NOT NULL').format(sql.Identifier(column), sql.SQL(kind)))
            fields.append(sql.SQL('PRIMARY KEY ({})').format(sql.SQL(',').join(map(sql.Identifier, spec['keys']))))
            cursor.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {}.{} ({})').format(
                sql.Identifier(schema), sql.Identifier(name), sql.SQL(',').join(fields)))
        cursor.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {}.publication (source_mode TEXT NOT NULL, source_snapshot TEXT NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, row_count BIGINT NOT NULL)').format(sql.Identifier(schema)))


def publish(tables, schema, snapshot, source_mode):
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    if (schema, source_mode) not in (('demo', 'synthetic_demo'), ('gold', 'hudi_gold')):
        raise ValueError('Synthetic data and real Gold must use separate schemas')
    validate_bundle(tables)  # Validate everything before opening the transaction.
    connection = connect()
    try:
        with connection:
            ensure_tables(connection, schema)
            with connection.cursor() as cursor:
                # Serialize publishers so two manual requests cannot interleave snapshots.
                cursor.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ('il05-' + schema,))
                for name, spec in TABLES.items():
                    cursor.execute(sql.SQL('DELETE FROM {}.{}').format(sql.Identifier(schema), sql.Identifier(name)))
                    query = sql.SQL('INSERT INTO {}.{} ({}) VALUES %s').format(
                        sql.Identifier(schema), sql.Identifier(name), sql.SQL(',').join(map(sql.Identifier, spec['columns'])))
                    execute_values(cursor, query.as_string(connection), [tuple(row[c] for c in spec['columns']) for row in tables[name]])
                cursor.execute(sql.SQL('DELETE FROM {}.publication').format(sql.Identifier(schema)))
                cursor.execute(sql.SQL('INSERT INTO {}.publication (source_mode, source_snapshot, row_count) VALUES (%s,%s,%s)').format(sql.Identifier(schema)),
                               (source_mode, snapshot, sum(map(len, tables.values()))))
        return {'schema': schema, 'source_mode': source_mode, 'source_snapshot': snapshot,
                'rows': {name: len(rows) for name, rows in tables.items()}}
    finally:
        connection.close()


def initialize_demo():
    path = Path(os.getenv('PROJECT_ROOT', '/opt/project')) / 'dashboard/sample_gold.json'
    content = path.read_bytes()
    tables = json.loads(content)['tables']
    connection = connect()
    try:
        with connection:
            ensure_tables(connection, 'gold')  # Empty until an actual export is published.
    finally:
        connection.close()
    return publish(tables, 'demo', hashlib.sha256(content).hexdigest(), 'synthetic_demo')
