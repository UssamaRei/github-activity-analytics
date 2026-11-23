"""Execute inside the runner to check publication isolation and read-only access."""

import copy
import json
from pathlib import Path
from unittest.mock import patch

from dashboard.serving import connect, publish


def main():
    import psycopg2
    from psycopg2.extras import execute_values
    tables = json.loads(Path('/opt/project/dashboard/sample_gold.json').read_text())['tables']
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT SUM(event_count) FROM demo.repository_activity_hourly')
            assert cursor.fetchone()[0] == 85
            cursor.execute('SELECT COUNT(*) FROM gold.repository_activity_hourly')
            assert cursor.fetchone()[0] == 0
            cursor.execute('SELECT source_snapshot, loaded_at FROM demo.publication')
            publication_before = cursor.fetchone()
        bad = copy.deepcopy(tables)
        bad['language_activity_hourly'][0]['event_count'] += 1
        try:
            publish(bad, 'demo', 'invalid-test', 'synthetic_demo')
        except ValueError:
            pass
        else:
            raise AssertionError('Invalid totals were accepted')
        try:
            publish(tables, 'gold', 'invalid-demo-in-gold', 'synthetic_demo')
        except ValueError:
            pass
        else:
            raise AssertionError('Demo data was accepted as real Gold')

        # Fail after the first table was changed, to exercise PostgreSQL rollback
        # rather than only validation before a transaction starts.
        changed = copy.deepcopy(tables)
        changed['repository_activity_hourly'][0]['repo_name'] = 'rollback-only/repo'
        insert_calls = 0

        def fail_second_insert(*args, **kwargs):
            nonlocal insert_calls
            insert_calls += 1
            if insert_calls == 2:
                raise RuntimeError('Deliberate mid-publication failure')
            return execute_values(*args, **kwargs)

        with patch('psycopg2.extras.execute_values', side_effect=fail_second_insert):
            try:
                publish(changed, 'demo', 'rollback-test', 'synthetic_demo')
            except RuntimeError as exc:
                assert str(exc) == 'Deliberate mid-publication failure'
            else:
                raise AssertionError('The injected publication failure did not occur')
        with connection.cursor() as cursor:
            cursor.execute('SELECT SUM(event_count) FROM demo.repository_activity_hourly')
            assert cursor.fetchone()[0] == 85
            cursor.execute("SELECT COUNT(*) FROM demo.repository_activity_hourly WHERE repo_name = 'rollback-only/repo'")
            assert cursor.fetchone()[0] == 0
            cursor.execute('SELECT source_snapshot, loaded_at FROM demo.publication')
            assert cursor.fetchone() == publication_before
        password = Path('/run/il05-secrets/reader_password').read_text().strip()
        reader = psycopg2.connect(host='serving-db', dbname='analytics', user='dashboard_reader', password=password)
        try:
            with reader.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM demo.repository_activity_hourly')
                assert cursor.fetchone()[0] == 6
                try:
                    cursor.execute('DELETE FROM demo.repository_activity_hourly')
                except psycopg2.errors.InsufficientPrivilege:
                    reader.rollback()
                else:
                    raise AssertionError('Dashboard reader unexpectedly has write access')
        finally:
            reader.close()
    finally:
        connection.close()
    print('PASS: validation and mid-transaction rollback preserve data/provenance; demo/Gold are isolated; dashboard access is read-only.')


if __name__ == '__main__':
    main()
