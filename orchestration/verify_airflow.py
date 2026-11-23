"""Run inside Airflow: exercise the real scheduler and restore DAG pause settings."""

import subprocess
import time
from pathlib import Path
from uuid import uuid4

import psycopg2

from orchestration.runner_client import call_job


def main():
    connection = psycopg2.connect(
        host='serving-db', dbname='airflow', user='il05',
        password=Path('/run/il05-secrets/postgres_password').read_text().strip(),
        connect_timeout=10)
    connection.autocommit = True

    def command(*arguments):
        result = subprocess.run(['airflow', 'dags', *arguments], capture_output=True,
                                text=True, timeout=120)
        if result.returncode:
            raise RuntimeError(result.stdout[-2000:] + result.stderr[-2000:])

    try:
        for dag_id in ('il05_sample_ingestion', 'il05_refresh_demo_dashboard'):
            with connection.cursor() as cursor:
                cursor.execute('SELECT is_paused FROM dag WHERE dag_id = %s', (dag_id,))
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError('Wait for DAG parsing before verification: ' + dag_id)
                was_paused = row[0]
            run_id = 'part4-verification-' + uuid4().hex[:12]
            try:
                command('unpause', dag_id)
                command('trigger', '--run-id', run_id, dag_id)
                print('Triggered {} / {}'.format(dag_id, run_id), flush=True)
                deadline = time.monotonic() + 300
                while time.monotonic() < deadline:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT state FROM dag_run WHERE dag_id = %s AND run_id = %s',
                                       (dag_id, run_id))
                        row = cursor.fetchone()
                    state = row[0] if row else 'not found'
                    if state in ('success', 'failed'):
                        break
                    time.sleep(5)
                with connection.cursor() as cursor:
                    cursor.execute('SELECT task_id, state, try_number FROM task_instance '
                                   'WHERE dag_id = %s AND run_id = %s ORDER BY task_id', (dag_id, run_id))
                    tasks = cursor.fetchall()
                print('{}: {}; tasks={}'.format(dag_id, state, tasks), flush=True)
                if state != 'success' or not tasks or any(task[1] != 'success' for task in tasks):
                    raise AssertionError('Airflow workflow did not finish successfully')
            finally:
                if was_paused:
                    command('pause', dag_id)

        try:
            call_job('preflight', {'run_id': 'verification-preflight'})
        except RuntimeError as exc:
            if 'Parts 2/3 are not connected yet' not in str(exc):
                raise
            print('Expected integration preflight failure: ' + str(exc), flush=True)
        else:
            print('Integration scripts now exist; preflight is ready.', flush=True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT is_paused FROM dag WHERE dag_id = 'il05_hourly_pipeline'")
            if not cursor.fetchone()[0]:
                raise AssertionError('Keep the hourly workflow paused until full integration is verified')
        print('PASS: actual scheduled tasks succeeded; the hourly workflow remains paused.', flush=True)
    finally:
        connection.close()


if __name__ == '__main__':
    main()
