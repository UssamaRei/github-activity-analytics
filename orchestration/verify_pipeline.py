"""Trigger the integrated real-data DAG and report actual scheduler task states."""

import argparse
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import psycopg2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hour', default='2025-01-01-15')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--run-id', default='integration-' + uuid4().hex[:12])
    args = parser.parse_args()
    dag_id = 'il05_pipeline_manual'
    connection = psycopg2.connect(host='serving-db', dbname='airflow', user='il05',
        password=Path('/run/il05-secrets/postgres_password').read_text().strip())
    connection.autocommit = True
    previous = None
    try:
        for command in (['unpause', dag_id], ['trigger', '--run-id', args.run_id,
                '--conf', json.dumps({'source_hour': args.hour, 'event_limit': args.limit}), dag_id]):
            subprocess.run(['airflow', 'dags'] + command, check=True, capture_output=True, timeout=120)
        print('Triggered {} / {}'.format(dag_id, args.run_id), flush=True)
        deadline = time.monotonic() + 2400
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute('SELECT task_id, state, try_number FROM task_instance '
                               'WHERE dag_id=%s AND run_id=%s ORDER BY task_id', (dag_id, args.run_id))
                tasks = cursor.fetchall()
                cursor.execute('SELECT state FROM dag_run WHERE dag_id=%s AND run_id=%s', (dag_id, args.run_id))
                row = cursor.fetchone()
            state = row[0] if row else 'queued'
            if tasks != previous:
                print(json.dumps({'state': state, 'tasks': tasks}), flush=True)
                previous = tasks
            if state in ('success', 'failed'):
                if state != 'success' or len(tasks) != 11 or any(t[1] != 'success' for t in tasks):
                    raise RuntimeError('Integrated pipeline failed: ' + args.run_id)
                print('PASS: all 11 real-data pipeline tasks succeeded. Run ID: ' + args.run_id, flush=True)
                return
            time.sleep(5)
        raise TimeoutError('Pipeline did not finish within 40 minutes: ' + args.run_id)
    finally:
        connection.close()


if __name__ == '__main__':
    main()
