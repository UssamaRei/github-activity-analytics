"""Private, fixed-job runner: Airflow needs no Docker socket or host shell."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import subprocess
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ingestion.events import iter_events
from orchestration.webhdfs import committed_offsets, offsets_reached

ROOT = Path(os.getenv('PROJECT_ROOT', '/opt/project')).resolve()
RUNTIME = Path(os.getenv('PIPELINE_RUNTIME', str(ROOT / 'data/runtime'))).resolve()
LOCK = threading.Lock()


def run_folder(payload):
    run_id = payload.get('run_id')
    if not isinstance(run_id, str) or not 1 <= len(run_id) <= 300:
        raise ValueError('A run_id of 1-300 characters is required')
    folder = RUNTIME / 'runs' / hashlib.sha256(run_id.encode()).hexdigest()[:24]
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def source_hour(payload):
    hour = payload.get('hour')
    if not isinstance(hour, str):
        raise ValueError('hour must be YYYY-MM-DD-HH in UTC')
    datetime.strptime(hour, '%Y-%m-%d-%H')
    if len(hour) != 13:
        raise ValueError('hour must include a two-digit UTC hour')
    return hour


def integration_config():
    return json.loads((ROOT / 'orchestration/job_config.json').read_text())


def preflight():
    missing = []
    for name, config in integration_config().items():
        path = (ROOT / config['script']).resolve()
        if ROOT not in path.parents:
            raise ValueError('Integration scripts must stay inside the project')
        if not path.is_file():
            missing.append('{}: {}'.format(name, config['script']))
    if missing:
        raise ValueError('Parts 2/3 are not connected yet. Missing jobs: ' + '; '.join(missing))
    return {'ready': True}


def download_hour(payload):
    from ingestion.download import download_sample
    hour = source_hour(payload)
    limit = payload.get('limit', 1000)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100000:
        raise ValueError('limit must be between 1 and 100000 events')
    output = RUNTIME / 'downloads' / (hour + '-' + str(limit) + '.jsonl')
    if not output.exists():
        download_sample(hour, output, limit)
    return {'hour': hour, 'events': sum(1 for _ in iter_events(output)), 'sample_limit': limit}


def publish_events(payload, sample=False):
    from kafka import KafkaProducer
    folder = run_folder(payload)
    marker = folder / 'kafka.json'
    if marker.exists():
        return json.loads(marker.read_text())
    if sample:
        path = ROOT / 'data/samples/github_events.jsonl'
    else:
        hour = source_hour(payload)
        path = RUNTIME / 'downloads' / (hour + '-' + str(payload.get('limit', 1000)) + '.jsonl')
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(','),
                             acks='all', retries=5, max_in_flight_requests_per_connection=1)
    required = {}
    count = 0
    topic = os.getenv('KAFKA_TOPIC', 'github_events')
    try:
        for event, raw in iter_events(path):
            metadata = producer.send(topic, key=event['repo']['name'].encode(), value=raw).get(timeout=30)
            partition = str(metadata.partition)
            required[partition] = max(required.get(partition, 0), metadata.offset + 1)
            count += 1
    finally:
        producer.close(timeout=10)
    if not count:
        raise ValueError('No events published')
    result = {'topic': topic, 'events': count, 'required_offsets': required,
              'source_mode': 'synthetic_sample' if sample else 'gharchive_sample'}
    temporary = folder / 'kafka.json.pending'
    temporary.write_text(json.dumps(result), encoding='utf-8')
    temporary.replace(marker)
    return result


def wait_for_bronze(payload):
    marker = run_folder(payload) / 'kafka.json'
    if not marker.exists():
        raise ValueError('Publish this run before checking Bronze')
    published = json.loads(marker.read_text())
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        current = committed_offsets(published['topic'])
        if offsets_reached(current, published['required_offsets']):
            return {'committed_offsets': current, 'required_offsets': published['required_offsets'], 'durable': True}
        time.sleep(3)
    raise TimeoutError('Spark has not committed this run to Bronze; check spark-bronze and HDFS')


def spark_command(script, arguments):
    return ['/opt/spark/bin/spark-submit', '--master', 'local[2]', '--driver-memory', '768m',
            '--conf', 'spark.jars.ivy=/tmp/ivy', '--conf', 'spark.jars.ivySettings=/opt/project/storage/ivysettings.xml',
            '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
            '--conf', 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog',
            '--packages', os.getenv('HUDI_PACKAGE', 'org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0'),
            str(script)] + arguments


def run_spark_job(name, payload):
    config = integration_config()[name]
    script = (ROOT / config['script']).resolve()
    if ROOT not in script.parents or not script.is_file():
        raise ValueError('Integration job not available: ' + config['script'])
    hour = source_hour(payload)
    arguments = [value.replace('{hour}', hour) for value in config.get('args', [])]
    execute_spark(script, arguments, run_folder(payload) / (name + '.log'))
    return {'job': name, 'status': 'completed'}


def execute_spark(script, arguments, log):
    # Arguments are lists from a reviewed repo config; there is no shell interpolation.
    with log.open('w') as output:
        result = subprocess.run(spark_command(script, arguments), cwd=ROOT, stdout=output,
                                stderr=subprocess.STDOUT, timeout=900, env=os.environ.copy())
    if result.returncode:
        tail = log.read_text(errors='replace')[-2500:]
        raise RuntimeError('Spark job failed ({}): {}'.format(script.name, tail))


def export_gold(payload):
    output = run_folder(payload) / 'gold-export'
    execute_spark(ROOT / 'dashboard/export_gold.py', ['--output', str(output)], run_folder(payload) / 'export.log')
    from dashboard.serving import load_export
    tables, snapshot = load_export(output)
    return {'snapshot': snapshot, 'rows': {name: len(rows) for name, rows in tables.items()}}


def publish_gold(payload):
    from dashboard.serving import load_export, publish
    tables, snapshot = load_export(run_folder(payload) / 'gold-export')
    return publish(tables, 'gold', snapshot, 'hudi_gold')


def dispatch(name, payload):
    if name == 'preflight':
        return preflight()
    if name == 'download_hour':
        return download_hour(payload)
    if name in ('publish_sample', 'publish_hour'):
        return publish_events(payload, sample=name == 'publish_sample')
    if name == 'wait_for_bronze':
        return wait_for_bronze(payload)
    if name == 'export_gold':
        return export_gold(payload)
    if name == 'publish_gold':
        return publish_gold(payload)
    if name == 'refresh_demo':
        from dashboard.serving import initialize_demo
        return initialize_demo()
    if name in integration_config():
        return run_spark_job(name, payload)
    raise ValueError('Unknown job: ' + name)


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body):
        content = json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        if self.path == '/health':
            self.reply(200, {'status': 'ok'})
        else:
            self.reply(404, {'error': 'Not found'})

    def do_POST(self):
        token = Path('/run/il05-secrets/runner_token').read_text().strip()
        if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
            self.reply(401, {'error': 'Unauthorized'})
            return
        if not self.path.startswith('/jobs/'):
            self.reply(404, {'error': 'Not found'})
            return
        if not LOCK.acquire(blocking=False):
            self.reply(503, {'error': 'Another pipeline job is running; retry later'})
            return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 16384:
                raise ValueError('Invalid request size')
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise ValueError('Payload must be a JSON object')
            self.reply(200, dispatch(self.path[len('/jobs/'):], payload))
        except (ValueError, FileNotFoundError) as exc:
            self.reply(409, {'error': str(exc)})
        except Exception as exc:
            self.reply(500, {'error': '{}: {}'.format(type(exc).__name__, exc)})
        finally:
            LOCK.release()


if __name__ == '__main__':
    from dashboard.serving import initialize_demo
    print(json.dumps(initialize_demo()), flush=True)
    print('Fixed-job runner listening on internal port 8090', flush=True)
    ThreadingHTTPServer(('0.0.0.0', 8090), Handler).serve_forever()
