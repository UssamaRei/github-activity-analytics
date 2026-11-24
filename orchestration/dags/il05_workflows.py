"""Integrated manual/hourly processing and separate synthetic demonstrations."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from orchestration.runner_client import call_job


DEFAULTS = {'owner': 'il05', 'retries': 2, 'retry_delay': timedelta(seconds=10),
            'execution_timeout': timedelta(minutes=20)}


def context_payload():
    context = get_current_context()
    stamp = context.get('data_interval_start')
    if stamp is None:
        # Airflow 3 manual runs can have neither a logical date nor an interval.
        # Anchor to the run, not the task's clock, so retries crossing an hour
        # boundary still download and process the same completed UTC hour.
        anchor = context.get('logical_date') or context['dag_run'].run_after
        stamp = anchor.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return {'run_id': context['dag'].dag_id + ':' + context['run_id'],
            'hour': context['params'].get('source_hour') or stamp.astimezone(timezone.utc).strftime('%Y-%m-%d-%H'),
            'limit': context['params'].get('event_limit', 1000),
            'full_hour': context['params'].get('full_hour', False)}


@task
def job(name):
    return call_job(name, context_payload())


with DAG(dag_id='il05_sample_ingestion', schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
         catchup=False, max_active_runs=1, default_args=DEFAULTS, tags=['il05', 'working-demo'],
         description='Publish the six existing sample events, then verify their committed Bronze offsets.'):
    sent = job.override(task_id='publish_six_sample_events')('publish_sample')
    stored = job.override(task_id='verify_committed_bronze')('wait_for_bronze')
    sent >> stored


with DAG(dag_id='il05_refresh_demo_dashboard', schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
         catchup=False, max_active_runs=1, default_args=DEFAULTS, tags=['il05', 'synthetic-demo'],
         description='Refresh ONLY the synthetic demo schema; never writes real Gold.'):
    job.override(task_id='validate_and_publish_synthetic_snapshot')('refresh_demo')


def build_pipeline():
    steps = [
        ('check_parts_2_and_3', 'preflight'),
        ('download_utc_hour_sample', 'download_hour'),
        ('publish_hour_to_kafka', 'publish_hour'),
        ('wait_for_committed_bronze', 'wait_for_bronze'),
        ('build_silver_hudi', 'silver'),
        ('check_silver_quality', 'silver_quality'),
        ('build_repository_and_contributor_gold', 'repository_metrics'),
        ('build_language_gold', 'language_metrics'),
        ('train_repository_clusters', 'cluster_repositories'),
        ('export_and_validate_hudi_gold', 'export_gold'),
        ('publish_gold_to_sql', 'publish_gold'),
    ]
    previous = None
    for task_id, name in steps:
        current = job.override(task_id=task_id)(name)
        if previous is not None:
            previous >> current
        previous = current


def pipeline_params():
    return {'event_limit': Param(1000, type='integer', minimum=1, maximum=100000),
            'source_hour': Param(None, type=['null', 'string'], pattern=r'^\d{4}-\d{2}-\d{2}-\d{2}$'),
            'full_hour': Param(False, type='boolean')}


with DAG(dag_id='il05_hourly_pipeline', schedule=CronDataIntervalTimetable('0 * * * *', timezone='UTC'),
         start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1,
         is_paused_upon_creation=True, default_args=DEFAULTS, params=pipeline_params(), tags=['il05', 'real-data'],
         description='Completed UTC-hour pipeline; unpause when automatic ingestion is wanted.'):
    build_pipeline()


with DAG(dag_id='il05_pipeline_manual', schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
         catchup=False, max_active_runs=1, default_args=DEFAULTS, params=pipeline_params(), tags=['il05', 'real-data'],
         description='The same eleven pipeline steps, with a chosen historical source_hour and event_limit.'):
    build_pipeline()
