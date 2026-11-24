"""Execute inside Superset to check authenticated chart queries and filtering."""

import json
import argparse
from pathlib import Path

import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-real-events', type=int)
    args = parser.parse_args()
    session = requests.Session()
    base = 'http://localhost:8088'
    response = session.post(base + '/api/v1/security/login', json={
        'username': 'student', 'password': Path('/run/il05-secrets/ui_password').read_text().strip(),
        'provider': 'db', 'refresh': False}, timeout=30)
    response.raise_for_status()
    session.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
    csrf = session.get(base + '/api/v1/security/csrf_token/', timeout=30)
    csrf.raise_for_status()
    session.headers['X-CSRFToken'] = csrf.json()['result']
    datasets = session.get(base + '/api/v1/dataset/', params={'q': '(page_size:100)'}, timeout=30)
    datasets.raise_for_status()
    by_name = {(item['schema'], item['table_name']): item['id'] for item in datasets.json()['result']}

    def query(schema, table, columns, metrics, time_range='No filter'):
        time_column = 'loaded_at' if table == 'publication' else ('snapshot_utc' if table == 'repository_clusters' else 'hour_utc')
        body = {'datasource': {'id': by_name[(schema, table)], 'type': 'table'},
                'queries': [{'columns': columns, 'metrics': metrics, 'granularity': time_column,
                             'time_range': time_range, 'filters': [], 'extras': {'where': '', 'having': ''},
                             'orderby': [], 'row_limit': 1000}],
                'result_format': 'json', 'result_type': 'full'}
        result = session.post(base + '/api/v1/chart/data', json=body, timeout=60)
        if not result.ok:
            raise AssertionError('Chart query failed: ' + result.text[:1000])
        data = result.json()['result'][0]
        if data.get('error'):
            raise AssertionError(data['error'])
        return data['data']

    events = {'expressionType': 'SQL', 'sqlExpression': 'SUM(event_count)', 'label': 'Events'}
    assert query('demo', 'repository_activity_hourly', [], [events])[0]['Events'] == 85
    assert query('demo', 'repository_activity_hourly', [], [events], '2025-01-01T10:00:00 : 2025-01-01T11:00:00')[0]['Events'] == 35
    expected = {
        'repository_activity_hourly': ('repo_name', {'demo-org/alpha': 25, 'demo-org/beta': 45, 'demo-org/gamma': 15}),
        'language_activity_hourly': ('language', {'Python': 40, 'Java': 45}),
        'contributor_activity_hourly': ('actor_login', {'demo-alice': 35, 'demo-bob': 30, 'demo-charlie': 20}),
    }
    for table, (column, totals) in expected.items():
        rows = query('demo', table, [column], [events])
        assert {row[column]: row['Events'] for row in rows} == totals, table
    assert len(query('demo', 'repository_clusters', ['repo_name', 'cluster_id'], [])) == 3
    provenance = query('demo', 'publication', ['source_mode', 'loaded_at'], [])
    assert provenance[0]['source_mode'] == 'synthetic_demo'
    assert query('gold', 'repository_activity_hourly', [], [events])[0]['Events'] == args.expected_real_events
    if args.expected_real_events is not None:
        for table in ('language_activity_hourly', 'contributor_activity_hourly'):
            assert query('gold', table, [], [events])[0]['Events'] == args.expected_real_events
        assert query('gold', 'publication', ['source_mode'], [])[0]['source_mode'] == 'hudi_gold'
    for schema in ('demo', 'gold'):
        for table, key in (('repository_growth_hourly', 'repo_name'), ('language_growth_hourly', 'language')):
            query(schema, table, ['hour_utc', key, 'previous_event_count', 'growth_pct'], [])
    for slug in ('il05-demo', 'il05-gold'):
        page = session.get(base + '/superset/dashboard/' + slug + '/', timeout=30)
        page.raise_for_status()
    print('PASS: authenticated dashboard queries, totals, UTC filtering, growth views and provenance. Real events: {}.'.format(args.expected_real_events))


if __name__ == '__main__':
    main()
