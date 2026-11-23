"""The explicit interface between Gold Hudi tables and the SQL serving copy."""

from __future__ import annotations

from datetime import datetime, timezone


TABLES = {
    'repository_activity_hourly': {
        'columns': ('hour_utc', 'repo_name', 'event_count', 'push_count', 'pr_count', 'issue_count', 'active_contributors'),
        'keys': ('hour_utc', 'repo_name'), 'time': 'hour_utc',
    },
    'language_activity_hourly': {
        'columns': ('hour_utc', 'language', 'event_count', 'active_repositories'),
        'keys': ('hour_utc', 'language'), 'time': 'hour_utc',
    },
    'contributor_activity_hourly': {
        'columns': ('hour_utc', 'actor_login', 'event_count', 'repo_count'),
        'keys': ('hour_utc', 'actor_login'), 'time': 'hour_utc',
    },
    'repository_clusters': {
        'columns': ('snapshot_utc', 'repo_name', 'cluster_id', 'event_count', 'active_contributors'),
        'keys': ('snapshot_utc', 'repo_name'), 'time': 'snapshot_utc',
    },
}
TEXT_COLUMNS = {'repo_name', 'actor_login', 'language'}


def utc_timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp must be an ISO 8601 string')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
        raise ValueError('Timestamp must explicitly use UTC')
    return stamp.astimezone(timezone.utc)


def validate_rows(table, rows):
    spec = TABLES[table]
    keys = set()
    if not rows:
        raise ValueError('{} has no rows; refusing to replace a published snapshot'.format(table))
    for row in rows:
        if set(row) != set(spec['columns']):
            raise ValueError('{} columns do not match the serving contract'.format(table))
        for name in spec['columns']:
            value = row[name]
            if name == spec['time']:
                stamp = utc_timestamp(value)
                if name == 'hour_utc' and (stamp.minute or stamp.second or stamp.microsecond):
                    raise ValueError('hour_utc must be the start of a UTC hour')
            elif name in TEXT_COLUMNS:
                if not isinstance(value, str) or not value.strip() or len(value) > 300:
                    raise ValueError('Invalid {}'.format(name))
            elif isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 2**63 - 1:
                raise ValueError('{} must be a nonnegative BIGINT integer'.format(name))
        # PostgreSQL treats Z and +00:00 as the same instant; validation must too.
        key = tuple(utc_timestamp(row[name]) if name == spec['time'] else row[name] for name in spec['keys'])
        if key in keys:
            raise ValueError('Duplicate key in {}'.format(table))
        keys.add(key)
        if 'active_contributors' in row and row['active_contributors'] > row['event_count']:
            raise ValueError('Contributors cannot exceed events')
        if table == 'repository_activity_hourly' and sum(row[k] for k in ('push_count', 'pr_count', 'issue_count')) > row['event_count']:
            raise ValueError('Event-type counts cannot exceed total events')
    return rows


def validate_bundle(tables):
    if set(tables) != set(TABLES):
        raise ValueError('A publication must contain all four Gold tables')
    for name, rows in tables.items():
        validate_rows(name, rows)
    totals = []
    for name in ('repository_activity_hourly', 'language_activity_hourly', 'contributor_activity_hourly'):
        by_hour = {}
        for row in tables[name]:
            hour = utc_timestamp(row['hour_utc']).isoformat()
            by_hour[hour] = by_hour.get(hour, 0) + row['event_count']
        totals.append(by_hour)
    if totals[0] != totals[1] or totals[0] != totals[2]:
        raise ValueError('Repository, language and contributor totals disagree by UTC hour')
    return tables
