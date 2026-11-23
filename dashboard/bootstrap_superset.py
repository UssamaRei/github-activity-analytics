"""Create reproducible datasets, charts and two clearly separated dashboards."""

import json
from pathlib import Path


def metric(expression, label):
    return {'expressionType': 'SQL', 'sqlExpression': expression, 'label': label,
            'hasCustomLabel': True, 'optionName': 'metric_' + label.lower().replace(' ', '_')}


def chart_definitions():
    events = metric('SUM(event_count)', 'Events')
    return [
        ('Total events', 'repository_activity_hourly', 'big_number_total',
         {'metric': events, 'subheader': 'Events in selected period', 'y_axis_format': ',d'}),
        ('Hourly activity (UTC)', 'repository_activity_hourly', 'echarts_timeseries_line',
         {'x_axis': 'hour_utc', 'time_grain_sqla': 'PT1H', 'metrics': [events], 'groupby': [],
          'rich_tooltip': True, 'show_legend': False, 'x_axis_title': 'UTC hour', 'y_axis_title': 'Events'}),
        ('Repository ranking', 'repository_activity_hourly', 'echarts_timeseries_bar',
         {'x_axis': 'repo_name', 'metrics': [events], 'groupby': [], 'row_limit': 10,
          'orientation': 'horizontal', 'show_legend': False, 'y_axis_title': 'Events',
          'order_desc': True, 'x_axis_sort': 'Events', 'x_axis_sort_asc': True}),
        ('Activity by language', 'language_activity_hourly', 'pie',
         {'groupby': ['language'], 'metric': events, 'row_limit': 20,
          'show_labels': True, 'label_type': 'key_percent', 'show_legend': True}),
        ('Contributor ranking', 'contributor_activity_hourly', 'table',
         {'query_mode': 'aggregate', 'groupby': ['actor_login'], 'metrics': [events],
          'order_desc': True, 'row_limit': 20, 'page_length': 10, 'include_time': False}),
        ('Repository groups', 'repository_clusters', 'table',
         {'query_mode': 'raw', 'all_columns': ['snapshot_utc', 'repo_name', 'cluster_id', 'event_count', 'active_contributors'],
          'row_limit': 100, 'page_length': 10}),
        ('Publication status', 'publication', 'table',
         {'query_mode': 'raw', 'all_columns': ['source_mode', 'source_snapshot', 'loaded_at', 'row_count'],
          'row_limit': 1, 'page_length': 1}),
    ]


def layout(charts, schema):
    notice = ('**DEMO — SYNTHETIC DATA.** These events, languages and cluster labels are fictional. '
              'They demonstrate the dashboard and are not GitHub or trained KMeans results.' if schema == 'demo' else
              '**GOLD RESULTS.** Only validated exports of the Hudi Gold tables appear here. '
              'Empty charts mean no snapshot has been published. Check Publication status for the source and load time.')
    result = {
        'DASHBOARD_VERSION_KEY': 'v2',
        'ROOT_ID': {'id': 'ROOT_ID', 'type': 'ROOT', 'children': ['GRID_ID']},
        'GRID_ID': {'id': 'GRID_ID', 'type': 'GRID', 'children': ['NOTICE']},
        'NOTICE': {'id': 'NOTICE', 'type': 'MARKDOWN', 'parents': ['ROOT_ID', 'GRID_ID'],
                   'meta': {'code': notice, 'width': 12, 'height': 10}},
    }
    for index in range(0, len(charts), 2):
        row_id = 'ROW-' + str(index)
        result['GRID_ID']['children'].append(row_id)
        result[row_id] = {'id': row_id, 'type': 'ROW', 'parents': ['ROOT_ID', 'GRID_ID'],
                          'children': [], 'meta': {'background': 'BACKGROUND_TRANSPARENT'}}
        for chart in charts[index:index + 2]:
            width = 12 if index == 6 else (4 if chart == charts[4] else 8 if chart == charts[5] else 6)
            chart_id = 'CHART-' + str(chart.id)
            result[row_id]['children'].append(chart_id)
            result[chart_id] = {'id': chart_id, 'type': 'CHART', 'parents': ['ROOT_ID', 'GRID_ID', row_id],
                                'meta': {'chartId': chart.id, 'sliceName': chart.slice_name,
                                         'uuid': str(chart.uuid), 'width': width, 'height': 24 if index == 6 else 40}}
    return result


def main():
    from superset.app import create_app
    from dashboard.contracts import TABLES
    from orchestration.connection_uri import connection_uri

    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        admin = security_manager.find_user(username='student')
        if admin is None:
            admin = security_manager.add_user('student', 'Project', 'Admin', 'student@localhost.invalid',
                                              security_manager.find_role('Admin'),
                                              Path('/run/il05-secrets/ui_password').read_text().strip())
            if not admin:
                raise RuntimeError('Could not create the local administrator')
        database = db.session.query(Database).filter_by(database_name='IL05 PostgreSQL serving').one_or_none()
        if database is None:
            database = Database(database_name='IL05 PostgreSQL serving', sqlalchemy_uri=connection_uri('analytics', reader=True))
            db.session.add(database)
        database.expose_in_sqllab = True
        database.allow_run_async = False
        db.session.commit()
        for schema in ('demo', 'gold'):
            datasets = {}
            for name in list(TABLES) + ['publication']:
                dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=schema, table_name=name).one_or_none()
                if dataset is None:
                    dataset = SqlaTable(table_name=name, schema=schema, database=database, owners=[admin])
                    db.session.add(dataset)
                    db.session.commit()
                dataset.main_dttm_col = TABLES[name]['time'] if name in TABLES else 'loaded_at'
                dataset.fetch_metadata()
                db.session.commit()
                datasets[name] = dataset
            charts = []
            for title, table, kind, settings in chart_definitions():
                dataset_id = datasets[table].id
                datetime_column = datasets[table].main_dttm_col
                name = '{} / {}'.format(schema.upper(), title)
                chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
                if chart is None:
                    chart = Slice(slice_name=name, owners=[admin], datasource_id=dataset_id,
                                  datasource_type='table', viz_type=kind)
                    db.session.add(chart)
                chart.datasource_id = dataset_id
                chart.datasource_type = 'table'
                chart.viz_type = kind
                params = {'datasource': str(dataset_id) + '__table', 'viz_type': kind,
                          'time_range': 'No filter', 'granularity_sqla': datetime_column,
                          'adhoc_filters': [], **settings}
                chart.params = json.dumps(params)
                db.session.commit()
                charts.append(chart)
            slug = 'il05-' + schema
            dashboard = db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
            if dashboard is None:
                dashboard = Dashboard(slug=slug, owners=[admin])
                db.session.add(dashboard)
            dashboard.dashboard_title = 'IL05 — DEMO (synthetic data)' if schema == 'demo' else 'IL05 — Gold results'
            dashboard.published = True
            dashboard.slices = charts
            dashboard.position_json = json.dumps(layout(charts, schema))
            dashboard.json_metadata = json.dumps({
                'refresh_frequency': 60, 'color_scheme': 'supersetColors',
                'native_filter_configuration': [{
                    'id': 'NATIVE_FILTER-time-' + schema, 'name': 'UTC time range',
                    'filterType': 'filter_time', 'targets': [{}],
                    'defaultDataMask': {'extraFormData': {'time_range': 'No filter'},
                                        'filterState': {'value': 'No filter'}},
                    'controlValues': {'enableEmptyFilter': False}, 'cascadeParentIds': [],
                    'scope': {'rootPath': ['ROOT_ID'], 'excluded': [charts[-1].id]},
                    'type': 'NATIVE_FILTER', 'description': 'Publication status remains unfiltered.'
                }]
            })
            db.session.commit()
        print('Created/updated il05-demo and il05-gold dashboards; no real Gold rows were fabricated.')


if __name__ == '__main__':
    main()
