"""Read local database credentials without embedding them in Compose."""

import os
import sys
from pathlib import Path
from urllib.parse import quote


def connection_uri(database, reader=False):
    if database not in ('airflow', 'superset', 'analytics'):
        raise ValueError('Unknown database')
    root = Path(os.getenv('IL05_SECRETS_DIR', '/run/il05-secrets'))
    password = (root / ('reader_password' if reader else 'postgres_password')).read_text().strip()
    user = 'dashboard_reader' if reader else 'il05'
    return 'postgresql+psycopg2://{}:{}@serving-db:5432/{}'.format(user, quote(password, safe=''), database)


if __name__ == '__main__':
    sys.stdout.write(connection_uri(sys.argv[1]))
