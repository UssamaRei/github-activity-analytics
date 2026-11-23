"""Generate persistent local credentials in a Docker volume, never in Git."""

import base64
import json
import os
import secrets
from pathlib import Path


def initialize(secret_dir=Path('/secrets'), runtime_dir=Path('/runtime')):
    secret_dir.mkdir(parents=True, exist_ok=True)
    names = ('postgres_password', 'reader_password', 'ui_password',
             'superset_secret_key', 'airflow_jwt_secret', 'runner_token')
    for name in names:
        path = secret_dir / name
        if not path.exists():
            path.write_text(secrets.token_urlsafe(36), encoding='utf-8')
        path.chmod(0o644)
    fernet = secret_dir / 'airflow_fernet_key'
    if not fernet.exists():
        fernet.write_text(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(), encoding='utf-8')
    password_file = secret_dir / 'airflow_passwords.json'
    password_file.write_text(json.dumps({'student': (secret_dir / 'ui_password').read_text()}), encoding='utf-8')
    for folder in (runtime_dir, runtime_dir / 'runs', runtime_dir / 'downloads', runtime_dir / 'exports'):
        folder.mkdir(parents=True, exist_ok=True)
        if hasattr(os, 'chown'):
            os.chown(folder, 185, 0)
        folder.chmod(0o770)
    print('Local credentials initialized; no passwords are written to the repository.')


if __name__ == '__main__':
    initialize()
