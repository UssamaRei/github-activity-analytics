#!/usr/bin/env bash
set -euo pipefail
superset db upgrade
superset init
python /opt/project/dashboard/bootstrap_superset.py
exec gunicorn --bind 0.0.0.0:8088 --workers 1 --worker-class gthread --threads 4 --timeout 120 'superset.app:create_app()'
