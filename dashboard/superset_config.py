"""Local Superset configuration; secrets stay in a private Docker volume."""

from pathlib import Path
from urllib.parse import quote

ROOT = Path('/run/il05-secrets')
SECRET_KEY = (ROOT / 'superset_secret_key').read_text().strip()
password = quote((ROOT / 'postgres_password').read_text().strip(), safe='')
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://il05:{}@serving-db:5432/superset'.format(password)
TALISMAN_ENABLED = False  # Local HTTP endpoints are bound to loopback by Compose.
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
ENABLE_PROXY_FIX = False
FEATURE_FLAGS = {'DASHBOARD_RBAC': True}
SQLALCHEMY_TRACK_MODIFICATIONS = False
ROW_LIMIT = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120
