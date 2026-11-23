#!/usr/bin/env bash
set -euo pipefail
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=reader_password="$(cat /run/il05-secrets/reader_password)" \
  --file=/opt/il05/databases.sql
