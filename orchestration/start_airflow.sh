#!/usr/bin/env bash
set -euo pipefail
# SimpleAuthManager opens its password file for updating, even if users exist.
# Keep the master secret volume read-only and give Airflow a private writable copy.
cp /run/il05-secrets/airflow_passwords.json /opt/airflow/simple_auth_manager_passwords.json
chmod 600 /opt/airflow/simple_auth_manager_passwords.json
exec airflow standalone
