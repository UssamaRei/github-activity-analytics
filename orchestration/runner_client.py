"""Small HTTP client shared by Airflow tasks and integration checks."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def call_job(name, payload):
    root = Path(os.getenv('IL05_SECRETS_DIR', '/run/il05-secrets'))
    token = (root / 'runner_token').read_text().strip()
    base = os.getenv('PIPELINE_RUNNER_URL', 'http://pipeline-runner:8090')
    request = Request(base + '/jobs/' + name, data=json.dumps(payload).encode(), method='POST',
                      headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token})
    try:
        with urlopen(request, timeout=930) as response:
            return json.load(response)
    except HTTPError as exc:
        try:
            reason = json.loads(exc.read())['error']
        except (ValueError, KeyError):
            reason = 'Runner returned HTTP ' + str(exc.code)
        raise RuntimeError(reason) from exc
