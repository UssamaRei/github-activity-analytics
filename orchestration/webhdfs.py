"""Read committed Spark progress through HDFS's existing HTTP API."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import urlopen


def read(path, operation='OPEN'):
    base = os.getenv('WEBHDFS_URL', 'http://namenode:9870')
    url = base + '/webhdfs/v1' + quote(path, safe='/') + '?' + urlencode({'op': operation, 'user.name': 'github'})
    with urlopen(url, timeout=20) as response:
        return response.read()


def committed_offsets(topic, checkpoint='/checkpoints/github_events_bronze'):
    try:
        listing = json.loads(read(checkpoint + '/commits', 'LISTSTATUS'))
    except HTTPError as exc:
        if exc.code == 404:
            return {}
        raise
    batches = [int(item['pathSuffix']) for item in listing['FileStatuses']['FileStatus']
               if item['pathSuffix'].isdigit()]
    if not batches:
        return {}
    # Only read offsets with a corresponding commit, never an in-progress batch.
    body = read(checkpoint + '/offsets/' + str(max(batches))).decode('utf-8')
    return parse_offsets(body, topic)


def parse_offsets(body, topic):
    for line in reversed(body.splitlines()):
        if not line.startswith('{'):
            continue
        value = json.loads(line)
        if topic in value:
            return {str(key): int(offset) for key, offset in value[topic].items()}
    return {}


def offsets_reached(current, required):
    return bool(required) and all(current.get(str(key), -1) >= offset for key, offset in required.items())
