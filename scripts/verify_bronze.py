"""Check that the sample records arrived in Bronze unchanged."""

import argparse
import json
import subprocess
import time
from collections import Counter
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", default="data/samples/github_events.jsonl")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    expected = Counter(line for line in Path(args.expected).read_bytes().splitlines() if line.strip())
    if not expected:
        parser.error("Expected file is empty")
    deadline = time.monotonic() + args.timeout
    last_error = "No Bronze files yet"
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-cat", "/lake/bronze/github_events/part-*"],
            capture_output=True, timeout=30,
        )
        if result.returncode == 0:
            actual = Counter(line for line in result.stdout.splitlines() if line.strip())
            missing = expected - actual
            if not missing:
                ids = {json.loads(line)["id"] for line in expected}
                print(f"PASS: {sum(expected.values())} expected raw records found unchanged in Bronze ({len(ids)} unique IDs).")
                print(f"Bronze contains {sum(actual.values())} records in total; replay duplicates are allowed in Bronze.")
                return
            last_error = f"Still missing {sum(missing.values())} expected records"
        else:
            last_error = result.stderr.decode("utf-8", errors="replace")[-600:]
        time.sleep(3)
    raise SystemExit(f"FAIL: {last_error}. Check: docker compose logs spark-bronze")


if __name__ == "__main__":
    main()
