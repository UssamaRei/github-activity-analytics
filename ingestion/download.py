"""Download selected UTC hours from GH Archive as original JSONL records."""

import argparse
import gzip
import os
import tempfile
import time
import zlib
from datetime import datetime
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ingestion.events import validate_event


def normalize_hour(hour: str) -> str:
    return datetime.strptime(hour, "%Y-%m-%d-%H").strftime("%Y-%m-%d-%H")


def archive_url(hour: str) -> str:
    timestamp = datetime.strptime(hour, "%Y-%m-%d-%H")
    return f"https://data.gharchive.org/{timestamp:%Y-%m-%d}-{timestamp.hour}.json.gz"


def download_once(url: str, output: Path, limit: int | None) -> int:
    request = Request(url, headers={"User-Agent": "IL05-GitHub-Analytics/0.1"})
    temporary = None
    count = 0
    try:
        with urlopen(request, timeout=60) as response, gzip.GzipFile(fileobj=response) as source:
            with tempfile.NamedTemporaryFile(dir=output.parent, suffix=".part", delete=False) as target:
                temporary = Path(target.name)
                for line in source:
                    raw = line.rstrip(b"\r\n")
                    if not raw.strip():
                        continue
                    validate_event(raw)
                    target.write(raw + b"\n")
                    count += 1
                    if limit is not None and count >= limit:
                        break
        if count == 0:
            raise ValueError("Archive contained no events")
        os.replace(temporary, output)
        return count
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def download_sample(hour: str, output: Path, limit: int | None = 1000) -> int:
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    url = archive_url(hour)
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}; choose a new output filename")
    output.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(3):
        try:
            return download_once(url, output, limit)
        except HTTPError as exc:
            if exc.code not in (408, 429) and not 500 <= exc.code < 600:
                raise
            if attempt == 2:
                raise
        except (URLError, HTTPException, TimeoutError, ConnectionError, EOFError, gzip.BadGzipFile, zlib.error):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hour", action="append", required=True, help="UTC YYYY-MM-DD-HH, repeatable")
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", type=Path, help="Explicit path for a single hour")
    destination.add_argument("--output-dir", type=Path, help="Directory for resumable hourly downloads")
    volume = parser.add_mutually_exclusive_group()
    volume.add_argument("--limit", type=int, default=1000, help="Sample size per hour (default: 1000)")
    volume.add_argument("--full-hour", action="store_true", help="Download every event for each hour")
    args = parser.parse_args()
    try:
        hours = list(dict.fromkeys(normalize_hour(hour) for hour in args.hour))
    except ValueError as exc:
        parser.error(str(exc))
    if args.limit < 1 or (args.output is not None and len(hours) != 1):
        parser.error("limit must be positive and --output requires a single hour")

    limit = None if args.full_hour else args.limit
    suffix = "full" if args.full_hour else f"sample-{limit}"
    mode = "complete hour" if args.full_hour else f"sample, up to {limit} events"
    for hour in hours:
        output = args.output or args.output_dir / f"{hour}-{suffix}.jsonl"
        if args.output_dir is not None and output.is_file():
            print(f"Already downloaded {hour}: {output} ({mode})")
            continue
        try:
            count = download_sample(hour, output, limit)
        except (ValueError, OSError, HTTPException, EOFError, zlib.error) as exc:
            parser.exit(1, f"Download failed for {hour}: {exc}\n")
        print(f"Saved {count} events to {output} from {archive_url(hour)} ({mode})")


if __name__ == "__main__":
    main()
