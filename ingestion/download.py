"""Stream a bounded sample of an hourly GH Archive file to JSONL."""

import argparse
import gzip
import os
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

from ingestion.events import validate_event


def archive_url(hour: str) -> str:
    # Hours are interpreted in UTC. GH Archive uses an unpadded hour suffix.
    timestamp = datetime.strptime(hour, "%Y-%m-%d-%H")
    return f"https://data.gharchive.org/{timestamp:%Y-%m-%d}-{timestamp.hour}.json.gz"


def download_sample(hour: str, output: Path, limit: int = 1000) -> int:
    if limit < 1:
        raise ValueError("limit must be positive")
    url = archive_url(hour)
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}; choose a new output filename")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".part")
    count = 0
    try:
        request = Request(url, headers={"User-Agent": "IL05-GitHub-Analytics/0.1"})
        with urlopen(request, timeout=60) as response, gzip.GzipFile(fileobj=response) as source, temporary.open("xb") as target:
            for line in source:
                raw = line.rstrip(b"\r\n")
                if not raw.strip():
                    continue
                validate_event(raw)
                target.write(raw + b"\n")
                count += 1
                if count >= limit:
                    break
        if count == 0:
            raise ValueError("Archive contained no events")
        os.replace(temporary, output)
    except BaseException:
        # Only clean up a partial file this invocation actually created.
        if 'target' in locals():
            temporary.unlink(missing_ok=True)
        raise
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hour", required=True, help="UTC hour, e.g. 2025-01-01-15")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    try:
        count = download_sample(args.hour, args.output, args.limit)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"Download failed: {exc}\n")
    print(f"Saved {count} events to {args.output} from {archive_url(args.hour)}")


if __name__ == "__main__":
    main()
