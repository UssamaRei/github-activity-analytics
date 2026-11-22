"""Summarize STREAMING_METRIC records from Spark Bronze container logs."""

import argparse
import json
import sys


LOG_PREFIX = "STREAMING_METRIC "


def read_records(lines):
    records = []
    for line_number, line in enumerate(lines, 1):
        marker = line.find(LOG_PREFIX)
        if marker < 0:
            continue
        payload = line[marker + len(LOG_PREFIX) :].strip()
        try:
            record = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Malformed streaming metric on line {}: {}".format(line_number, exc)
            ) from exc
        if not isinstance(record, dict):
            raise ValueError(
                "Streaming metric on line {} must be a JSON object".format(
                    line_number
                )
            )
        records.append(record)
    return records


def _format_rate(value):
    return "-" if value is None else "{:.2f}".format(value)


def _write_latest_status(records, output):
    statuses = [
        record
        for record in records
        if record.get("event") in ("query_started", "query_status", "query_terminated")
    ]
    if statuses:
        latest = statuses[-1]
        print(
            "Latest query status: {} ({})".format(
                latest.get("query_status", "unknown"),
                latest.get("logged_at", "timestamp unavailable"),
            ),
            file=output,
        )


def write_report(records, output):
    batches = [record for record in records if record.get("event") == "batch_progress"]
    if not batches:
        print("No batch progress records found.", file=output)
        _write_latest_status(records, output)
        return

    print(
        "batch_id  rows  input_rows/s  processed_rows/s  duration_ms  Kafka offsets",
        file=output,
    )
    total_rows = 0
    total_duration_ms = 0
    durations = []
    for batch in batches:
        rows = batch.get("num_input_rows") or 0
        duration = batch.get("batch_duration_ms")
        total_rows += rows
        if isinstance(duration, (int, float)):
            total_duration_ms += duration
            durations.append(duration)
        offsets = [
            {
                "start": source.get("start_offset"),
                "end": source.get("end_offset"),
            }
            for source in batch.get("sources") or []
        ]
        print(
            "{batch_id}  {rows}  {input_rate}  {processed_rate}  {duration}  {offsets}".format(
                batch_id=batch.get("batch_id", "-"),
                rows=rows,
                input_rate=_format_rate(batch.get("input_rows_per_second")),
                processed_rate=_format_rate(
                    batch.get("processed_rows_per_second")
                ),
                duration=duration if duration is not None else "-",
                offsets=json.dumps(offsets, sort_keys=True, separators=(",", ":")),
            ),
            file=output,
        )

    effective_rate = (
        total_rows / (total_duration_ms / 1000)
        if total_duration_ms > 0
        else None
    )
    print(
        "Summary: {} batches, {} input rows, max duration {} ms, "
        "effective batch throughput {} rows/s.".format(
            len(batches),
            total_rows,
            max(durations) if durations else "unavailable",
            _format_rate(effective_rate),
        ),
        file=output,
    )
    _write_latest_status(records, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "log_file",
        nargs="?",
        default="-",
        help="Spark log file to read; defaults to stdin",
    )
    args = parser.parse_args()

    if args.log_file == "-":
        records = read_records(sys.stdin)
    else:
        with open(args.log_file, encoding="utf-8") as log_file:
            records = read_records(log_file)
    write_report(records, sys.stdout)


if __name__ == "__main__":
    main()
