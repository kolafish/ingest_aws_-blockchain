#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


BUCKET = "aws-public-blockchain"
PREFIX = "v1.0/eth/transactions"


def parse_date(value):
    return date.fromisoformat(value)


def parse_size(value):
    text = value.strip().lower()
    units = [
        ("tib", 1 << 40),
        ("tb", 10**12),
        ("gib", 1 << 30),
        ("gb", 10**9),
        ("mib", 1 << 20),
        ("mb", 10**6),
        ("kib", 1 << 10),
        ("kb", 10**3),
        ("b", 1),
    ]
    for suffix, scale in units:
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)].strip()) * scale)
    return int(text)


def list_date(day):
    uri = f"s3://{BUCKET}/{PREFIX}/date={day}/"
    proc = subprocess.run(
        ["aws", "s3", "ls", "--no-sign-request", uri],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"aws s3 ls failed for {uri}: {proc.stderr.strip()}")
    entries = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4 or not parts[-1].endswith(".parquet"):
            continue
        size = int(parts[2])
        key = f"{PREFIX}/date={day}/{parts[-1]}"
        entries.append(
            {
                "source": "s3",
                "bucket": BUCKET,
                "key": key,
                "date": str(day),
                "size_bytes": size,
            }
        )
    return entries


def main():
    parser = argparse.ArgumentParser(
        description="Lock an Ethereum transactions S3 object list for repeatable benchmarks."
    )
    parser.add_argument("--start-date", required=True, help="Inclusive YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Inclusive YYYY-MM-DD.")
    parser.add_argument("--target-size", default="0", help="Stop after this byte target; 0 means all listed dates.")
    parser.add_argument("--output", required=True, help="Output S3 lock manifest JSON.")
    parser.add_argument("--name", default="eth_transactions_30gb_s3_lock")
    parser.add_argument("--dataset", default="eth_transactions")
    args = parser.parse_args()

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if end < start:
        raise SystemExit("--end-date must be >= --start-date")
    target = parse_size(args.target_size)

    selected = []
    total = 0
    day = start
    while day <= end:
        for entry in list_date(day):
            if target > 0 and total >= target:
                break
            selected.append(entry)
            total += entry["size_bytes"]
        if target > 0 and total >= target:
            break
        day += timedelta(days=1)

    if not selected:
        raise SystemExit("no parquet objects selected")

    manifest = {
        "name": args.name,
        "dataset": args.dataset,
        "region": "us-east-2",
        "target_bytes": target,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "entries": selected,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {out} entries={len(selected)} bytes={total}", file=sys.stderr)


if __name__ == "__main__":
    main()
