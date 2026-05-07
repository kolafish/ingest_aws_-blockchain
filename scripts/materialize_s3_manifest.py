#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def download(entry, output_dir):
    filename = entry["key"].rsplit("/", 1)[-1]
    dest = output_dir / f"date={entry['date']}" / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size == entry["size_bytes"]:
        return dest
    uri = f"s3://{entry['bucket']}/{entry['key']}"
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    proc = subprocess.run(
        ["aws", "s3", "cp", "--no-sign-request", "--only-show-errors", uri, str(tmp)],
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"download failed: {uri}")
    if tmp.stat().st_size != entry["size_bytes"]:
        raise RuntimeError(f"size mismatch for {uri}: got {tmp.stat().st_size}, want {entry['size_bytes']}")
    os.replace(tmp, dest)
    return dest


def main():
    parser = argparse.ArgumentParser(
        description="Download a locked S3 manifest and emit a local writer manifest."
    )
    parser.add_argument("--s3-manifest", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--local-manifest", required=True)
    parser.add_argument("--name", default="eth_transactions_30gb_local")
    args = parser.parse_args()

    source = json.loads(Path(args.s3_manifest).read_text())
    output_dir = Path(args.output_dir).resolve()
    local_entries = []
    total = 0
    for i, entry in enumerate(source["entries"], 1):
        if entry.get("source") != "s3":
            raise RuntimeError(f"entry {i} is not an s3 entry")
        dest = download(entry, output_dir)
        total += entry["size_bytes"]
        local_entries.append(
            {
                "source": "local",
                "path": str(dest),
                "date": entry["date"],
                "size_bytes": entry["size_bytes"],
            }
        )
        print(f"{i}/{len(source['entries'])} {entry['date']} {entry['size_bytes']} {dest}", file=sys.stderr)

    local = {
        "name": args.name,
        "dataset": source.get("dataset", "eth_transactions"),
        "region": source.get("region", "us-east-2"),
        "target_bytes": source.get("target_bytes", 0),
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "entries": local_entries,
    }
    out = Path(args.local_manifest)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(local, indent=2) + "\n")
    print(f"wrote {out} entries={len(local_entries)} bytes={total}", file=sys.stderr)


if __name__ == "__main__":
    main()
