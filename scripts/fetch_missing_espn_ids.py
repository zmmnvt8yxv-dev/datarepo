#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MISSING = ROOT / "data_raw" / "verify" / "espn_ids_missing.csv"
DEFAULT_QUEUE = ROOT / "data_raw" / "espn_core" / "espn_id_queue_missing.csv"


def write_queue(src: Path, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with src.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "espn_id" not in (reader.fieldnames or []):
            raise SystemExit(f"Missing espn_id column in {src}")
        ids = []
        for row in reader:
            value = (row.get("espn_id") or "").strip()
            if value.isdigit():
                ids.append(value)
        rows = len(ids)

    with dest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["espn_id"])
        for value in ids:
            writer.writerow([value])
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--missing-csv", default=str(DEFAULT_MISSING))
    ap.add_argument("--queue-csv", default=str(DEFAULT_QUEUE))
    ap.add_argument("--min-delay", type=float, default=0.25)
    ap.add_argument("--max-delay", type=float, default=0.75)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    missing_path = Path(args.missing_csv)
    if not missing_path.exists():
        raise SystemExit(f"Missing: {missing_path}")
    queue_path = Path(args.queue_csv)
    count = write_queue(missing_path, queue_path)
    if count == 0:
        print("No missing ESPN IDs to fetch.")
        return

    cmd = [
        "python3",
        str(ROOT / "scripts" / "pull_espn_core_by_id.py"),
        "--id-csv",
        str(queue_path),
        "--min-delay",
        str(args.min_delay),
        "--max-delay",
        str(args.max_delay),
        "--timeout",
        str(args.timeout),
    ]
    if args.resume:
        cmd.append("--resume")

    print("Fetching", count, "missing ESPN IDs...")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
