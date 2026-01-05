#!/usr/bin/env python3
import argparse, csv, json, os, time, random
from pathlib import Path
import requests
from tqdm import tqdm

CORE = "https://sports.core.api.espn.com/v3/sports/football/nfl/athletes/"

def read_ids(csv_path: Path):
    ids = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if "espn_id" not in (r.fieldnames or []):
            raise SystemExit(f"CSV missing espn_id column: {csv_path}")
        for row in r:
            v = (row.get("espn_id") or "").strip()
            if v.isdigit():
                ids.append(int(v))
    return ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id-csv", default="data_raw/espn_core/espn_id_queue_next_1m.csv")
    ap.add_argument("--outdir", default="data_raw/espn_core/athletes_by_id")
    ap.add_argument("--log", default="data_raw/espn_core/pull_by_id_log.csv")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-delay", type=float, default=0.25)
    ap.add_argument("--max-delay", type=float, default=0.75)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    id_csv = Path(args.id_csv)
    outdir = Path(args.outdir)
    log_path = Path(args.log)
    outdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    ids = read_ids(id_csv)
    if args.start < 0 or args.start >= len(ids):
        raise SystemExit(f"--start out of range. ids={len(ids)} start={args.start}")

    if args.limit and args.limit > 0:
        ids = ids[args.start: args.start + args.limit]
    else:
        ids = ids[args.start:]

    done = set()
    if args.resume and log_path.exists():
        with log_path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                v = (row.get("espn_id") or "").strip()
                st = (row.get("status") or "").strip().lower()
                if v.isdigit() and st in ("ok", "404"):
                    done.add(int(v))

    file_exists_skip = 0

    is_new_log = not log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if is_new_log:
            w.writerow(["espn_id","status","http_status","bytes","path","error"])

        sess = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }

        for espn_id in tqdm(ids, desc="Fetch ESPN core athletes by id"):
            if espn_id in done:
                continue

            out_path = outdir / f"{espn_id}.json"
            if out_path.exists() and out_path.stat().st_size > 50:
                file_exists_skip += 1
                w.writerow([espn_id, "skip_exists", "", out_path.stat().st_size, str(out_path), ""])
                f.flush()
                continue

            url = f"{CORE}{espn_id}"
            try:
                resp = sess.get(url, headers=headers, timeout=args.timeout)
                http_status = resp.status_code
                body = resp.text or ""
                if http_status == 200 and body.strip():
                    payload = {
                        "meta": {
                            "source": url,
                            "http_status": http_status,
                            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        },
                        "data": None,
                        "raw": None,
                    }
                    try:
                        payload["data"] = resp.json()
                    except Exception:
                        payload["raw"] = body

                    tmp = out_path.with_suffix(".json.tmp")
                    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    os.replace(tmp, out_path)

                    w.writerow([espn_id, "ok", http_status, out_path.stat().st_size, str(out_path), ""])
                    f.flush()
                elif http_status == 404:
                    w.writerow([espn_id, "404", http_status, len(body.encode("utf-8")), "", ""])
                    f.flush()
                else:
                    w.writerow([espn_id, "http_error", http_status, len(body.encode("utf-8")), "", body[:200].replace("\n"," ")])
                    f.flush()

            except Exception as e:
                w.writerow([espn_id, "exception", "", "", "", repr(e)])
                f.flush()

            time.sleep(random.uniform(args.min_delay, args.max_delay))

    print("Done. Wrote log:", log_path)
    print("Output dir:", outdir)
    print("skip_exists rows logged:", file_exists_skip)

if __name__ == "__main__":
    main()
