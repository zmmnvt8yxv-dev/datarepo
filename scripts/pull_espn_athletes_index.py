#!/usr/bin/env python3
import argparse, json, os, time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
from tqdm import tqdm

BASE_URL = "https://sports.core.api.espn.com/v3/sports/football/nfl/athletes"

def fetch_json(session: requests.Session, url: str, params: Dict[str, Any], retries: int = 6, backoff: float = 1.4):
    last_exc = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=45)
            if r.status_code >= 500:
                raise RuntimeError(f"{r.status_code} server error")
            r.raise_for_status()
            return r.json(), r.status_code
        except Exception as e:
            last_exc = e
            time.sleep(backoff ** i)
    raise RuntimeError(f"Failed after {retries} retries: {url} params={params} err={last_exc}")

def safe_write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200000)
    ap.add_argument("--outdir", type=str, default="data_raw/espn_core/index")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--max-pages", type=int, default=0)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    pages_dir = outdir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "TatnallLegacy/1.0 (contact: local script)",
        "Accept": "application/json",
    })

    all_items: List[Dict[str, Any]] = []
    page_index = 1
    page_count: Optional[int] = None

    param_styles = [
        ("page", "limit"),
        ("pageIndex", "pageSize"),
    ]

    chosen_style = None

    for style in param_styles:
        p_page, p_size = style
        params = {p_page: page_index, p_size: args.limit}
        try:
            data, status = fetch_json(session, BASE_URL, params=params)
            if isinstance(data, dict) and "items" in data and "pageIndex" in data:
                chosen_style = style
                safe_write_json(pages_dir / f"athletes_index_{page_index:04d}.json", data)
                items = data.get("items") or []
                all_items.extend(items if isinstance(items, list) else [])
                page_count = data.get("pageCount")
                page_index = int(data.get("pageIndex", page_index)) + 1
                break
        except Exception:
            continue

    if chosen_style is None:
        data, status = fetch_json(session, BASE_URL, params={"limit": args.limit})
        safe_write_json(pages_dir / "athletes_index_0001.json", data)
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            raise SystemExit("No items[] found. Check data_raw/espn_core/index/pages.")
        all_items = items
        page_count = 1
        chosen_style = ("(none)", "(none)")

    if page_count and page_count > 1:
        p_page, p_size = chosen_style
        max_pages = args.max_pages if args.max_pages > 0 else page_count

        for _ in tqdm(range(2, max_pages + 1), desc="Fetching index pages"):
            params = {p_page: page_index, p_size: args.limit}
            page_path = pages_dir / f"athletes_index_{page_index:04d}.json"
            if args.resume and page_path.exists():
                data = json.loads(page_path.read_text(encoding="utf-8"))
            else:
                data, status = fetch_json(session, BASE_URL, params=params)
                safe_write_json(page_path, data)

            items = data.get("items") or []
            if isinstance(items, list) and items:
                all_items.extend(items)
            else:
                break

            pi = data.get("pageIndex")
            if pi is not None:
                page_index = int(pi) + 1
            else:
                page_index += 1

    df = pd.json_normalize(all_items, sep="__")

    outdir.mkdir(parents=True, exist_ok=True)
    flat_parquet = outdir / "athletes_index_flat.parquet"
    flat_csv = outdir / "athletes_index_flat.csv"
    cols_csv = outdir / "athletes_index_columns.csv"
    active_csv = outdir / "athletes_active_only.csv"

    df.to_parquet(flat_parquet, index=False)
    df.to_csv(flat_csv, index=False)
    pd.DataFrame({"column": df.columns}).to_csv(cols_csv, index=False)

    if "active" in df.columns:
        df[df["active"] == True].to_csv(active_csv, index=False)
    else:
        df.head(0).to_csv(active_csv, index=False)

    print("\nDONE")
    print("Pages dir:", pages_dir)
    print("Rows:", len(df), "Cols:", len(df.columns))
    print("Wrote:", flat_parquet)
    print("Wrote:", flat_csv)
    print("Wrote:", cols_csv)
    print("Wrote:", active_csv)

if __name__ == "__main__":
    main()
