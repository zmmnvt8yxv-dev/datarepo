from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data_raw"
VERIFY_DIR = DATA_RAW / "verify"

TRANSACTIONS_DIR = DATA_RAW / "espn_transactions"
LINEUPS_DIR = DATA_RAW / "espn_lineups"
ESPN_INDEX = DATA_RAW / "espn_core" / "index" / "athletes_index_flat.parquet"


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def collect_transaction_ids() -> set[str]:
    ids: set[str] = set()
    if not TRANSACTIONS_DIR.exists():
        return ids
    for path in sorted(TRANSACTIONS_DIR.glob("transactions_*.json")):
        data = read_json(path)
        txns = data.get("transactions", [])
        for txn in txns:
            for item in txn.get("items", []) or []:
                player_id = item.get("playerId")
                if player_id is None:
                    continue
                ids.add(str(player_id))
    return ids


def collect_lineup_ids() -> set[str]:
    ids: set[str] = set()
    if not LINEUPS_DIR.exists():
        return ids
    for season_dir in sorted(p for p in LINEUPS_DIR.iterdir() if p.is_dir()):
        for path in sorted(season_dir.glob("week-*.json")):
            data = read_json(path)
            for entry in data.get("lineups", []) or []:
                player_id = entry.get("player_id")
                if player_id is None:
                    continue
                ids.add(str(player_id))
    return ids


def read_espn_index_ids() -> set[str]:
    if not ESPN_INDEX.exists():
        return set()
    df = pd.read_parquet(ESPN_INDEX)
    if "id" not in df.columns:
        raise SystemExit(f"ESPN index missing id column. Columns: {list(df.columns)[:25]}")
    return set(df["id"].dropna().astype(int).astype(str).tolist())


def main() -> None:
    txn_ids = collect_transaction_ids()
    lineup_ids = collect_lineup_ids()
    seen_ids = sorted(txn_ids | lineup_ids)

    index_ids = read_espn_index_ids()
    missing = sorted(set(seen_ids) - index_ids)

    report = {
        "transactions": len(txn_ids),
        "lineups": len(lineup_ids),
        "total_seen": len(seen_ids),
        "index_total": len(index_ids),
        "missing_from_index": len(missing),
    }

    write_json(VERIFY_DIR / "espn_ids_seen.json", {"report": report, "ids": seen_ids})
    pd.DataFrame({"espn_id": missing}).to_csv(VERIFY_DIR / "espn_ids_missing.csv", index=False)

    print("=== ESPN ID AUDIT ===")
    for key, value in report.items():
        print(f"{key}: {value}")
    print("Wrote:", VERIFY_DIR / "espn_ids_seen.json")
    print("Wrote:", VERIFY_DIR / "espn_ids_missing.csv")


if __name__ == "__main__":
    main()
