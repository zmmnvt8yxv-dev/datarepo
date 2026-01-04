from __future__ import annotations

import json
import os
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data_raw" / "sleeper"


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def fetch_json(url: str):
    with urllib.request.urlopen(url) as response:
        if response.status != 200:
            raise RuntimeError(f"Failed to fetch {url} (status {response.status})")
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    season = int(os.environ.get("SEASON", "2025"))
    max_round = int(os.environ.get("MAX_ROUND", "18"))
    league_id = os.environ.get("SLEEPER_LEAGUE_ID")
    if not league_id:
        season_path = DATA_DIR / f"{season}.json"
        if not season_path.exists():
            raise SystemExit("Missing SLEEPER_LEAGUE_ID and season file.")
        payload = read_json(season_path)
        league_id = payload.get("league_id") or payload.get("leagueId")
    if not league_id:
        raise SystemExit("Missing Sleeper league id.")

    all_transactions = []
    for round_num in range(1, max_round + 1):
        url = f"https://api.sleeper.app/v1/league/{league_id}/transactions/{round_num}"
        transactions = fetch_json(url)
        if isinstance(transactions, list):
            for row in transactions:
                row["week"] = row.get("week") or round_num
            all_transactions.extend(transactions)
        time.sleep(0.2)

    output = {
        "season": season,
        "league_id": league_id,
        "transactions": all_transactions,
    }
    output_path = DATA_DIR / f"transactions-{season}.json"
    write_json(output_path, output)
    print(f"Saved {len(all_transactions)} transactions to {output_path}")


if __name__ == "__main__":
    main()
