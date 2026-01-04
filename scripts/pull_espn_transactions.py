from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data_raw" / "espn_transactions"
DEBUG_PATH = Path("/tmp/espn_tx_debug.json")
HOST = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def normalize_cookie(raw_cookie: str) -> str:
    raw = raw_cookie.strip()
    if raw.lower().startswith("cookie:"):
        raw = raw.split(":", 1)[1].strip()
    raw = " ".join(raw.split())
    values: dict[str, str] = {}
    for part in raw.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key in {"espn_s2", "SWID"}:
            values[key] = value.strip()
    ordered = []
    if "espn_s2" in values:
        ordered.append(f"espn_s2={values['espn_s2']}")
    if "SWID" in values:
        ordered.append(f"SWID={values['SWID']}")
    return "; ".join(ordered)


def build_headers(league_id: str, cookie: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.1 Safari/605.1.15"
        ),
        "Referer": f"https://fantasy.espn.com/football/league?leagueId={league_id}",
        "Origin": "https://fantasy.espn.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def report_bad_response(response: requests.Response, body: str) -> None:
    status = response.status_code
    final_url = response.url
    location = response.headers.get("Location")
    content_type = response.headers.get("Content-Type", "")
    preview = body[:200]
    print(f"Status: {status}")
    print(f"Final URL: {final_url}")
    print(f"Location: {location}")
    print(f"Content-Type: {content_type}")
    print(f"Body preview: {preview}")
    raise SystemExit(1)


def fetch_json(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[Any, requests.Response, str]:
    request_headers = dict(headers)
    if extra_headers:
        request_headers.update(extra_headers)
    response = session.get(url, headers=request_headers, params=params, allow_redirects=False, timeout=30)
    body = response.text
    if response.status_code in {301, 302}:
        report_bad_response(response, body)
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        report_bad_response(response, body)
    try:
        payload = response.json()
    except ValueError:
        report_bad_response(response, body)
    return payload, response, body


def unwrap_payload(payload: Any) -> Any:
    if isinstance(payload, list) and payload:
        return payload[0]
    return payload


def write_debug(payload: Any, url: str, params: dict[str, Any] | None, response: requests.Response, body: str) -> None:
    debug_payload = {
        "url": url,
        "params": params,
        "status": response.status_code,
        "headers": dict(response.headers),
        "body": body[:2000],
        "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None,
    }
    write_json(DEBUG_PATH, debug_payload)


def build_base_urls(season: int, league_id: str) -> list[tuple[str, dict[str, Any]]]:
    season_base = f"{HOST}/seasons/{season}/segments/0/leagues/{league_id}"
    history_base = f"{HOST}/leagueHistory/{league_id}"
    return [
        (season_base, {}),
        (history_base, {"seasonId": season}),
    ]


def fetch_settings(
    session: requests.Session,
    season: int,
    league_id: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    for base_url, base_params in build_base_urls(season, league_id):
        params = dict(base_params)
        params["view"] = "mSettings"
        payload, response, body = fetch_json(session, base_url, headers, params=params)
        payload = unwrap_payload(payload)
        if isinstance(payload, dict) and payload.get("status"):
            return payload
        write_debug(payload, base_url, params, response, body)
    return {}


def fetch_team_data(
    session: requests.Session,
    season: int,
    league_id: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    for base_url, base_params in build_base_urls(season, league_id):
        params = dict(base_params)
        params["view"] = "mTeam"
        payload, response, body = fetch_json(session, base_url, headers, params=params)
        payload = unwrap_payload(payload)
        if isinstance(payload, dict) and payload.get("teams"):
            return payload
        write_debug(payload, base_url, params, response, body)
    return {}


def fetch_transactions_for_period(
    session: requests.Session,
    season: int,
    league_id: str,
    headers: dict[str, str],
    scoring_period_id: int,
) -> tuple[list[dict[str, Any]], str | None]:
    views = ["mTransactions2", "mTransactions"]
    filters = [None, {"transactions": {"limit": 2000, "offset": 0}}]
    last_payload: Any = None
    last_response: requests.Response | None = None
    last_body = ""
    last_url = ""
    last_params: dict[str, Any] | None = None

    for base_url, base_params in build_base_urls(season, league_id):
        for view in views:
            for filter_payload in filters:
                params = dict(base_params)
                params["view"] = view
                params["scoringPeriodId"] = scoring_period_id
                extra_headers = None
                if filter_payload:
                    extra_headers = {"X-Fantasy-Filter": json.dumps(filter_payload)}
                payload, response, body = fetch_json(
                    session,
                    base_url,
                    headers,
                    params=params,
                    extra_headers=extra_headers,
                )
                payload = unwrap_payload(payload)
                transactions = []
                if isinstance(payload, dict):
                    transactions = payload.get("transactions") or []
                if isinstance(transactions, list) and transactions:
                    return transactions, view
                last_payload = payload
                last_response = response
                last_body = body
                last_url = base_url
                last_params = params

    if last_response is not None:
        write_debug(last_payload, last_url, last_params, last_response, last_body)
    return [], None


def pull_season(
    session: requests.Session,
    season: int,
    league_id: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    settings = fetch_settings(session, season, league_id, headers)
    status = settings.get("status", {}) if isinstance(settings, dict) else {}
    final_scoring = status.get("finalScoringPeriod") or status.get("currentMatchupPeriod")
    try:
        max_scoring = int(final_scoring)
    except (TypeError, ValueError):
        max_scoring = 18
    max_scoring = max(1, min(max_scoring, 18))

    transactions: list[dict[str, Any]] = []
    seen_ids: set[Any] = set()
    for scoring_id in range(1, max_scoring + 1):
        items, view_used = fetch_transactions_for_period(
            session,
            season,
            league_id,
            headers,
            scoring_id,
        )
        for item in items:
            if not isinstance(item, dict):
                continue
            item.setdefault("season", season)
            item.setdefault("scoringPeriodId", scoring_id)
            if view_used:
                item.setdefault("__view", view_used)
            txn_id = item.get("id")
            if txn_id is not None and txn_id in seen_ids:
                continue
            if txn_id is not None:
                seen_ids.add(txn_id)
            transactions.append(item)

    team_payload = fetch_team_data(session, season, league_id, headers)
    teams = team_payload.get("teams", []) if isinstance(team_payload, dict) else []
    members = team_payload.get("members", []) if isinstance(team_payload, dict) else []

    return {
        "season": season,
        "league_id": league_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "transactions": transactions,
        "teams": teams,
        "members": members,
    }


def main() -> None:
    season = int(os.environ.get("SEASON", "2025"))
    start_season = int(os.environ.get("START_SEASON", season))
    end_season = int(os.environ.get("END_SEASON", season))
    league_id = os.environ.get("ESPN_LEAGUE_ID")
    if not league_id:
        raise SystemExit("Missing ESPN_LEAGUE_ID environment variable.")

    raw_cookie = os.environ.get("ESPN_COOKIE")
    cookie_file = os.environ.get("ESPN_COOKIE_FILE")
    if not raw_cookie and cookie_file:
        raw_cookie = Path(cookie_file).read_text(encoding="utf-8").strip()
    cookie = normalize_cookie(raw_cookie or "")
    if not cookie:
        raise SystemExit("Missing ESPN cookie values. Provide ESPN_COOKIE or ESPN_COOKIE_FILE.")

    headers = build_headers(league_id, cookie)
    session = requests.Session()

    combined: list[dict[str, Any]] = []
    by_season: dict[str, int] = {}

    for year in range(start_season, end_season + 1):
        payload = pull_season(session, year, league_id, headers)
        output_path = DATA_DIR / f"transactions_{year}.json"
        write_json(output_path, payload)
        count = len(payload.get("transactions", []))
        by_season[str(year)] = count
        combined.extend(payload.get("transactions", []))
        print(f"Saved {count} ESPN transactions to {output_path}")

    combined_output = {
        "league_id": league_id,
        "start_season": start_season,
        "end_season": end_season,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "transactions": combined,
        "by_season": by_season,
    }
    combined_path = DATA_DIR / f"transactions_{start_season}_{end_season}.json"
    write_json(combined_path, combined_output)
    print(f"Saved combined ESPN transactions to {combined_path}")


if __name__ == "__main__":
    main()
