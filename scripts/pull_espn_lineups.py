#!/usr/bin/env python3
import json
import os
from pathlib import Path
from urllib.parse import urlencode

import requests


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data_raw" / "espn_lineups"


def normalize_cookie(raw_cookie: str) -> str:
  raw = raw_cookie.strip()
  if raw.lower().startswith("cookie:"):
    raw = raw.split(":", 1)[1].strip()
  raw = " ".join(raw.split())
  values = {}
  for part in raw.replace(";", " ").split():
    if "=" in part:
      key, value = part.split("=", 1)
    elif ":" in part:
      key, value = part.split(":", 1)
    else:
      continue
    key = key.strip()
    if key in {"espn_s2", "SWID"}:
      values[key] = value.strip()
  ordered = []
  if "espn_s2" in values:
    ordered.append(f"espn_s2={values['espn_s2']}")
  if "SWID" in values:
    ordered.append(f"SWID={values['SWID']}")
  return "; ".join(ordered)


def load_cookie_header(path: Path) -> str:
  raw = path.read_text(encoding="utf-8").strip().replace("\n", "").replace("\r", "")
  if not raw:
    raise RuntimeError("Cookie file is empty.")
  cookie = normalize_cookie(raw)
  if not cookie:
    raise RuntimeError("Cookie file missing espn_s2/SWID.")
  return cookie


def fetch_json(url, headers):
  response = requests.get(url, headers=headers, timeout=30)
  content_type = response.headers.get("content-type", "")
  if response.status_code in (301, 302) or "application/json" not in content_type:
    preview = response.text[:200]
    raise RuntimeError(
      f"Non-JSON response from ESPN. status={response.status_code} url={response.url} "
      f"location={response.headers.get('location')} preview={preview}"
    )
  return response.json()


def build_team_name(team, member_by_id):
  name = team.get("name")
  if not name:
    location = team.get("location") or ""
    nickname = team.get("nickname") or ""
    name = f"{location} {nickname}".strip()
  if not name:
    owners = team.get("owners") or []
    if owners:
      owner = member_by_id.get(owners[0], {})
      name = owner.get("displayName") or owner.get("firstName")
  return name or f"Team {team.get('id')}"


def parse_lineups(payload, week):
  teams = payload.get("teams", [])
  members = payload.get("members", [])
  member_by_id = {member.get("id"): member for member in members if member.get("id")}
  team_name_by_id = {}
  for team in teams:
    team_id = team.get("id")
    if team_id is None:
      continue
    team_name_by_id[str(team_id)] = build_team_name(team, member_by_id)

  lineups = []
  for team in teams:
    team_id = team.get("id")
    roster = team.get("roster") or {}
    entries = roster.get("entries") or []
    for entry in entries:
      player_id = entry.get("playerId")
      if player_id is None:
        continue
      slot_id = entry.get("lineupSlotId")
      started = slot_id not in (20, 21)  # bench or IR
      points = entry.get("appliedStatTotal")
      lineups.append(
        {
          "week": week,
          "team": team_name_by_id.get(str(team_id), f"Team {team_id}"),
          "player_id": str(player_id),
          "started": bool(started),
          "points": points,
        }
      )
  return lineups


def main():
  league_id = os.getenv("ESPN_LEAGUE_ID")
  if not league_id:
    raise RuntimeError("Missing ESPN_LEAGUE_ID.")
  start_season = int(os.getenv("START_SEASON", "2015"))
  end_season = int(os.getenv("END_SEASON", "2024"))
  cookie_path = Path(os.getenv("ESPN_COOKIE_FILE", "")).expanduser()
  if not cookie_path.exists():
    raise RuntimeError(f"Cookie file not found: {cookie_path}")

  cookie_header = load_cookie_header(cookie_path)
  headers = {
    "Cookie": cookie_header,
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Referer": f"https://fantasy.espn.com/football/league?leagueId={league_id}",
    "Origin": "https://fantasy.espn.com",
  }

  for season in range(start_season, end_season + 1):
    for week in range(1, 19):
      params = [
        ("view", "mMatchup"),
        ("view", "mMatchupScore"),
        ("view", "mTeam"),
        ("view", "mRoster"),
        ("scoringPeriodId", str(week)),
      ]
      url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}"
        f"/segments/0/leagues/{league_id}?{urlencode(params)}"
      )
      payload = fetch_json(url, headers)
      if not payload.get("teams") or not payload.get("members"):
        raise RuntimeError(f"Missing team/member data for season {season} week {week}.")
      lineups = parse_lineups(payload, week)
      min_nonempty = int(os.environ.get("MIN_NONEMPTY_SEASON", "2018"))
      if season >= min_nonempty and len(lineups) == 0:
        raise RuntimeError(f"No lineups returned for season {season} week {week}.")
      out_dir = OUTPUT_DIR / str(season)
      out_dir.mkdir(parents=True, exist_ok=True)
      out_path = out_dir / f"week-{week}.json"
      out_path.write_text(
        json.dumps({"season": season, "week": week, "lineups": lineups}, indent=2),
        encoding="utf-8",
      )
      print(f"Saved {len(lineups)} lineups to {out_path}")


if __name__ == "__main__":
  main()
