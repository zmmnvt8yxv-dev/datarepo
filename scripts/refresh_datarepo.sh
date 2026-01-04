#!/bin/zsh
set -euo pipefail

REPO_DIR="/Users/connermalley/datarepo"
COOKIE_FILE="${HOME}/.config/tatnall_espn_cookie.txt"

if [[ ! -f "${COOKIE_FILE}" ]]; then
  echo "Missing ESPN cookie file: ${COOKIE_FILE}" >&2
  exit 1
fi

cd "${REPO_DIR}"

git pull --rebase
git lfs install --local

ESPN_LEAGUE_ID=1773893 \
START_SEASON=2015 \
END_SEASON=2024 \
MIN_NONEMPTY_SEASON=2020 \
ESPN_COOKIE_FILE="${COOKIE_FILE}" \
ESPN_COOKIE_PASSTHROUGH=1 \
python3 scripts/pull_espn_transactions.py

ESPN_LEAGUE_ID=1773893 \
START_SEASON=2015 \
END_SEASON=2024 \
ESPN_COOKIE_FILE="${COOKIE_FILE}" \
ESPN_COOKIE_PASSTHROUGH=1 \
python3 scripts/pull_espn_lineups.py

SLEEPER_LEAGUE_ID=1262418074540195841 \
SEASON=2025 \
MAX_ROUND=18 \
python3 scripts/pull_sleeper_transactions.py

if git diff --quiet --exit-code; then
  echo "No changes to commit."
  exit 0
fi

git add data_raw
git commit -m "Refresh data_raw (ESPN transactions/lineups + Sleeper transactions)"
git push origin main
