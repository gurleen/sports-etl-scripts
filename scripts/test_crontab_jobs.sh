#!/usr/bin/env bash
# Run the same commands as crontab.sh (repo root) for manual verification.
#
# Usage:
#   ./scripts/test_crontab_jobs.sh              # all jobs, in cron order
#   ./scripts/test_crontab_jobs.sh statcast     # one job
#   ./scripts/test_crontab_jobs.sh pbp
#   ./scripts/test_crontab_jobs.sh transactions
#   ./scripts/test_crontab_jobs.sh roster
#
# Options (env):
#   ETL_ROOT, ETL_LOG, UV   — same defaults as crontab.sh
#   USE_FLOCK=1             — use the same flock -n locks as cron (skips if locked)
#   LOG_ONLY=1                — append to log files only (no stdout), like cron

set -euo pipefail

ETL_ROOT="${ETL_ROOT:-/home/gsingh/sports-etl-scripts}"
ETL_LOG="${ETL_LOG:-$ETL_ROOT/logs}"
UV="${UV:-/home/gsingh/.local/bin/uv}"
export ETL_ROOT ETL_LOG UV
export PATH="/home/gsingh/.local/bin:/usr/local/bin:/usr/bin:/bin"

mkdir -p "$ETL_LOG"
cd "$ETL_ROOT"

maybe_flock() {
  local lock_path=$1
  shift
  if [[ "${USE_FLOCK:-0}" == "1" ]]; then
    flock -n "$lock_path" "$@"
  else
    "$@"
  fi
}

run_logged() {
  local log_file=$1
  shift
  if [[ "${LOG_ONLY:-0}" == "1" ]]; then
    "$@" >>"$log_file" 2>&1
  else
    echo "==> Logging to $log_file"
    "$@" 2>&1 | tee -a "$log_file"
  fi
}

job_statcast() {
  echo "=== statcast-update-recent (cron: 0 10 * * * UTC) ==="
  run_logged "$ETL_LOG/statcast-update-recent.log" \
    maybe_flock /tmp/statcast-update-recent.lock \
    bash -lc \
    '"$UV" run python update_statcast.py update-recent --days 1 && "$UV" run python -c "from datetime import date, timedelta; from etl_scripts.statcast import get_database_url; from etl_scripts.statcast_extra import sync_missing_gamefeeds_for_year; today=date.today(); sync_missing_gamefeeds_for_year(today.year, start_date=today-timedelta(days=1), end_date=today, database_url=get_database_url())"'
}

job_pbp() {
  echo "=== mlbam-pbp-update-recent (cron: 30 10 * * * UTC) ==="
  run_logged "$ETL_LOG/mlbam-pbp-update-recent.log" \
    maybe_flock /tmp/mlbam-pbp-update-recent.lock \
    bash -lc \
    '"$UV" run python update_mlb_schedule.py season && "$UV" run python update_mlbam_pbp.py update-recent --days 3'
}

job_transactions() {
  echo "=== mlb-transactions-update-recent (cron: 45 10 * * * UTC) ==="
  run_logged "$ETL_LOG/mlb-transactions-update-recent.log" \
    maybe_flock /tmp/mlb-transactions-update-recent.lock \
    "$UV" run python update_mlb_transactions.py update-recent --days 7
}

job_roster() {
  echo "=== mlb-roster-entries-update-recent (cron: 50 10 * * * UTC) ==="
  run_logged "$ETL_LOG/mlb-roster-entries-update-recent.log" \
    maybe_flock /tmp/mlb-roster-entries-update-recent.lock \
    "$UV" run python update_mlb_roster_entries.py update-recent --days 7 --max-workers 8
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [all|statcast|pbp|transactions|roster]

Runs the same commands defined in crontab.sh. Output is tee'd to the cron log
files under \$ETL_LOG (default: $ETL_LOG).

Environment:
  ETL_ROOT=$ETL_ROOT
  ETL_LOG=$ETL_LOG
  UV=$UV
  USE_FLOCK=1   also use cron's flock -n (job no-ops if lock is held)
  LOG_ONLY=1    append to log files only, no terminal output
EOF
}

target="${1:-all}"

case "$target" in
  all)
    job_statcast
    job_pbp
    job_transactions
    job_roster
    ;;
  statcast)
    job_statcast
    ;;
  pbp)
    job_pbp
    ;;
  transactions)
    job_transactions
    ;;
  roster)
    job_roster
    ;;
  -h | --help | help)
    usage
    ;;
  *)
    echo "Unknown job: $target" >&2
    usage >&2
    exit 1
    ;;
esac

echo "Done."
