# Sports ETL — replaces Prefect scheduled deployments (times are UTC)
# Install: crontab -e
SHELL=/bin/bash
CRON_TZ=UTC
PATH=/home/gsingh/.local/bin:/usr/local/bin:/usr/bin:/bin

ETL_ROOT=/home/gsingh/sports-etl-scripts
ETL_LOG=/home/gsingh/sports-etl-scripts/logs
UV=/home/gsingh/.local/bin/uv

# statcast-update-recent (10:00 UTC) — ingest + statcast_extra
0 10 * * * cd "$ETL_ROOT" && flock -n /tmp/statcast-update-recent.lock bash -lc '"$UV" run python update_statcast.py update-recent --days 1 && "$UV" run python -c "from datetime import date, timedelta; from etl_scripts.statcast import get_database_url; from etl_scripts.statcast_extra import sync_missing_gamefeeds_for_year; today=date.today(); sync_missing_gamefeeds_for_year(today.year, start_date=today-timedelta(days=1), end_date=today, database_url=get_database_url())"' >> "$ETL_LOG/statcast-update-recent.log" 2>&1

# mlbam-pbp-update-recent (10:30 UTC) — schedule first, then recent PBP
30 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlbam-pbp-update-recent.lock bash -lc '"$UV" run python update_mlb_schedule.py && "$UV" run python update_mlbam_pbp.py update-recent --days 3' >> "$ETL_LOG/mlbam-pbp-update-recent.log" 2>&1

# mlb-transactions-update-recent (10:45 UTC)
45 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-transactions-update-recent.lock "$UV" run python update_mlb_transactions.py update-recent --days 7 >> "$ETL_LOG/mlb-transactions-update-recent.log" 2>&1

# mlb-roster-entries-update-recent (10:50 UTC)
50 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-roster-entries-update-recent.lock "$UV" run python update_mlb_roster_entries.py update-recent --days 7 --max-workers 8 >> "$ETL_LOG/mlb-roster-entries-update-recent.log" 2>&1