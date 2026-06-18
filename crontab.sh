# Sports ETL nightly jobs (cron is the orchestrator; times are local)
# Install: crontab -e
SHELL=/bin/bash
PATH=/home/gsingh/.local/bin:/usr/local/bin:/usr/bin:/bin

ETL_ROOT=/home/gsingh/sports-etl-scripts
ETL_LOG=/home/gsingh/sports-etl-scripts/logs
UV=/home/gsingh/.local/bin/uv

# statcast-update-recent (10:00) — ingest statcast + statcast_extra, then rebuild statcast marts (dbt)
0 10 * * * cd "$ETL_ROOT" && flock -n /tmp/statcast-update-recent.lock "$UV" run --extra dbt etl statcast update-recent --days 1 >> "$ETL_LOG/statcast-update-recent.log" 2>&1

# mlbam-pbp-update-recent (10:30) — schedule sync, recent PBP, then dbt season stats (inside update-recent)
30 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlbam-pbp-update-recent.lock bash -lc '"$UV" run --extra dbt etl schedule season && "$UV" run --extra dbt etl pbp update-recent --days 3' >> "$ETL_LOG/mlbam-pbp-update-recent.log" 2>&1

# mlb-transactions-update-recent (10:45)
45 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-transactions-update-recent.lock "$UV" run etl transactions update-recent --days 7 >> "$ETL_LOG/mlb-transactions-update-recent.log" 2>&1

# mlb-roster-entries-update-recent (10:50)
50 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-roster-entries-update-recent.lock "$UV" run etl roster update-recent --days 7 --max-workers 8 >> "$ETL_LOG/mlb-roster-entries-update-recent.log" 2>&1
