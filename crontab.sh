# Sports ETL scheduling has moved to GitHub Actions (Hugging Face Parquet output).
#
# The nightly cron jobs below previously wrote into the Postgres warehouse on a
# VPS. Datasets are now published as Parquet to the Hugging Face dataset repo
# (gurleen/baseball) by the scheduled workflows in .github/workflows/:
#
#   hf-pbp.yml                  -> mlb_schedule.parquet, retrosheet_plays.parquet,
#                                  baserunning_events.parquet   (14:00 UTC)
#   hf-transactions.yml         -> mlb_transactions.parquet     (14:30 UTC)
#   hf-statcast-extra.yml       -> statcast_<year>.parquet      (15:00 UTC)
#   hf-retrosheet-historical.yml-> retrosheet_plays.parquet ('retrosheet' source; manual)
#
# See docs/huggingface.md. The workflows need an HF_TOKEN repo secret.
#
# NOT migrated to Hugging Face (no equivalent job): the original `statcast`
# pitch table, `mlb_roster_entries`, and `mlb_contracts`. If you still need these
# in a database, run the corresponding `etl` commands manually or restore a cron
# entry for them; they are intentionally out of scope for the HF publishing jobs.
#
# ---------------------------------------------------------------------------
# Retired Postgres cron jobs (kept for reference only — no longer installed):
#
# 0  10 * * * cd "$ETL_ROOT" && flock -n /tmp/statcast-update-recent.lock        "$UV" run --extra dbt etl statcast update-recent --days 1
# 30 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlbam-pbp-update-recent.lock       bash -lc '"$UV" run --extra dbt etl schedule season && "$UV" run --extra dbt etl pbp update-recent --days 3'
# 45 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-transactions-update-recent.lock "$UV" run etl transactions update-recent --days 7
# 50 10 * * * cd "$ETL_ROOT" && flock -n /tmp/mlb-roster-entries-update-recent.lock "$UV" run etl roster update-recent --days 7 --max-workers 8
