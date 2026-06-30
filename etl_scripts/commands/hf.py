"""``etl hf`` — publish ETL datasets to a Hugging Face dataset repo as Parquet.

These commands replace the database-writing cron jobs: instead of upserting into
Postgres, each one stages data in a throwaway local DuckDB
(``ETL_DB_BACKEND=duckdb``), seeds the relevant tables from the Parquet already on
the Hub (so history accumulates), runs the existing ingest, then exports each
table back to Parquet and uploads it to the dataset repo (default
``gurleen/baseball``; override with ``HF_DATASET_REPO``). Uploads need a
write-scoped ``HF_TOKEN``.

Examples
--------
    # nightly-style jobs (what the GitHub Actions workflows run)
    uv run --extra hf etl hf pbp --days 3
    uv run --extra hf etl hf transactions --days 7
    uv run --extra hf etl hf statcast-extra --days 3

    # one-off historical Retrosheet build (replaces the 'retrosheet' source rows)
    uv run --extra hf etl hf retrosheet --full

    # local dry run: export Parquet but don't upload
    uv run --extra hf etl hf transactions --days 1 --no-upload
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import typer
from loguru import logger

app = typer.Typer(help="Publish ETL datasets to Hugging Face as Parquet.")

_WORK_DIR: Path | None = None


def _work_dir() -> Path:
    """A scratch dir for downloaded/exported Parquet (``HF_WORK_DIR`` or a temp dir)."""
    global _WORK_DIR
    if _WORK_DIR is None:
        base = os.getenv("HF_WORK_DIR")
        _WORK_DIR = Path(base) if base else Path(tempfile.mkdtemp(prefix="etl_hf_"))
        _WORK_DIR.mkdir(parents=True, exist_ok=True)
    return _WORK_DIR


def _force_duckdb_staging() -> None:
    """Pin the ingest backend to a local DuckDB file so jobs never touch Postgres."""
    os.environ["ETL_DB_BACKEND"] = "duckdb"
    os.environ.setdefault("DUCKDB_PATH", str(_work_dir() / "warehouse.duckdb"))


def _publish_card(no_upload: bool) -> None:
    """Ensure the dataset card (per-table subsets) exists after an upload run."""
    if no_upload:
        return
    from etl_scripts import hf_sync

    hf_sync.ensure_dataset_card()


@app.command()
def card(
    force: bool = typer.Option(True, "--force/--no-force", help="Overwrite an existing README.md card."),
):
    """Publish/refresh the dataset card so each Parquet table is its own Data Studio subset."""
    from etl_scripts import hf_sync

    changed = hf_sync.ensure_dataset_card(force=force)
    logger.info("Dataset card {}", "published" if changed else "already present (use --force to overwrite)")


@app.command()
def pbp(
    days: int = typer.Option(3, help="Re-fetch Final games from the last N days (recent mode)."),
    year: int | None = typer.Option(None, help="Season (defaults to current year)."),
    backfill: bool = typer.Option(False, "--backfill", help="Load every not-yet-loaded Final game for the whole season (ignores --days)."),
    workers: int = typer.Option(8, help="Concurrent fetch workers (forced to 1 on DuckDB)."),
    no_upload: bool = typer.Option(False, "--no-upload", help="Export Parquet locally but skip the Hub upload."),
):
    """Sync schedule + play-by-play, publish mlb_schedule / retrosheet_plays / baserunning_events.

    Default re-fetches the last ``--days`` of Final games. With ``--backfill`` it loads
    every Final regular-season game for the season that isn't already present.
    """
    _force_duckdb_staging()
    work = _work_dir()
    from etl_scripts import db, hf_sync
    from etl_scripts.mlb_schedule import (
        MLB_SCHEDULE_TABLE,
        _mlb_schedule_ddl,
        sync_mlb_schedule_for_year,
    )
    from etl_scripts.mlbam_pbp import BASERUNNING_TABLE, baserunning_ddl, load_season
    from etl_scripts.mlbam_pbp import TABLE_NAME as PLAYS_TABLE
    from etl_scripts.retrosheet import create_table_ddl as plays_ddl

    y = year or date.today().year
    today = date.today()
    start = today - timedelta(days=days)

    con = db.connect()
    try:
        hf_sync.seed_table_from_hf(con, MLB_SCHEDULE_TABLE, _mlb_schedule_ddl(), "mlb_schedule.parquet", work)
        hf_sync.seed_table_from_hf(con, PLAYS_TABLE, plays_ddl(PLAYS_TABLE), "retrosheet_plays.parquet", work)
        hf_sync.seed_table_from_hf(con, BASERUNNING_TABLE, baserunning_ddl(), "baserunning_events.parquet", work)
        con.commit()
    finally:
        con.close()

    logger.info("Syncing schedule for {}", y)
    sync_mlb_schedule_for_year(y)
    if backfill:
        logger.info("Backfilling all not-yet-loaded Final games for {}", y)
        summary = load_season(y, only_missing=True, max_workers=workers)
    else:
        logger.info("Loading recent PBP {}..{}", start, today)
        summary = load_season(
            y, only_missing=False, start_date=start, end_date=today, max_workers=workers
        )
    logger.info("PBP load complete: {}", {k: v for k, v in summary.items() if k != "failures"})

    con = db.connect()
    try:
        hf_sync.publish_table(con, MLB_SCHEDULE_TABLE, "mlb_schedule.parquet", work, upload=not no_upload)
        hf_sync.publish_table(con, PLAYS_TABLE, "retrosheet_plays.parquet", work, upload=not no_upload)
        hf_sync.publish_table(con, BASERUNNING_TABLE, "baserunning_events.parquet", work, upload=not no_upload)
    finally:
        con.close()
    _publish_card(no_upload)


@app.command()
def transactions(
    days: int = typer.Option(7, help="Re-fetch the trailing N days of transactions."),
    no_upload: bool = typer.Option(False, "--no-upload", help="Export Parquet locally but skip the Hub upload."),
):
    """Sync recent transactions and publish mlb_transactions.parquet."""
    _force_duckdb_staging()
    work = _work_dir()
    from etl_scripts import db, hf_sync
    from etl_scripts.mlb_transactions import (
        MLB_TRANSACTIONS_TABLE,
        _mlb_transactions_ddl,
        update_recent_transactions,
    )

    con = db.connect()
    try:
        hf_sync.seed_table_from_hf(
            con, MLB_TRANSACTIONS_TABLE, _mlb_transactions_ddl(), "mlb_transactions.parquet", work
        )
        con.commit()
    finally:
        con.close()

    res = update_recent_transactions(days=days)
    logger.info("Transactions sync complete: {}", res)

    con = db.connect()
    try:
        hf_sync.publish_table(
            con, MLB_TRANSACTIONS_TABLE, "mlb_transactions.parquet", work, upload=not no_upload
        )
    finally:
        con.close()
    _publish_card(no_upload)


@app.command("statcast-extra")
def statcast_extra(
    days: int = typer.Option(3, help="Fetch gamefeeds for Final games from the last N days (recent mode)."),
    year: int | None = typer.Option(None, help="Season (defaults to current year)."),
    backfill: bool = typer.Option(False, "--backfill", help="Fetch every not-yet-present Final game for the whole season (ignores --days)."),
    pause_sec: float = typer.Option(0.0, help="Pause between gamefeed fetches."),
    fetch_attempts: int = typer.Option(4, help="Retries per gamefeed (Savant /gf can truncate large bodies)."),
    no_upload: bool = typer.Option(False, "--no-upload", help="Write Parquet locally but skip the Hub upload."),
):
    """Fetch missing Savant gamefeeds and publish statcast_<year>.parquet.

    Default fetches the last ``--days`` of Final games; ``--backfill`` fetches every
    Final regular-season game for the season not already present. Game discovery uses
    the schedule snapshot (mlb_schedule.parquet) on the Hub — run ``etl hf pbp`` first.
    """
    work = _work_dir()
    from etl_scripts import hf_sync

    y = year or date.today().year
    res = hf_sync.collect_statcast_extra(
        year=y, days=(None if backfill else days), work_dir=work, upload=not no_upload,
        pause_sec=pause_sec, fetch_attempts=fetch_attempts,
    )
    logger.info("statcast-extra publish complete: {}", res)
    _publish_card(no_upload)


@app.command()
def retrosheet(
    full: bool = typer.Option(False, "--full", help="Build every season (single plays.zip) instead of a year range."),
    start_year: int | None = typer.Option(None, help="First season (range mode; defaults to earliest available)."),
    end_year: int | None = typer.Option(None, help="Last season (range mode; defaults to current year)."),
    no_upload: bool = typer.Option(False, "--no-upload", help="Export Parquet locally but skip the Hub upload."),
):
    """One-off historical Retrosheet build; replaces the 'retrosheet' source rows in retrosheet_plays.parquet."""
    _force_duckdb_staging()
    work = _work_dir()
    import duckdb

    from etl_scripts import db, hf_sync
    from etl_scripts import retrosheet as rs

    path = db.get_duckdb_path()

    # Retrosheet's DuckDB loader connects to the file directly (default `main`
    # schema), so seed/export here use a plain connection to match it.
    con = duckdb.connect(path)
    try:
        hf_sync.seed_table_from_hf(
            con, rs.TABLE_NAME, rs.create_table_ddl(rs.TABLE_NAME), "retrosheet_plays.parquet", work
        )
        con.commit()
    finally:
        con.close()

    rs.ensure_table(path, backend="duckdb")
    years = None if full else rs.season_range(start_year, end_year)
    lf = rs.build_dataset(years=years, use_full_bundle=full)
    build_parquet = work / "retrosheet_build.parquet"
    rs.write_dataset(lf, build_parquet)
    rs.load_parquet_to_db(
        build_parquet, path, backend="duckdb", replace_source=rs.SOURCE_LABEL
    )

    con = duckdb.connect(path)
    try:
        hf_sync.publish_table(con, rs.TABLE_NAME, "retrosheet_plays.parquet", work, upload=not no_upload)
    finally:
        con.close()
    _publish_card(no_upload)
