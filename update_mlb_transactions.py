"""Typer CLI for loading MLB Stats API transactions into ``mlb_transactions``.

Reads DATABASE_URL / POSTGRES_* from the environment or repo .env (same as the
other MLB ETLs). Rows are upserted on ``transaction_id``, so every command is
idempotent.

Examples
--------
    uv run python update_mlb_transactions.py range-load 2026-01-01 2026-01-31
    uv run python update_mlb_transactions.py backfill 2021-01-01           # ..through today
    uv run python update_mlb_transactions.py backfill 2021-01-01 2023-12-31 --chunk-days 30
    uv run python update_mlb_transactions.py update-recent --days 7
"""

from __future__ import annotations

import typer
from loguru import logger

from etl_scripts.mlb_transactions import (
    backfill_transactions,
    sync_transactions_for_range,
    update_recent_transactions,
)

app = typer.Typer(help="Load MLB Stats API transactions into mlb_transactions.")


@app.command("range-load")
def range_load(
    start_date: str = typer.Argument(..., help="Inclusive window start (YYYY-MM-DD)."),
    end_date: str = typer.Argument(..., help="Inclusive window end (YYYY-MM-DD)."),
    team_id: int | None = typer.Option(None, help="Limit to one team id."),
    sport_id: int | None = typer.Option(None, help="Limit to one sport id (1 = MLB)."),
):
    """Fetch and upsert transactions for a single date window."""
    res = sync_transactions_for_range(
        start_date, end_date, team_id=team_id, sport_id=sport_id
    )
    logger.info("Range {}..{} complete: {}", start_date, end_date, res)


@app.command()
def backfill(
    start_date: str = typer.Argument(..., help="Inclusive backfill start (YYYY-MM-DD)."),
    end_date: str | None = typer.Argument(None, help="Inclusive end (YYYY-MM-DD); defaults to today."),
    chunk_days: int = typer.Option(30, help="Days per API request window."),
    team_id: int | None = typer.Option(None, help="Limit to one team id."),
    sport_id: int | None = typer.Option(None, help="Limit to one sport id (1 = MLB)."),
):
    """Walk a historical range in bounded chunks, upserting each window."""
    summary = backfill_transactions(
        start_date, end_date, chunk_days=chunk_days, team_id=team_id, sport_id=sport_id
    )
    logger.info("Backfill complete: {}", summary)


@app.command("update-recent")
def update_recent(
    days: int = typer.Option(7, help="Re-fetch the trailing N days (effective/resolution dates get filled in later)."),
    team_id: int | None = typer.Option(None, help="Limit to one team id."),
    sport_id: int | None = typer.Option(None, help="Limit to one sport id (1 = MLB)."),
):
    """Re-fetch recent transactions (idempotent upsert)."""
    res = update_recent_transactions(days=days, team_id=team_id, sport_id=sport_id)
    logger.info("Recent update complete: {}", res)


if __name__ == "__main__":
    app()
