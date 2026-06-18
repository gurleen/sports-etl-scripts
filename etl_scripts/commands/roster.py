"""``etl roster`` — load MLB Stats API roster stints into ``mlb_roster_entries``.

Each row is one roster stint ``(person_id, team_id, start_date, status_code)``
pulled from ``GET /people/{id}?hydrate=rosterEntries``. Rows are upserted on that
key, so every command is idempotent.

Reads DATABASE_URL / POSTGRES_* from the environment or repo .env (same as the
other MLB ETLs). Set ``ETL_DB_BACKEND=duckdb`` to target a local DuckDB file.

Examples
--------
    # one player (smoke test)
    uv run etl roster person 660271
    # first 50 people from the transactions universe
    uv run etl roster backfill --limit 50
    # full backfill over every person in mlb_transactions
    uv run etl roster backfill
    # nightly refresh of recently-moved players + open stints
    uv run etl roster update-recent --days 7
"""

from __future__ import annotations

import typer
from loguru import logger

from etl_scripts.mlb_roster_entries import (
    backfill_roster_entries,
    sync_roster_entries_for_person,
    update_recent_roster_entries,
)

app = typer.Typer(help="Load MLB Stats API roster stints into mlb_roster_entries.")


@app.command()
def person(
    person_id: int = typer.Argument(..., help="MLB person id to fetch roster stints for."),
):
    """Fetch and upsert one player's roster stints."""
    res = sync_roster_entries_for_person(person_id)
    logger.info("Person {} complete: {}", person_id, res)


@app.command()
def backfill(
    limit: int | None = typer.Option(None, help="Cap the number of people (smoke tests)."),
    max_workers: int = typer.Option(8, help="Concurrent API fetch workers."),
    pause_sec: float = typer.Option(0.0, help="Per-request delay inside each worker (throttle)."),
    skip_existing: bool = typer.Option(
        False, help="Skip person ids already in mlb_roster_entries (resume an interrupted backfill)."
    ),
    source_table: str = typer.Option(
        "mlb_transactions", help="Table to draw the distinct person_id universe from."
    ),
):
    """Scrape roster stints for every person in the companion transactions table."""
    summary = backfill_roster_entries(
        pause_sec=pause_sec,
        max_workers=max_workers,
        limit=limit,
        skip_existing=skip_existing,
        source_table=source_table,
    )
    logger.info("Backfill complete: {}", {k: v for k, v in summary.items() if k != "failed_ids"})


@app.command("update-recent")
def update_recent(
    days: int = typer.Option(7, help="Refresh people who appear in the trailing N days of transactions."),
    max_workers: int = typer.Option(8, help="Concurrent API fetch workers."),
    pause_sec: float = typer.Option(0.0, help="Per-request delay inside each worker (throttle)."),
):
    """Re-fetch recently-moved players plus anyone with an open stint (idempotent upsert)."""
    res = update_recent_roster_entries(days=days, max_workers=max_workers, pause_sec=pause_sec)
    logger.info("Recent update complete: {}", {k: v for k, v in res.items() if k != "failed_ids"})
