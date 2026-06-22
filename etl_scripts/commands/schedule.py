"""``etl schedule`` — load MLB Stats API schedule into ``mlb_schedule``.

Reads DATABASE_URL / POSTGRES_* from the environment or repo .env (same as the
other MLB ETLs). Replaces all rows for the target ``season_year`` on each run.

Examples
--------
    uv run etl schedule season
    uv run etl schedule season 2025
    uv run etl schedule season --sport-id 1
"""

from __future__ import annotations

import typer
from loguru import logger

from etl_scripts import db
from etl_scripts.mlb_schedule import sync_mlb_schedule_for_year

app = typer.Typer(help="Load MLB Stats API schedule into mlb_schedule.")


@app.command()
def season(
    year: int | None = typer.Argument(None, help="Season year (defaults to current calendar year)."),
    sport_id: int = typer.Option(1, help="Sport id (1 = MLB)."),
    target: db.LoadTarget = typer.Option(
        db.LoadTarget.postgres, "--target", case_sensitive=False,
        help="Where to load: postgres (default), motherduck, or a local duckdb file.",
    ),
    connection: str | None = typer.Option(
        None, "--connection",
        help="Override the destination: a Postgres URL, an 'md:' MotherDuck URI, or a DuckDB file path.",
    ),
):
    """Fetch and replace the full schedule for one season.

    Use --target motherduck to populate mlb_schedule in the same database that
    'etl pbp --target motherduck' reads from (token via the 'motherduck_token' env var).
    """
    # None keeps the legacy ETL_DB_BACKEND path for the default postgres target.
    dest = None
    if target is not db.LoadTarget.postgres or connection is not None:
        try:
            dest = db.Destination.from_target(target, connection)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    summary = sync_mlb_schedule_for_year(year, sport_id=sport_id, dest=dest)
    logger.info("Schedule sync complete: {}", summary)
