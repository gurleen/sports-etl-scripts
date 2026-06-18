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

from etl_scripts.mlb_schedule import sync_mlb_schedule_for_year

app = typer.Typer(help="Load MLB Stats API schedule into mlb_schedule.")


@app.command()
def season(
    year: int | None = typer.Argument(None, help="Season year (defaults to current calendar year)."),
    sport_id: int = typer.Option(1, help="Sport id (1 = MLB)."),
):
    """Fetch and replace the full schedule for one season."""
    summary = sync_mlb_schedule_for_year(year, sport_id=sport_id)
    logger.info("Schedule sync complete: {}", summary)
