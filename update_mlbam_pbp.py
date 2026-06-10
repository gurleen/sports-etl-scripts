"""Typer CLI for loading MLB Stats API play-by-play into ``retrosheet_plays`` (source='mlbam').

Reads DATABASE_URL / POSTGRES_* from the environment or repo .env (same as the
Statcast ETL). Drives off the ``mlb_schedule`` table — run the schedule sync
first if the season isn't loaded there.

Examples
--------
    uv run python update_mlbam_pbp.py update-game 776135
    uv run python update_mlbam_pbp.py season 2025            # only-missing by default
    uv run python update_mlbam_pbp.py season 2025 --reload   # re-fetch every game
    uv run python update_mlbam_pbp.py update-recent --days 3  # re-fetch recent finals
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import typer
from loguru import logger

from etl_scripts.mlbam_pbp import baserunning_ddl, load_game, load_season

app = typer.Typer(help="Load current/recent MLB Stats API play-by-play into retrosheet_plays.")


@app.command()
def season(
    year: int,
    reload: bool = typer.Option(False, help="Re-fetch every game (default: only games not yet loaded)."),
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
    workers: int = typer.Option(8, help="Concurrent fetch/load workers (each its own DB connection)."),
):
    """Load all Final regular-season games for a season."""
    summary = load_season(
        year, only_missing=not reload, write_baserunning=not no_baserunning, max_workers=workers
    )
    logger.info("Season {} load complete: {}", year, {k: v for k, v in summary.items() if k != "failures"})


@app.command("update-recent")
def update_recent(
    days: int = typer.Option(3, help="Re-fetch Final games from the last N days (box scores get corrected)."),
    year: int | None = typer.Option(None, help="Season (defaults to current year)."),
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
    workers: int = typer.Option(8, help="Concurrent fetch/load workers (each its own DB connection)."),
):
    """Re-fetch recently-finalized games (idempotent replace)."""
    today = date.today()
    y = year or today.year
    start = today - timedelta(days=days)
    summary = load_season(
        y, only_missing=False, start_date=start, end_date=today,
        write_baserunning=not no_baserunning, max_workers=workers,
    )
    logger.info("Recent load ({}..{}) complete: {}", start, today, {k: v for k, v in summary.items() if k != "failures"})


@app.command("update-game")
def update_game(
    game_pk: int,
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
):
    """Load a single game by game_pk."""
    res = load_game(game_pk, write_baserunning=not no_baserunning)
    logger.info("Loaded game_pk={}: {}", game_pk, res)


@app.command("emit-baserunning-ddl")
def emit_baserunning_ddl():
    """Print the baserunning_events CREATE TABLE statement."""
    typer.echo(baserunning_ddl())


if __name__ == "__main__":
    app()
