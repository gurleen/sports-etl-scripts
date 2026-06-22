"""``etl pbp`` — load MLB Stats API play-by-play into ``retrosheet_plays`` (source='mlbam').

Reads DATABASE_URL / POSTGRES_* from the environment or repo .env (same as the
Statcast ETL). Drives off the ``mlb_schedule`` table — run ``etl schedule season``
first if the season isn't loaded there.

Examples
--------
    uv run etl pbp update-game 776135
    uv run etl pbp season 2025            # only-missing by default
    uv run etl pbp season 2025 --reload   # re-fetch every game
    uv run --extra dbt etl pbp update-recent --days 3  # re-fetch recent finals + dbt season stats
"""

from __future__ import annotations

from datetime import date, timedelta

import typer
from loguru import logger

from etl_scripts import db
from etl_scripts.mlbam_pbp import TABLE_NAME, baserunning_ddl, load_game, load_season

app = typer.Typer(help="Load current/recent MLB Stats API play-by-play into retrosheet_plays.")

# Shared --target / --connection / --table-name options for the load commands.
_TARGET_OPTION = typer.Option(
    db.LoadTarget.postgres,
    "--target",
    case_sensitive=False,
    help="Where to load: postgres (default), motherduck, or a local duckdb file.",
)
_CONNECTION_OPTION = typer.Option(
    None,
    "--connection",
    help="Override the destination: a Postgres URL, an 'md:' MotherDuck URI, or a DuckDB file path.",
)
_TABLE_NAME_OPTION = typer.Option(
    TABLE_NAME, "--table-name", help="Play-by-play table name (e.g. 'plays' on MotherDuck)."
)


def _maybe_dest(target: db.LoadTarget, connection: str | None) -> db.Destination | None:
    """Resolve an explicit destination, or ``None`` to keep the env-var backend.

    Returns ``None`` for the default postgres target with no ``--connection`` so the
    legacy ``ETL_DB_BACKEND=duckdb`` path still works for the CLI; otherwise resolves
    the chosen ``--target`` (surfacing the MotherDuck-token error via typer).
    """
    if target is db.LoadTarget.postgres and connection is None:
        return None
    try:
        return db.Destination.from_target(target, connection)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command()
def season(
    year: int,
    reload: bool = typer.Option(False, help="Re-fetch every game (default: only games not yet loaded)."),
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
    workers: int = typer.Option(8, help="Concurrent fetch/load workers (each its own DB connection)."),
    target: db.LoadTarget = _TARGET_OPTION,
    connection: str | None = _CONNECTION_OPTION,
    table_name: str = _TABLE_NAME_OPTION,
):
    """Load all Final regular-season games for a season.

    With --target motherduck/duckdb the game list and rows go to that database; the
    'mlb_schedule' table must already exist there (run 'etl schedule season' against
    the same database first). Use --table-name to match your existing table, e.g.:

        uv run etl pbp season 2026 --target motherduck --table-name plays
    """
    dest = _maybe_dest(target, connection)
    summary = load_season(
        year, dest=dest, plays_table=table_name, only_missing=not reload,
        write_baserunning=not no_baserunning, max_workers=workers,
    )
    logger.info("Season {} load complete: {}", year, {k: v for k, v in summary.items() if k != "failures"})


@app.command("update-recent")
def update_recent(
    days: int = typer.Option(3, help="Re-fetch Final games from the last N days (box scores get corrected)."),
    year: int | None = typer.Option(None, help="Season (defaults to current year)."),
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
    workers: int = typer.Option(8, help="Concurrent fetch/load workers (each its own DB connection)."),
    target: db.LoadTarget = _TARGET_OPTION,
    connection: str | None = _CONNECTION_OPTION,
    table_name: str = _TABLE_NAME_OPTION,
):
    """Re-fetch recently-finalized games (idempotent replace), then rebuild PBP season stats."""
    dest = _maybe_dest(target, connection)
    today = date.today()
    y = year or today.year
    start = today - timedelta(days=days)
    summary = load_season(
        y, dest=dest, plays_table=table_name, only_missing=False, start_date=start, end_date=today,
        write_baserunning=not no_baserunning, max_workers=workers,
    )
    logger.info("Recent load ({}..{}) complete: {}", start, today, {k: v for k, v in summary.items() if k != "failures"})

    if db.is_duckdb(dest):
        # The dbt season-stat models build against the Postgres warehouse profile,
        # not a DuckDB/MotherDuck target, so skip them for non-Postgres loads.
        logger.info("Skipping dbt season stats rebuild for non-Postgres target")
    elif int(summary.get("games_loaded") or 0) > 0:
        # dbt is an optional dependency; import lazily so `etl` works without --extra dbt.
        from etl_scripts.dbt_runner import run_mlbam_pbp_season_stats_dbt

        logger.info("Running dbt season stat models for year {}", y)
        dbt_summary = run_mlbam_pbp_season_stats_dbt(y)
        logger.info("dbt season stats rebuild complete: {}", dbt_summary)
    else:
        logger.info("No games loaded; skipping dbt season stats rebuild")


@app.command("update-game")
def update_game(
    game_pk: int,
    no_baserunning: bool = typer.Option(False, help="Skip the baserunning_events table."),
    target: db.LoadTarget = _TARGET_OPTION,
    connection: str | None = _CONNECTION_OPTION,
    table_name: str = _TABLE_NAME_OPTION,
):
    """Load a single game by game_pk."""
    dest = _maybe_dest(target, connection)
    res = load_game(game_pk, dest=dest, plays_table=table_name, write_baserunning=not no_baserunning)
    logger.info("Loaded game_pk={}: {}", game_pk, res)


@app.command("emit-baserunning-ddl")
def emit_baserunning_ddl():
    """Print the baserunning_events CREATE TABLE statement."""
    typer.echo(baserunning_ddl())
