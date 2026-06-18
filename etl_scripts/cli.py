"""Unified ``etl`` command-line entrypoint.

One Typer app with a sub-command group per domain. Replaces the old top-level
``update_*.py`` / ``build_retrosheet.py`` scripts. Exposed as the ``etl`` console
script (see ``[project.scripts]`` in ``pyproject.toml``):

    uv run etl --help
    uv run --extra dbt etl statcast update-recent --days 1
    uv run --extra dbt etl pbp update-recent --days 3
    uv run etl transactions update-recent --days 7
    uv run etl roster update-recent --days 7 --max-workers 8

The ``dbt``-backed steps (``statcast``/``pbp`` recent rebuilds, ``etl dbt build``)
import dbt lazily, so the rest of the CLI works without the ``dbt`` extra.
"""

from __future__ import annotations

import typer

from etl_scripts.commands import (
    contracts,
    dbt,
    pbp,
    retrosheet,
    roster,
    schedule,
    statcast,
    transactions,
)

app = typer.Typer(help="Baseball ETL — ingest MLB data and build dbt marts.", no_args_is_help=True)

app.add_typer(statcast.app, name="statcast", help="Baseball Savant / Statcast ingest + marts.")
app.add_typer(pbp.app, name="pbp", help="MLB Stats API play-by-play (retrosheet_plays).")
app.add_typer(schedule.app, name="schedule", help="MLB Stats API schedule (mlb_schedule).")
app.add_typer(transactions.app, name="transactions", help="MLB Stats API transactions.")
app.add_typer(roster.app, name="roster", help="MLB Stats API roster stints.")
app.add_typer(contracts.app, name="contracts", help="Team payroll CSV -> mlb_contracts.")
app.add_typer(retrosheet.app, name="retrosheet", help="Build historical Retrosheet play-by-play.")
app.add_typer(dbt.app, name="dbt", help="Run dbt builds (needs the dbt extra).")


if __name__ == "__main__":
    app()
