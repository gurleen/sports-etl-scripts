"""``etl dbt`` — run dbt builds programmatically (requires the ``dbt`` extra).

Thin wrapper over :func:`etl_scripts.dbt_runner.run_dbt_build`. Most nightly dbt
rebuilds happen automatically inside ``etl statcast update-recent`` and
``etl pbp update-recent``; use this for manual/ad-hoc rebuilds.

Examples
--------
    uv run --extra dbt etl dbt build                                   # post_statcast_ingest selector
    uv run --extra dbt etl dbt build --selector post_statcast_extra_ingest
    uv run --extra dbt etl dbt build --season-year 2025
"""

from __future__ import annotations

import typer
from loguru import logger

app = typer.Typer(help="Run dbt builds programmatically (needs the dbt extra).")


@app.command()
def build(
    selector: str = typer.Option("post_statcast_ingest", help="dbt selector name (see selectors.yml)."),
    season_year: int | None = typer.Option(None, help="Override season_year var; defaults to current year."),
):
    """Run ``dbt build --selector <selector>`` via the in-process dbt runner."""
    # dbt is an optional dependency; import lazily so `etl` works without --extra dbt.
    from etl_scripts.dbt_runner import run_dbt_build

    summary = run_dbt_build(selector=selector, season_year=season_year)
    logger.info("dbt build complete: {}", summary)
