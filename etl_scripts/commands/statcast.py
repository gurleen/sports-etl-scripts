"""``etl statcast`` — load Baseball Savant / Statcast data into the warehouse.

``update-recent`` is the nightly entrypoint: it ingests recent Statcast rows,
syncs the matching Savant ``/gf`` rows into ``statcast_extra``, then rebuilds the
Statcast-derived dbt marts (games, coverage, statcast_events, abs_challenges) when
the ingest changed anything. The dbt step needs the ``dbt`` extra:

    uv run --extra dbt etl statcast update-recent --days 1
"""

from datetime import date, datetime, time, timedelta

import typer
from loguru import logger

from etl_scripts.statcast import (
    EARLIEST_DATA_DATE,
    compare_statcast_columns,
    get_database_url,
    get_statcast_data,
    load_data_to_db,
    write_statcast_csv,
)
from etl_scripts.statcast_backfill import backfill_statcast_missing_dates_for_year
from etl_scripts.statcast_extra import sync_missing_gamefeeds_for_year

app = typer.Typer(help="Load Baseball Savant / Statcast data into the warehouse.")


@app.command()
def update_full():
    today = datetime.today()
    url = get_database_url()
    data = get_statcast_data(EARLIEST_DATA_DATE, today)
    load_data_to_db(data, database_url=url)
    logger.info("Full update completed")


@app.command()
def backfill(
    year: int,
    limit_days: int | None = typer.Option(None, help="Max missing dates to process (oldest first)."),
    pause_sec: float = typer.Option(0.2, help="Pause between game loads."),
):
    url = get_database_url()
    summary = backfill_statcast_missing_dates_for_year(
        year,
        database_url=url,
        limit_days=limit_days,
        pause_sec=pause_sec,
    )
    logger.info("Backfill year {} complete: {}", year, summary)


@app.command()
def season(year: int):
    start_date = datetime(year, 3, 1)
    end_date = datetime(year, 11, 30)
    url = get_database_url()
    data = get_statcast_data(start_date, end_date)
    load_data_to_db(data, database_url=url)
    logger.info("Season {} update completed", year)


@app.command()
def update_date(
    game_date: str = typer.Argument(..., help="Calendar date (YYYY-MM-DD) to fetch and upsert."),
):
    d = date.fromisoformat(game_date[:10])
    day = datetime.combine(d, time.min)
    url = get_database_url()
    data = get_statcast_data(day, day)
    load_data_to_db(data, database_url=url)
    logger.info("Date {} update completed", d.isoformat())


@app.command()
def update_recent(days: int = 1):
    """Ingest recent Statcast + statcast_extra, then rebuild the Statcast marts."""
    today = datetime.today()
    start_date = today - timedelta(days=days)
    url = get_database_url()

    data = get_statcast_data(start_date, today)
    written = load_data_to_db(data, database_url=url)
    logger.info("Statcast ingest complete: rows_fetched={} rows_written={}", len(data), written)

    extra = sync_missing_gamefeeds_for_year(
        today.year, start_date=start_date.date(), end_date=today.date(), database_url=url
    )
    logger.info("statcast_extra sync complete: {}", extra)

    # dbt is an optional dependency; import lazily so `etl` works without --extra dbt.
    from etl_scripts.dbt_runner import DEFAULT_SELECTOR, run_dbt_build, statcast_relevant_data_changed

    if statcast_relevant_data_changed(
        ingest={"rows_fetched": len(data), "rows_written": written},
        statcast_extra=extra,
    ):
        logger.info("Statcast data changed; rebuilding marts (selector={})", DEFAULT_SELECTOR)
        dbt_summary = run_dbt_build(selector=DEFAULT_SELECTOR)
        logger.info("dbt build complete: {}", dbt_summary)
    else:
        logger.info("No Statcast data changed; skipping dbt rebuild")


@app.command()
def write_recent(filename: str, days: int = 1):
    today = datetime.today()
    start_date = today - timedelta(days=days)
    data = get_statcast_data(start_date, today)
    write_statcast_csv(data, filename)
    logger.info("Recent data written to {}", filename)


@app.command()
def compare_columns(
    filename: str = typer.Argument(..., help="Sample Statcast CSV (e.g. from write-recent)."),
):
    result = compare_statcast_columns(filename)
    new_cols = result["new_in_csv"]
    if new_cols:
        logger.warning("Columns in CSV but not in database ({}): {}", len(new_cols), ", ".join(new_cols))
        raise typer.Exit(code=1)
    only_in_db = result["only_in_db"]
    if only_in_db:
        logger.info(
            "No new CSV columns. Database has {} column(s) not in this sample: {}",
            len(only_in_db),
            ", ".join(only_in_db),
        )
    else:
        logger.info("CSV columns match the database (deprecated Statcast fields ignored).")


@app.command()
def write_to_file(season: int, filename: str):
    start_date = datetime(season, 3, 1)
    end_date = datetime(season, 11, 30)
    data = get_statcast_data(start_date, end_date)
    write_statcast_csv(data, filename)
    logger.info("Data written to {}", filename)
