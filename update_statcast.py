"""Typer CLI for ad-hoc Statcast updates (scheduled runs use Prefect flows)."""

from datetime import datetime, timedelta

import typer
from loguru import logger

from etl_scripts.statcast import (
    EARLIEST_DATA_DATE,
    get_database_url,
    get_statcast_data,
    load_data_to_db,
    write_statcast_csv,
)

app = typer.Typer()


@app.command()
def update_full():
    today = datetime.today()
    url = get_database_url()
    data = get_statcast_data(EARLIEST_DATA_DATE, today)
    load_data_to_db(data, database_url=url)
    logger.info("Full update completed")


@app.command()
def season(year: int):
    start_date = datetime(year, 3, 1)
    end_date = datetime(year, 11, 30)
    url = get_database_url()
    data = get_statcast_data(start_date, end_date)
    load_data_to_db(data, database_url=url)
    logger.info("Season {} update completed", year)


@app.command()
def update_recent(days: int = 1):
    today = datetime.today()
    start_date = today - timedelta(days=days)
    url = get_database_url()
    data = get_statcast_data(start_date, today)
    load_data_to_db(data, database_url=url)
    logger.info("Recent update completed")


@app.command()
def write_to_file(season: int, filename: str):
    start_date = datetime(season, 3, 1)
    end_date = datetime(season, 11, 30)
    data = get_statcast_data(start_date, end_date)
    write_statcast_csv(data, filename)
    logger.info("Data written to {}", filename)


if __name__ == "__main__":
    app()
