import typer
from datetime import datetime, timedelta
from loguru import logger
import pybaseball as pb  # type: ignore
import polars as pl
import os

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not POSTGRES_PASSWORD:
    raise ValueError("POSTGRES_PASSWORD environment variable is not set.")

DATABASE_URL = f"postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/postgres"
EARLIEST_DATA_DATE = datetime(2017, 1, 1)
STATCAST_TABLE_NAME = "statcast_data"


app = typer.Typer()


def format_date(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def get_statcast_data(start_date: datetime, end_date: datetime):
    logger.info(f"Fetching Statcast data from {start_date} to {end_date}")

    start_date_str = format_date(start_date)
    end_date_str = format_date(end_date)

    raw_data = pb.statcast(start_dt=start_date_str, end_dt=end_date_str)

    data = (
        pl.from_pandas(raw_data)
        .with_columns(pl.col("game_date").str.to_date("%Y-%m-%d"))
        .filter(pl.col("game_type").eq("R"))
    )

    logger.info(f"Fetched {len(data)} records")
    return data


def load_data_to_db(data: pl.DataFrame):
    logger.info("Loading data into the database")
    rows_affected = data.write_database(            # pyright: ignore[reportUnknownMemberType]
        table_name=STATCAST_TABLE_NAME,
        connection=DATABASE_URL,
        if_table_exists="append",
    )
    logger.info(f"Rows affected: {rows_affected}")


@app.command()
def update_full():
    today = datetime.today()
    data = get_statcast_data(EARLIEST_DATA_DATE, today)
    load_data_to_db(data)
    logger.info("Full update completed")


@app.command()
def update_recent(days: int = 1):
    today = datetime.today()
    start_date = today - timedelta(days=days)
    data = get_statcast_data(start_date, today)
    load_data_to_db(data)
    logger.info("Recent update completed")


if __name__ == "__main__":
    app()
