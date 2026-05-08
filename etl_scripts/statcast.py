"""Statcast fetch and load helpers."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import polars as pl
import pybaseball as pb  # type: ignore
import pybaseball.cache as pb_cache  # type: ignore
from loguru import logger
from sqlalchemy import create_engine, text

STATCAST_TABLE_NAME = "statcast"
_pb_cache_enabled = False


def _ensure_pybaseball_cache() -> None:
    global _pb_cache_enabled
    if not _pb_cache_enabled:
        pb_cache.enable()
        _pb_cache_enabled = True
EARLIEST_DATA_DATE = datetime(2017, 1, 1)

_DEPRECATED_COLUMNS = (
    "spin_dir",
    "spin_rate_deprecated",
    "break_angle_deprecated",
    "break_length_deprecated",
    "tfs_deprecated",
    "tfs_zulu_deprecated",
    "umpire",
    "sv_id",
)


def get_database_url() -> str:
    """Resolve Postgres URL from DATABASE_URL or POSTGRES_* variables."""
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError(
            "Set DATABASE_URL or POSTGRES_PASSWORD (and optional POSTGRES_HOST, "
            "POSTGRES_USER, POSTGRES_DB, POSTGRES_PORT)."
        )
    host = os.getenv("POSTGRES_HOST", "172.237.129.152")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def format_date(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def get_statcast_data(start_date: datetime, end_date: datetime) -> pl.DataFrame:
    _ensure_pybaseball_cache()
    logger.info("Fetching Statcast data from {} to {}", start_date, end_date)

    start_date_str = format_date(start_date)
    end_date_str = format_date(end_date)

    raw_data = pb.statcast(start_dt=start_date_str, end_dt=end_date_str)

    data = (
        pl.from_pandas(raw_data)
        .with_columns(pl.col("game_date").str.to_date("%Y-%m-%d"))
        .filter(pl.col("game_type").eq("R"))
        .drop(*_DEPRECATED_COLUMNS)
    )

    logger.info("Fetched {} records", len(data))
    return data


def load_data_to_db(
    data: pl.DataFrame,
    *,
    database_url: str | None = None,
    table_name: str = STATCAST_TABLE_NAME,
) -> int:
    """Append ``data`` to ``table_name``. Returns rows written (best effort)."""
    url = database_url or get_database_url()
    logger.info("Loading data into {}", table_name)
    rows_affected = data.write_database(  # pyright: ignore[reportUnknownMemberType]
        table_name=table_name,
        connection=url,
        if_table_exists="append",
    )
    if rows_affected is not None:
        n = int(rows_affected)
    else:
        n = len(data)
    logger.info("Rows affected: {}", n)
    return n


def statcast_table_metrics(
    database_url: str | None = None,
    *,
    table_name: str = STATCAST_TABLE_NAME,
) -> dict[str, Any]:
    """Row count and max game_date for the Statcast table (before/after probes)."""
    if table_name != STATCAST_TABLE_NAME:
        raise ValueError("Only the configured Statcast table is supported for metrics.")
    url = database_url or get_database_url()
    engine = create_engine(url)
    stmt_count = text(f"SELECT COUNT(*) AS c FROM {STATCAST_TABLE_NAME}")
    stmt_max = text(f"SELECT MAX(game_date) AS m FROM {STATCAST_TABLE_NAME}")
    with engine.connect() as conn:
        count = conn.execute(stmt_count).scalar_one()
        max_game_date = conn.execute(stmt_max).scalar_one()
    return {
        "row_count": int(count),
        "max_game_date": max_game_date.isoformat() if max_game_date is not None else None,
    }
