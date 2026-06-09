"""Statcast fetch and load helpers."""

from __future__ import annotations

import csv
import math
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence, cast

import numpy as np
import pandas as pd
import pybaseball as pb  # type: ignore
import pybaseball.cache as pb_cache  # type: ignore
from loguru import logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text


def _load_repo_dotenv() -> None:
    """Load repo-root ``.env`` for local runs (``uv run`` does not load it automatically)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)


_load_repo_dotenv()

STATCAST_TABLE_NAME = "statcast"
"""Primary-key columns on ``statcast``; used as the ``ON CONFLICT`` target for upserts."""
STATCAST_CONFLICT_COLUMNS = ("game_pk", "at_bat_number", "pitch_number")
EARLIEST_DATA_DATE = datetime(2017, 1, 1)
_pb_cache_enabled = False


def _ensure_pybaseball_cache() -> None:
    global _pb_cache_enabled
    if not _pb_cache_enabled:
        pb_cache.enable()
        _pb_cache_enabled = True

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


def format_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _sanitize_value(v: Any) -> Any:
    """Normalize values for Postgres bindings (NaN/NA/numpy/pandas → plain Python)."""
    if v is None:
        return None
    if v is pd.NA:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, np.generic):
        return v.item()
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return v
    return v


def _dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Materialize rows as plain dicts (no Polars / second frame)."""
    df = df.astype(object).where(pd.notna(df), None)
    records: list[dict[str, Any]] = []
    for raw in df.to_dict(orient="records"):
        rec = {str(k): _sanitize_value(v) for k, v in raw.items()}
        records.append(rec)
    return records


def get_statcast_data(start_date: datetime, end_date: datetime) -> list[dict[str, Any]]:
    _ensure_pybaseball_cache()
    logger.info("Fetching Statcast data from {} to {}", start_date, end_date)

    start_date_str = format_date(start_date)
    end_date_str = format_date(end_date)

    raw_data = pb.statcast(start_dt=start_date_str, end_dt=end_date_str)
    drop_cols = [c for c in _DEPRECATED_COLUMNS if c in raw_data.columns]
    df = cast(pd.DataFrame, raw_data.drop(columns=drop_cols, errors="ignore"))
    df = cast(pd.DataFrame, df[df["game_type"].eq("R")].copy())
    if "game_date" in df.columns:
        parsed = pd.to_datetime(df["game_date"], errors="coerce")
        if isinstance(parsed, pd.Series):
            df["game_date"] = parsed.dt.date

    records = _dataframe_to_records(df)
    logger.info("Fetched {} records", len(records))
    return records


def _union_columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    keys: set[str] = set()
    for r in rows:
        keys.update(r.keys())
    return sorted(keys)


def _build_upsert_statement(
    table_name: str,
    columns: Sequence[str],
    *,
    conflict_columns: Sequence[str] = STATCAST_CONFLICT_COLUMNS,
) -> sql.Composed:
    for c in conflict_columns:
        if c not in columns:
            raise ValueError(
                f"Statcast upsert requires column {c!r} (must match the table primary key columns)."
            )
    update_cols = [c for c in columns if c not in conflict_columns]
    fields = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    placeholders = sql.SQL(", ").join([sql.Placeholder() for _ in columns])
    conflict = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns)
    if update_cols:
        sets = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
        )
        action = sql.SQL("DO UPDATE SET {}").format(sets)
    else:
        action = sql.SQL("DO NOTHING")

    return sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
    ).format(sql.Identifier(table_name), fields, placeholders, conflict, action)


def load_data_to_db(
    data: Sequence[dict[str, Any]],
    *,
    database_url: str | None = None,
    table_name: str = STATCAST_TABLE_NAME,
    batch_size: int = 500,
) -> int:
    """Upsert Statcast rows in batches. Returns number of rows processed.

    ``ON CONFLICT`` targets the table primary key
    ``(game_pk, at_bat_number, pitch_number)`` — see ``STATCAST_CONFLICT_COLUMNS``.
    """
    rows = list(data)
    if not rows:
        logger.info("No rows to load into {}", table_name)
        return 0

    url = database_url or get_database_url()
    logger.info("Upserting {} rows into {} (batch size {})", len(rows), table_name, batch_size)
    columns = _union_columns(rows)

    stmt = _build_upsert_statement(table_name, columns)
    tuples = [tuple(row.get(c) for c in columns) for row in rows]

    total = 0
    with psycopg2.connect(url) as conn:
        query_str = stmt.as_string(conn)
        with conn.cursor() as cur:
            for i in range(0, len(tuples), batch_size):
                chunk = tuples[i : i + batch_size]
                execute_batch(cur, query_str, chunk, page_size=len(chunk))
                total += len(chunk)
        conn.commit()

    logger.info("Rows processed: {}", total)
    return total


def _read_csv_columns(filename: str) -> list[str]:
    with open(filename, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def get_statcast_table_columns(
    database_url: str | None = None,
    *,
    table_name: str = STATCAST_TABLE_NAME,
) -> list[str]:
    """Column names on ``statcast`` (or ``table_name``), in ordinal order."""
    url = database_url or get_database_url()
    stmt = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
        ORDER BY ordinal_position
        """
    )
    engine = create_engine(url)
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"table_name": table_name}).fetchall()
    return [str(r[0]) for r in rows]


def compare_statcast_columns(
    csv_path: str,
    *,
    database_url: str | None = None,
    table_name: str = STATCAST_TABLE_NAME,
) -> dict[str, list[str]]:
    """Compare a sample CSV header to the warehouse table, ignoring deprecated Statcast fields."""
    csv_cols = set(_read_csv_columns(csv_path)) - set(_DEPRECATED_COLUMNS)
    db_cols = set(get_statcast_table_columns(database_url, table_name=table_name))
    return {
        "new_in_csv": sorted(csv_cols - db_cols),
        "only_in_db": sorted(db_cols - csv_cols),
    }


def write_statcast_csv(rows: Sequence[dict[str, Any]], filename: str) -> None:
    """Write Statcast dict rows to CSV (header from union of keys)."""
    data = list(rows)
    if not data:
        logger.warning("No rows to write to {}", filename)
        return
    columns = _union_columns(data)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(data)
    logger.info("Data written to {}", filename)


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
