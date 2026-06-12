"""Connection helper for switching between Postgres (prod) and a local DuckDB file (testing).

Set ``ETL_DB_BACKEND=duckdb`` and optionally ``DUCKDB_PATH`` (default ``dev.duckdb``) to
point the loaders in :mod:`etl_scripts.mlb_schedule` and :mod:`etl_scripts.mlbam_pbp` at a
local DuckDB file instead of the production Postgres database. Postgres remains the default.
"""

from __future__ import annotations

import os
from typing import Any, Sequence

from psycopg2.extras import execute_batch

from etl_scripts.statcast import get_database_url


def get_backend() -> str:
    return os.getenv("ETL_DB_BACKEND", "postgres").lower()


def get_duckdb_path() -> str:
    return os.getenv("DUCKDB_PATH", "dev.duckdb")


def connect() -> Any:
    """Open a connection on the configured backend.

    DuckDB connections get a ``public`` schema (matching the Postgres warehouse layout
    that dbt sources expect) on the default search path.
    """
    if get_backend() == "duckdb":
        import duckdb

        con = duckdb.connect(get_duckdb_path())
        con.execute("CREATE SCHEMA IF NOT EXISTS public")
        con.execute("SET search_path = 'public'")
        return con
    import psycopg2

    return psycopg2.connect(get_database_url())


def cursor(conn: Any) -> Any:
    """Get a cursor on ``conn``, with the ``public`` search path applied for DuckDB.

    DuckDB cursors are independent connections that don't inherit the parent
    connection's session settings (e.g. ``search_path``), so unqualified
    ``CREATE TABLE``/``INSERT`` would otherwise land in the default ``main`` schema.
    """
    cur = conn.cursor()
    if get_backend() == "duckdb":
        cur.execute("SET search_path = 'public'")
    return cur


def placeholder() -> str:
    """Parameter placeholder for the configured backend (``%s`` for Postgres, ``?`` for DuckDB)."""
    return "?" if get_backend() == "duckdb" else "%s"


def insert_many(cur: Any, stmt: str, tuples: Sequence[tuple]) -> None:
    """Bulk-execute a parameterized INSERT/UPSERT statement built with :func:`placeholder`."""
    if not tuples:
        return
    if get_backend() == "duckdb":
        cur.executemany(stmt, tuples)
    else:
        execute_batch(cur, stmt, tuples, page_size=500)


def executescript(cur: Any, sql_text: str) -> None:
    """Run one or more ``;``-separated DDL statements (DuckDB's execute() is single-statement)."""
    if get_backend() == "duckdb":
        for stmt in sql_text.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    else:
        cur.execute(sql_text)
