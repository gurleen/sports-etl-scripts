"""Connection helper for switching between Postgres (prod) and DuckDB / MotherDuck.

Two ways to pick a backend:

* **Env vars (legacy default).** Set ``ETL_DB_BACKEND=duckdb`` and optionally
  ``DUCKDB_PATH`` (default ``dev.duckdb``) to point the loaders at a local DuckDB
  file instead of the production Postgres database. Postgres remains the default.
  All helpers fall back to this when no :class:`Destination` is passed.
* **An explicit :class:`Destination`.** Resolve one with
  :meth:`Destination.from_target` (``postgres`` / ``motherduck`` / ``duckdb``) and
  thread it through the helpers to load into a chosen database + schema — e.g. the
  same MotherDuck ``plays`` table the Retrosheet loader writes to.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence

from psycopg2.extras import execute_batch

from etl_scripts.statcast import get_database_url

POSTGRES_BACKEND = "postgres"
DUCKDB_BACKEND = "duckdb"
"""``MotherDuck`` is just DuckDB reached over an ``md:`` connection string, so it
shares the ``duckdb`` backend (token via the ``motherduck_token`` env var)."""


def get_backend() -> str:
    return os.getenv("ETL_DB_BACKEND", "postgres").lower()


def get_duckdb_path() -> str:
    return os.getenv("DUCKDB_PATH", "dev.duckdb")


class LoadTarget(str, Enum):
    """A user-facing ``--target`` choice for where a loader pushes data."""

    postgres = "postgres"
    motherduck = "motherduck"
    duckdb = "duckdb"


@dataclass(frozen=True)
class Destination:
    """A resolved load target: which engine, how to connect, and which schema.

    ``connection`` is a Postgres URL, a local DuckDB file path, or an ``md:`` URI.
    ``schema`` is the search_path schema to write into; ``None`` means "leave the
    connection default" (``main`` for DuckDB/MotherDuck, the server default for
    Postgres) — which is what the Retrosheet ``load`` writes to, so the env-driven
    local-mirror's ``public`` schema is opt-in via :meth:`from_env`.
    """

    backend: str
    connection: str
    schema: str | None = None

    @property
    def is_duckdb(self) -> bool:
        return self.backend == DUCKDB_BACKEND

    @classmethod
    def from_env(cls) -> "Destination":
        """The legacy env-var destination (``ETL_DB_BACKEND`` / ``DUCKDB_PATH``).

        The local DuckDB mirror uses the ``public`` schema to match the Postgres
        warehouse layout that dbt sources expect.
        """
        if get_backend() == DUCKDB_BACKEND:
            return cls(DUCKDB_BACKEND, get_duckdb_path(), schema="public")
        return cls(POSTGRES_BACKEND, get_database_url(), schema=None)

    @classmethod
    def from_target(cls, target: "LoadTarget | str", connection: str | None = None) -> "Destination":
        """Resolve a ``--target`` choice into a destination, applying env defaults.

        - ``postgres``   -> ``DATABASE_URL`` / ``POSTGRES_*`` (or ``connection``).
        - ``motherduck`` -> ``md:`` (``MOTHERDUCK_DATABASE`` or ``connection``); the
          token is read from the ``motherduck_token`` env var by DuckDB itself.
        - ``duckdb``     -> a local DuckDB file (``DUCKDB_PATH`` or ``connection``).

        Raises ``ValueError`` if MotherDuck is selected with neither a token nor an
        explicit connection. Explicit targets write to the connection-default schema
        (``schema=None``) so they line up with the Retrosheet ``load``.
        """
        target = LoadTarget(target)
        if target is LoadTarget.postgres:
            return cls(POSTGRES_BACKEND, connection or get_database_url(), schema=None)
        if target is LoadTarget.motherduck:
            if connection is None and not os.getenv("motherduck_token"):
                raise ValueError(
                    "MotherDuck needs an access token: set the 'motherduck_token' env var "
                    "(or pass --connection 'md:db?motherduck_token=...')."
                )
            return cls(DUCKDB_BACKEND, connection or os.getenv("MOTHERDUCK_DATABASE", "md:"), schema=None)
        return cls(DUCKDB_BACKEND, connection or os.getenv("DUCKDB_PATH", "dev.duckdb"), schema=None)


def is_duckdb(dest: Destination | None = None) -> bool:
    """Whether the active backend is DuckDB (``dest`` if given, else env-configured)."""
    return dest.is_duckdb if dest is not None else get_backend() == DUCKDB_BACKEND


def connect(dest: Destination | None = None) -> Any:
    """Open a connection on ``dest`` (or the env-configured backend when ``None``).

    DuckDB connections optionally get a schema created + put on the search path
    (``Destination.schema``); the legacy env path uses ``public`` to match the
    Postgres warehouse layout that dbt sources expect.
    """
    dest = dest or Destination.from_env()
    if dest.is_duckdb:
        import duckdb

        con = duckdb.connect(dest.connection)
        if dest.schema:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {dest.schema}")
            con.execute(f"SET search_path = '{dest.schema}'")
        return con
    import psycopg2

    return psycopg2.connect(dest.connection)


def cursor(conn: Any, dest: Destination | None = None) -> Any:
    """Get a cursor on ``conn``, re-applying the DuckDB search path.

    DuckDB cursors are independent connections that don't inherit the parent
    connection's session settings (e.g. ``search_path``), so unqualified
    ``CREATE TABLE``/``INSERT`` would otherwise land in the default ``main`` schema.
    """
    cur = conn.cursor()
    if dest is None:
        if get_backend() == DUCKDB_BACKEND:
            cur.execute("SET search_path = 'public'")
    elif dest.is_duckdb and dest.schema:
        cur.execute(f"SET search_path = '{dest.schema}'")
    return cur


def placeholder(dest: Destination | None = None) -> str:
    """Parameter placeholder for the backend (``%s`` for Postgres, ``?`` for DuckDB)."""
    return "?" if is_duckdb(dest) else "%s"


def insert_many(cur: Any, stmt: str, tuples: Sequence[tuple], dest: Destination | None = None) -> None:
    """Bulk-execute a parameterized INSERT/UPSERT statement built with :func:`placeholder`."""
    if not tuples:
        return
    if is_duckdb(dest):
        cur.executemany(stmt, tuples)
    else:
        execute_batch(cur, stmt, tuples, page_size=500)


def executescript(cur: Any, sql_text: str, dest: Destination | None = None) -> None:
    """Run one or more ``;``-separated DDL statements (DuckDB's execute() is single-statement)."""
    if is_duckdb(dest):
        for stmt in sql_text.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    else:
        cur.execute(sql_text)
