"""Seed a local DuckDB file for testing against the dbt ``local`` target.

Creates the ``public`` schema with the empty pbp/schedule tables that
``etl_scripts.mlb_schedule`` / ``etl_scripts.mlbam_pbp`` populate (with
``ETL_DB_BACKEND=duckdb``), and copies small static reference tables
(``players``, ``weights`` by default) read-only from the production Postgres
warehouse via DuckDB's postgres scanner extension.

Usage::

    DUCKDB_PATH=./dev.duckdb uv run python -m etl_scripts.seed_local_duckdb
"""

from __future__ import annotations

import duckdb
import typer
from loguru import logger

from etl_scripts import db
from etl_scripts.mlb_schedule import _mlb_schedule_ddl
from etl_scripts.mlbam_pbp import baserunning_ddl
from etl_scripts.retrosheet import full_ddl
from etl_scripts.statcast import get_database_url

app = typer.Typer(help="Seed a local DuckDB file for the dbt `local` target.")

REFERENCE_TABLES: tuple[str, ...] = ("players", "weights")


def _exec_script(con: duckdb.DuckDBPyConnection, sql_text: str) -> None:
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


@app.command()
def main(
    tables: list[str] = typer.Option(
        list(REFERENCE_TABLES), "--table", help="Reference tables to copy from prod (repeatable)."
    ),
) -> None:
    path = db.get_duckdb_path()
    con = duckdb.connect(path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS public")
        con.execute("SET search_path = 'public'")

        # Empty pbp/schedule tables that the loaders will fill.
        _exec_script(con, full_ddl())
        _exec_script(con, baserunning_ddl())
        _exec_script(con, _mlb_schedule_ddl())
        logger.info("Ensured retrosheet_plays, baserunning_events, mlb_schedule tables exist in {}", path)

        # Static reference tables, copied read-only from prod.
        pg_url = get_database_url().replace("'", "''")
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
        try:
            for t in tables:
                con.execute(f'DROP TABLE IF EXISTS public."{t}"')
                con.execute(f'CREATE TABLE public."{t}" AS SELECT * FROM pg.public."{t}"')
                n = con.execute(f'SELECT count(*) FROM public."{t}"').fetchone()[0]
                logger.info("Copied {} rows from prod into {}", n, t)
        finally:
            con.execute("DETACH pg")
    finally:
        con.close()


if __name__ == "__main__":
    app()
