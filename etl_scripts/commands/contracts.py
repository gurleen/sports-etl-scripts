"""``etl contracts`` — parse team payroll CSVs into one row per player contract.

The ``parse`` command flattens the Cot's-style payroll sheet and prints the clean rows
(or writes a tidy CSV with ``--out``). The ``load`` command upserts them into
``mlb_contracts`` (idempotent on ``source_file`` + ``source_row``); pass ``--print``
to preview without touching the database. Reads DATABASE_URL / POSTGRES_* from the
environment or repo .env, same as the other MLB ETLs.

Examples
--------
    uv run etl contracts parse "MLB Philadelphia 26.csv"
    uv run etl contracts parse *.csv --out contracts_clean.csv
    uv run etl contracts load "MLB Philadelphia 26.csv" --print
    uv run etl contracts load "MLB Philadelphia 26.csv"
"""

from __future__ import annotations

import csv
import sys

import typer
from loguru import logger

from etl_scripts.mlb_contracts import (
    CSV_COLUMNS,
    load_contracts,
    parse_files,
    to_csv_rows,
    write_csv,
)

app = typer.Typer(help="Parse team payroll CSVs into one clean row per player contract.")


def _print_csv(contracts) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(to_csv_rows(contracts))


@app.command()
def parse(
    files: list[str] = typer.Argument(..., help="One or more payroll CSV files."),
    out: str | None = typer.Option(None, "--out", help="Write the clean CSV here (default: print to stdout)."),
):
    """Parse payroll CSV(s) and emit the cleaned one-row-per-player table."""
    contracts = parse_files(files)
    if out:
        write_csv(contracts, out)
    else:
        _print_csv(contracts)
    logger.info("Parsed {} contracts from {} file(s)", len(contracts), len(files))


@app.command()
def load(
    files: list[str] = typer.Argument(..., help="One or more payroll CSV files."),
    print_only: bool = typer.Option(
        False, "--print", help="Preview the rows on stdout instead of writing to the database."
    ),
):
    """Parse payroll CSV(s) and upsert into ``mlb_contracts`` (or preview with --print)."""
    contracts = parse_files(files)
    if print_only:
        _print_csv(contracts)
        logger.info("Previewed {} contracts (no DB write)", len(contracts))
        return
    written = load_contracts(contracts)
    logger.info("Load complete: {} rows upserted", written)
