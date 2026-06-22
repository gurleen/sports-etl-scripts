"""``etl retrosheet`` — build a Retrosheet play-by-play Parquet locally.

This is a **local, manual** tool. Run it on your machine, then load the resulting
Parquet into the warehouse yourself (the emitted DDL maps 1:1 to the Parquet columns).

Examples
--------
    # Build the modern era (default 2000..current) into one Parquet file
    uv run etl retrosheet build

    # Every season Retrosheet publishes (single plays.zip download)
    uv run etl retrosheet build --full

    # A specific range, postseason included, partitioned by season
    uv run etl retrosheet build --start-year 2015 --end-year 2024 \
        --game-types regular --game-types worldseries --partition-by-season

    # Emit the matching CREATE TABLE statement
    uv run etl retrosheet emit-ddl --output data/retrosheet_plays_schema.sql
"""

from __future__ import annotations

from pathlib import Path

import typer
from loguru import logger

from etl_scripts.retrosheet import (
    DEFAULT_CACHE_DIR,
    TABLE_NAME,
    build_dataset,
    chadwick_id_map,
    create_indexes,
    ensure_table,
    full_ddl,
    load_parquet_to_db,
    season_range,
    write_dataset,
)

app = typer.Typer(help="Build Retrosheet play-by-play Parquet for the warehouse.")

DEFAULT_START_YEAR = 2000
"""Default to the modern era, where MLBAM id coverage is near-complete."""


@app.command()
def build(
    full: bool = typer.Option(
        False,
        "--full",
        help="Download Retrosheet's all-season plays.zip instead of per-year files.",
    ),
    start_year: int = typer.Option(DEFAULT_START_YEAR, help="First season (inclusive). Ignored with --full."),
    end_year: int | None = typer.Option(None, help="Last season (inclusive). Defaults to current year. Ignored with --full."),
    game_types: list[str] = typer.Option(
        ["regular"],
        "--game-types",
        help="Retrosheet gametype(s) to keep (e.g. regular, worldseries, lcs). Repeat the flag.",
    ),
    output: Path | None = typer.Option(None, help="Output Parquet path (file, or dir when partitioning)."),
    cache_dir: Path = typer.Option(DEFAULT_CACHE_DIR, help="Where season zips/CSVs are cached."),
    partition_by_season: bool = typer.Option(False, help="Write a Hive-partitioned dir (season=YYYY/)."),
):
    """Download, clean, and write Retrosheet plays to Parquet."""
    keep_types = None if game_types == ["all"] else game_types
    id_map = chadwick_id_map()
    if full:
        logger.info("Building Retrosheet plays from full plays.zip bundle")
        lf = build_dataset(id_map=id_map, cache_dir=cache_dir, game_types=keep_types, use_full_bundle=True)
    else:
        years = season_range(start_year, end_year)
        logger.info("Building Retrosheet plays for seasons {}..{}", years[0], years[-1])
        lf = build_dataset(years, id_map=id_map, cache_dir=cache_dir, game_types=keep_types)

    if output is None:
        suffix = "" if partition_by_season else ".parquet"
        if full:
            output = cache_dir.parent / f"retrosheet_plays_full{suffix}"
        else:
            output = cache_dir.parent / f"retrosheet_plays_{years[0]}_{years[-1]}{suffix}"

    write_dataset(lf, output, partition_by_season=partition_by_season)
    logger.info("Done. Load into the warehouse table that matches `emit-ddl` output.")


@app.command("emit-ddl")
def emit_ddl(
    table_name: str = typer.Option(TABLE_NAME, help="Target table name."),
    output: Path | None = typer.Option(None, help="Write DDL here instead of stdout."),
):
    """Print (or write) the CREATE TABLE + index statements matching the Parquet schema."""
    ddl = full_ddl(table_name)
    if output is None:
        typer.echo(ddl)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(ddl)
        logger.info("Wrote DDL to {}", output)


@app.command()
def load(
    parquet: Path = typer.Argument(..., help="Parquet file produced by `build`."),
    table_name: str = typer.Option(TABLE_NAME, help="Target table name."),
    create_index: bool = typer.Option(True, help="Create secondary indexes after loading."),
    replace_source: str | None = typer.Option(
        None, help="Delete existing rows with this source (e.g. 'retrosheet') before loading."
    ),
):
    """Create the table, load a Parquet into it, then build indexes.

    Reads DATABASE_URL / POSTGRES_* from the environment (or repo .env), same as
    the Statcast ETL. Indexes are created *after* the bulk load (far faster).
    """
    from etl_scripts.statcast import get_database_url

    url = get_database_url()
    ensure_table(url, table_name=table_name)
    rows = load_parquet_to_db(parquet, url, table_name=table_name, replace_source=replace_source)
    if create_index:
        create_indexes(url, table_name=table_name)
    logger.info("Load complete: {} rows into {}", rows, table_name)
