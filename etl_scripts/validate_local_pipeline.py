"""End-to-end validation of the local DuckDB pipeline for a given season.

Seeds the local DuckDB file (if missing), syncs ``mlb_schedule``, loads
play-by-play from the MLB Stats API, runs the full-season dbt marts, and
prints batting/pitching leaderboards for spot-checking against an external
source (e.g. FanGraphs).

Usage::

    DUCKDB_PATH=./dev.duckdb uv run python -m etl_scripts.validate_local_pipeline run 2026
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import typer
from loguru import logger

app = typer.Typer(help="Validate the local DuckDB pipeline against a given season.")

REPO_ROOT = Path(__file__).resolve().parents[1]

BATTING_COLUMNS: tuple[str, ...] = (
    "player_id", "season", "full_name", "pa", "ab", "h", "singles", "doubles",
    "triples", "hr", "bb", "ubb", "ibb", "hbp", "so", "sf", "qualified",
    "avg", "obp", "slg", "ops", "iso", "babip", "bb_pct", "k_pct", "woba",
)

PITCHING_COLUMNS: tuple[str, ...] = (
    "player_id", "season", "full_name", "bf", "h", "hr", "bb", "ubb", "ibb",
    "hbp", "so", "r", "er", "qualified", "ip", "baa", "era", "whip", "k9",
    "bb9", "hr9", "k_pct", "bb_pct", "fip",
)

DBT_SELECT: tuple[str, ...] = (
    "stg_pbp__events",
    "int_pitching__responsible_er",
    "batting_stats_season",
    "pitching_stats_season",
    "pitching_stats_monthly",
)


@app.command()
def run(
    season: int = typer.Argument(..., help="Season year to load and build, e.g. 2026."),
    workers: int = typer.Option(1, help="Concurrent PBP fetch/load workers."),
    reseed: bool = typer.Option(False, help="Recreate reference tables (players, weights) from prod."),
    top_n: int = typer.Option(10, help="Number of rows to show in each leaderboard."),
) -> None:
    """Seed (if needed), load season data, build dbt marts, and print leaderboards."""
    os.environ.setdefault("ETL_DB_BACKEND", "duckdb")
    os.environ.setdefault("DUCKDB_PATH", "dev.duckdb")

    from etl_scripts import db
    from etl_scripts.mlb_schedule import sync_mlb_schedule_for_year
    from etl_scripts.mlbam_pbp import load_season
    from etl_scripts.seed_local_duckdb import REFERENCE_TABLES
    from etl_scripts.seed_local_duckdb import main as seed_main

    duckdb_path = db.get_duckdb_path()
    if reseed or not Path(duckdb_path).exists():
        logger.info("Seeding {}", duckdb_path)
        seed_main(tables=list(REFERENCE_TABLES))

    logger.info("Syncing mlb_schedule for {}", season)
    sched = sync_mlb_schedule_for_year(season)
    logger.info("Schedule sync: {}", sched)

    logger.info("Loading play-by-play for {} (workers={})", season, workers)
    pbp = load_season(season, only_missing=True, write_baserunning=False, max_workers=workers)
    logger.info("PBP load: {}", {k: v for k, v in pbp.items() if k != "failures"})

    logger.info("Running dbt build")
    subprocess.run(
        [
            "uv", "run", "dbt", "build",
            "--target", "local",
            "--profiles-dir", str(REPO_ROOT),
            "--select", *DBT_SELECT,
        ],
        cwd=REPO_ROOT,
        check=True,
    )

    _print_leaderboards(season, top_n)


def _print_leaderboards(season: int, top_n: int) -> None:
    import duckdb

    con = duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    try:
        bdf = con.execute("SELECT * FROM baseball.batting_stats_season WHERE season = ?", [season]).df()
        bdf.columns = BATTING_COLUMNS

        pdf = con.execute("SELECT * FROM baseball.pitching_stats_season WHERE season = ?", [season]).df()
        pdf.columns = PITCHING_COLUMNS
    finally:
        con.close()

    bdf = bdf[bdf["qualified"]].sort_values("pa", ascending=False).head(top_n)
    pdf = pdf[pdf["qualified"]].sort_values("ip", ascending=False).head(top_n)

    print(f"\n=== Batting leaders ({season}, qualified, top {top_n} by PA) ===")
    print(bdf[["full_name", "pa", "hr", "avg", "obp", "slg", "ops", "woba"]].to_string(index=False))

    print(f"\n=== Pitching leaders ({season}, qualified, top {top_n} by IP) ===")
    print(
        pdf[["full_name", "ip", "bf", "h", "hr", "bb", "so", "er", "era", "whip", "k9", "bb9", "fip"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    app()
