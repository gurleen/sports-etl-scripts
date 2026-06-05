"""Load MLB Stats API schedule into ``mlb_schedule``."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

import psycopg2
from loguru import logger
from psycopg2 import sql
from psycopg2.extras import execute_batch

from api_clients import MlbApiClient
from etl_scripts.statcast import get_database_url
from models.mlb_schedule import ScheduleGame

MLB_SCHEDULE_TABLE = "mlb_schedule"

_INSERT_COLUMNS: tuple[str, ...] = (
    "game_pk",
    "season_year",
    "game_date",
    "official_date",
    "abstract_game_state",
    "detailed_state",
    "coded_game_state",
    "away_team_id",
    "away_team_name",
    "away_score",
    "away_is_winner",
    "home_team_id",
    "home_team_name",
    "home_score",
    "home_is_winner",
    "venue_id",
    "venue_name",
    "game_type",
    "series_description",
    "double_header",
    "is_tie",
)


def _mlb_schedule_ddl() -> str:
    cols = ",\n    ".join(
        [
            '"game_pk" BIGINT NOT NULL',
            '"season_year" INTEGER NOT NULL',
            '"game_date" TIMESTAMPTZ NOT NULL',
            '"official_date" DATE NOT NULL',
            '"abstract_game_state" TEXT NOT NULL',
            '"detailed_state" TEXT NOT NULL',
            '"coded_game_state" TEXT NOT NULL',
            '"away_team_id" INTEGER NOT NULL',
            '"away_team_name" TEXT NOT NULL',
            '"away_score" INTEGER',
            '"away_is_winner" BOOLEAN',
            '"home_team_id" INTEGER NOT NULL',
            '"home_team_name" TEXT NOT NULL',
            '"home_score" INTEGER',
            '"home_is_winner" BOOLEAN',
            '"venue_id" INTEGER NOT NULL',
            '"venue_name" TEXT NOT NULL',
            '"game_type" TEXT NOT NULL',
            '"series_description" TEXT',
            '"double_header" TEXT NOT NULL',
            '"is_tie" BOOLEAN NOT NULL',
        ]
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {MLB_SCHEDULE_TABLE} (\n"
        f"    {cols},\n"
        f"    PRIMARY KEY (game_pk)\n"
        f");"
    )


def ensure_mlb_schedule_table(*, database_url: str | None = None) -> None:
    url = database_url or get_database_url()
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(_mlb_schedule_ddl())
        conn.commit()
    logger.debug("Ensured table {} exists", MLB_SCHEDULE_TABLE)


def _row_for_game(game: ScheduleGame, *, season_year: int) -> dict[str, Any]:
    return {
        "game_pk": game.game_pk,
        "season_year": season_year,
        "game_date": game.game_date,
        "official_date": game.official_date,
        "abstract_game_state": game.status.abstract_game_state,
        "detailed_state": game.status.detailed_state,
        "coded_game_state": game.status.coded_game_state,
        "away_team_id": game.teams.away.team.id,
        "away_team_name": game.teams.away.team.name,
        "away_score": game.teams.away.score,
        "away_is_winner": game.teams.away.is_winner,
        "home_team_id": game.teams.home.team.id,
        "home_team_name": game.teams.home.team.name,
        "home_score": game.teams.home.score,
        "home_is_winner": game.teams.home.is_winner,
        "venue_id": game.venue.id,
        "venue_name": game.venue.name,
        "game_type": game.game_type,
        "series_description": game.series_description,
        "double_header": game.double_header,
        "is_tie": game.is_tie,
    }


def _rows_from_schedule_response(response: Any, *, season_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in response.dates:
        for game in day.games:
            rows.append(_row_for_game(game, season_year=season_year))
    return rows


def replace_schedule_for_year(
    year: int,
    rows: Sequence[dict[str, Any]],
    *,
    database_url: str | None = None,
) -> int:
    """Replace all ``mlb_schedule`` rows for ``season_year`` with ``rows``."""
    url = database_url or get_database_url()
    ensure_mlb_schedule_table(database_url=url)
    if not rows:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE season_year = %s").format(
                        sql.Identifier(MLB_SCHEDULE_TABLE)
                    ),
                    (year,),
                )
            conn.commit()
        return 0

    columns = _INSERT_COLUMNS
    fields = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))
    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(MLB_SCHEDULE_TABLE),
        fields,
        placeholders,
    )
    tuples = [tuple(r[c] for c in columns) for r in rows]

    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE season_year = %s").format(
                    sql.Identifier(MLB_SCHEDULE_TABLE)
                ),
                (year,),
            )
            execute_batch(cur, insert_stmt.as_string(conn), tuples, page_size=500)
        conn.commit()
    return len(tuples)


def mlb_schedule_table_metrics(*, database_url: str | None = None) -> dict[str, Any]:
    url = database_url or get_database_url()
    ensure_mlb_schedule_table(database_url=url)
    q = f"""
    SELECT
        COUNT(*)::bigint AS row_count,
        MAX(official_date) AS max_official_date,
        MIN(official_date) AS min_official_date
    FROM {MLB_SCHEDULE_TABLE}
    """
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            row = cur.fetchone()
    if not row:
        return {"row_count": 0, "max_official_date": None, "min_official_date": None}
    return {
        "row_count": int(row[0]),
        "max_official_date": row[1].isoformat() if row[1] else None,
        "min_official_date": row[2].isoformat() if row[2] else None,
    }


def sync_mlb_schedule_for_year(
    year: int | None = None,
    *,
    sport_id: int = 1,
    database_url: str | None = None,
) -> dict[str, Any]:
    """
    Fetch ``GET /schedule`` for ``season=year`` and replace ``mlb_schedule`` rows for that year.

    Includes all game types returned by the API (regular season, spring training, postseason, etc.).
    """
    y = year if year is not None else datetime.now().year
    url = database_url or get_database_url()
    ensure_mlb_schedule_table(database_url=url)

    client = MlbApiClient()
    schedule = client.stats.get_schedule(sport_id=sport_id, season=y)
    rows = _rows_from_schedule_response(schedule, season_year=y)
    written = replace_schedule_for_year(y, rows, database_url=url)

    game_types: dict[str, int] = {}
    for r in rows:
        gt = str(r["game_type"])
        game_types[gt] = game_types.get(gt, 0) + 1

    logger.info(
        "mlb_schedule sync year={}: api_total_games={} rows_written={} game_types={}",
        y,
        schedule.total_games,
        written,
        game_types,
    )
    return {
        "year": y,
        "sport_id": sport_id,
        "api_total_games": schedule.total_games,
        "schedule_dates": len(schedule.dates),
        "rows_written": written,
        "game_types": game_types,
    }
