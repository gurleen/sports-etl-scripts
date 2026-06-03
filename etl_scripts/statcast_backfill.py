"""Backfill ``statcast`` for regular-season dates in a year with no loaded rows."""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import date, datetime, time as dt_time
from typing import Any

from loguru import logger
import psycopg2

from etl_scripts.statcast import (
    STATCAST_TABLE_NAME,
    get_database_url,
    get_statcast_data,
    load_data_to_db,
)

SEASON_START = (3, 1)
SEASON_END = (11, 30)


def season_date_bounds(year: int) -> tuple[date, date]:
    """Inclusive MLB regular-season calendar window used for gap detection."""
    return date(year, *SEASON_START), date(year, *SEASON_END)


def list_game_dates_missing_statcast(
    year: int,
    *,
    database_url: str | None = None,
    statcast_table: str = STATCAST_TABLE_NAME,
    limit_days: int | None = None,
) -> list[date]:
    """
    Calendar dates in the season window with no regular-season ``statcast`` rows for ``year``.

    Returns dates oldest-first; ``limit_days`` caps how many to return (still oldest-first).
    """
    start, end = season_date_bounds(year)
    url = database_url or get_database_url()
    q = f"""
    WITH season_days AS (
        SELECT gs::date AS d
        FROM generate_series(%s::date, %s::date, interval '1 day') AS gs
    ),
    loaded AS (
        SELECT DISTINCT game_date::date AS d
        FROM {statcast_table}
        WHERE game_type = 'R'
          AND EXTRACT(YEAR FROM game_date)::int = %s
    )
    SELECT sd.d
    FROM season_days sd
    LEFT JOIN loaded ld ON ld.d = sd.d
    WHERE ld.d IS NULL
    ORDER BY sd.d ASC
    """
    params: list[Any] = [start, end, year]
    if limit_days is not None:
        q += " LIMIT %s"
        params.append(limit_days)
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
    return [r[0] for r in rows]


def _group_rows_by_game_pk(rows: Sequence[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        gpk = row.get("game_pk")
        if gpk is None:
            continue
        groups[int(gpk)].append(row)
    return dict(groups)


def backfill_statcast_missing_dates_for_year(
    year: int,
    *,
    database_url: str | None = None,
    pause_sec: float = 0.2,
    limit_days: int | None = None,
    on_progress: Callable[[int, int, int, int], None] | None = None,
    progress_every: int = 25,
) -> dict[str, Any]:
    """
    Fetch and load Statcast for dates in ``year`` with no rows yet.

    One ``game_date`` per API call; within each date, upsert **one ``game_pk`` at a time**
    (same pacing pattern as ``statcast_extra``).
    """
    url = database_url or get_database_url()
    game_dates = list_game_dates_missing_statcast(year, database_url=url, limit_days=limit_days)
    logger.info(
        "statcast backfill year={} limit_days={}: {} dates without statcast rows",
        year,
        limit_days,
        len(game_dates),
    )
    if not game_dates:
        return {
            "year": year,
            "limit_days": limit_days,
            "dates_targeted": 0,
            "dates_processed": [],
            "games_loaded": 0,
            "rows_written": 0,
            "games_failed": 0,
            "date_fetch_failures": [],
            "failures": [],
        }

    games_loaded = 0
    rows_written = 0
    games_failed = 0
    failed_games: list[dict[str, Any]] = []
    failed_dates: list[dict[str, str]] = []
    games_done = 0
    total_games = 0

    if on_progress is not None:
        on_progress(0, len(game_dates), 0, 0)

    for day_idx, game_date in enumerate(game_dates):
        day = datetime.combine(game_date, dt_time.min)
        logger.info(
            "statcast backfill date {} ({}/{}): fetching",
            game_date,
            day_idx + 1,
            len(game_dates),
        )
        try:
            day_rows = get_statcast_data(day, day)
        except Exception as e:
            msg = str(e)
            logger.exception("game_date={}: Statcast fetch failed", game_date)
            failed_dates.append({"game_date": game_date.isoformat(), "error": msg})
            if on_progress is not None:
                on_progress(day_idx + 1, len(game_dates), games_loaded, games_failed)
            continue

        by_game = _group_rows_by_game_pk(day_rows)
        game_pks = sorted(by_game.keys())
        total_games += len(game_pks)
        logger.info(
            "statcast backfill date {} ({}/{}): {} games, {} pitch rows",
            game_date,
            day_idx + 1,
            len(game_dates),
            len(game_pks),
            len(day_rows),
        )

        for i, gpk in enumerate(game_pks):
            game_rows = by_game[gpk]
            try:
                n = load_data_to_db(game_rows, database_url=url)
                rows_written += n
                games_loaded += 1
            except Exception as e:
                msg = str(e)
                logger.exception("game_pk={} game_date={}: load failed", gpk, game_date)
                games_failed += 1
                failed_games.append({"game_pk": gpk, "game_date": game_date.isoformat(), "error": msg})
            games_done += 1
            if on_progress is not None and (
                games_done == total_games
                or (progress_every > 0 and games_done % progress_every == 0)
            ):
                on_progress(day_idx + 1, len(game_dates), games_loaded, games_failed)
            elif on_progress is None and progress_every > 0 and games_done % progress_every == 0:
                logger.info("statcast backfill progress: {}/{} games", games_done, total_games)
            if pause_sec > 0 and (i + 1 < len(game_pks) or day_idx + 1 < len(game_dates)):
                time.sleep(pause_sec)

        if on_progress is not None:
            on_progress(day_idx + 1, len(game_dates), games_loaded, games_failed)

    return {
        "year": year,
        "limit_days": limit_days,
        "dates_targeted": len(game_dates),
        "dates_processed": [d.isoformat() for d in game_dates],
        "games_targeted": total_games,
        "games_loaded": games_loaded,
        "rows_written": rows_written,
        "games_failed": games_failed,
        "date_fetch_failures": failed_dates[:50],
        "failures": failed_games[:50],
    }
