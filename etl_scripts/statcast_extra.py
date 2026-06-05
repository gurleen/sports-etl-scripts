"""Savant gamefeed (`/gf`) fetch and load into ``statcast_extra``."""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from types import UnionType
from collections.abc import Callable
from typing import Any, Literal, Sequence, Union, get_args, get_origin
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

import psycopg2
from loguru import logger
from pydantic import ValidationError
from psycopg2 import sql
from psycopg2.extras import execute_batch

from etl_scripts.statcast import STATCAST_TABLE_NAME, get_database_url
from models.savant_gamefeed import PitchData, SavantGamefeed, parse_pitch_rows

STATCAST_EXTRA_TABLE = "statcast_extra"
GAMEFEED_URL = "https://baseballsavant.mlb.com/gf"
SAVANT_REQUEST_USER_AGENT = (
    "Mozilla/5.0 (compatible; etl-scripts/1.0; +https://github.com/) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_SEC = 60


def _summarize_exception(exc: BaseException, *, max_details: int = 3) -> str:
    """Short message for logs and run summaries (avoids multi-page pydantic output)."""
    if isinstance(exc, ValidationError):
        errs = exc.errors()
        n = len(errs)
        snippets: list[str] = []
        for err in errs[:max_details]:
            loc = ".".join(str(part) for part in err.get("loc", ()))
            msg = err.get("msg", "invalid")
            snippets.append(f"{loc} {msg}" if loc else msg)
        joined = "; ".join(snippets)
        if n > max_details:
            joined = f"{joined} (+{n - max_details} more)"
        return f"{n} validation error(s): {joined}"
    return str(exc)


def _pg_type_for_annotation(ann: Any) -> str:
    if ann is int:
        return "INTEGER"
    if ann is float:
        return "DOUBLE PRECISION"
    if ann is bool:
        return "BOOLEAN"
    if ann is UUID:
        return "UUID"
    if ann is str:
        return "TEXT"
    origin = get_origin(ann)
    if origin is Literal:
        return "TEXT"
    return "TEXT"


def _annotation_nullable(ann: Any) -> tuple[Any, bool]:
    """Strip ``| None`` / ``Optional`` for column typing; return (inner annotation, allows_null)."""
    origin = get_origin(ann)
    if origin is UnionType or origin is Union:
        args = get_args(ann)
        non_none = [a for a in args if a is not type(None)]
        nullable = any(a is type(None) for a in args)
        if len(non_none) == 1:
            return non_none[0], nullable
        if non_none:
            return non_none[0], nullable
    return ann, False


def _statcast_extra_column_defs(*, nullable_required: bool = False) -> list[tuple[str, str]]:
    """Column name and Postgres type clause for ``statcast_extra``."""
    lead = [
        ("game_pk", "BIGINT NOT NULL"),
        ("savant_pitch_source", "TEXT NOT NULL"),
    ]
    pitch_cols: list[tuple[str, str]] = []
    for name, finfo in PitchData.model_fields.items():
        base_ann, nullable = _annotation_nullable(finfo.annotation)
        null_sql = "NULL" if (nullable or nullable_required) else "NOT NULL"
        pitch_cols.append((name, f"{_pg_type_for_annotation(base_ann)} {null_sql}"))
    return lead + pitch_cols


def _statcast_extra_ddl() -> str:
    """CREATE TABLE for ``statcast_extra`` derived from :class:`PitchData` columns."""
    parts = [f'"{n}" {t}' for n, t in _statcast_extra_column_defs()]
    body = ",\n    ".join(parts)
    return (
        f"CREATE TABLE IF NOT EXISTS {STATCAST_EXTRA_TABLE} (\n"
        f"    {body},\n"
        f"    PRIMARY KEY (game_pk, play_id)\n"
        f");"
    )


def _ensure_statcast_extra_columns(cur) -> None:
    """Add columns introduced after the table was first created."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (STATCAST_EXTRA_TABLE,),
    )
    existing = {row[0] for row in cur.fetchall()}
    for name, typ in _statcast_extra_column_defs(nullable_required=True):
        if name in existing:
            continue
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(STATCAST_EXTRA_TABLE),
                sql.Identifier(name),
                sql.SQL(typ),
            )
        )
        logger.info("Added column {}.{} ({})", STATCAST_EXTRA_TABLE, name, typ)


def ensure_statcast_extra_table(*, database_url: str | None = None) -> None:
    url = database_url or get_database_url()
    ddl = _statcast_extra_ddl()
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            _ensure_statcast_extra_columns(cur)
        conn.commit()
    logger.debug("Ensured table {} exists", STATCAST_EXTRA_TABLE)


def _insert_columns() -> tuple[str, ...]:
    return ("game_pk", "savant_pitch_source") + tuple(PitchData.model_fields.keys())


def _fetch_gamefeed_raw(game_pk: int) -> dict[str, Any]:
    req = Request(
        f"{GAMEFEED_URL}?game_pk={game_pk}",
        headers={"User-Agent": SAVANT_REQUEST_USER_AGENT, "Accept": "application/json"},
        method="GET",
    )
    with urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def fetch_and_parse_gamefeed(game_pk: int) -> SavantGamefeed:
    data = _fetch_gamefeed_raw(game_pk)
    try:
        home = data["team_home"]
        away = data["team_away"]
    except KeyError as e:
        raise ValueError(f"game_pk={game_pk}: missing team arrays in gamefeed response") from e
    return SavantGamefeed.model_validate(
        {"team_home": parse_pitch_rows(home), "team_away": parse_pitch_rows(away)}
    )


def _game_date_range_sql(
    start_date: date | None,
    end_date: date | None,
    params: list[Any],
) -> str:
    clauses: list[str] = []
    if start_date is not None:
        clauses.append("s.game_date::date >= %s")
        params.append(start_date)
    if end_date is not None:
        clauses.append("s.game_date::date <= %s")
        params.append(end_date)
    return (" AND " + " AND ".join(clauses)) if clauses else ""


def list_game_dates_missing_extra(
    year: int,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    database_url: str | None = None,
    statcast_table: str = STATCAST_TABLE_NAME,
    limit_days: int | None = None,
) -> list[date]:
    """
    Distinct ``game_date`` values in ``year`` with at least one ``game_pk`` missing ``statcast_extra``.

    When ``start_date`` / ``end_date`` are set, only dates in that inclusive window are considered.
    Otherwise ``limit_days`` selects the N most recent missing dates (``ORDER BY`` desc).
    With no range and no limit, returns all missing dates oldest-first.
    """
    url = database_url or get_database_url()
    params: list[Any] = [year]
    range_sql = _game_date_range_sql(start_date, end_date, params)
    if start_date is not None or end_date is not None:
        order = "ASC"
    elif limit_days is not None:
        order = "DESC"
    else:
        order = "ASC"
    q = f"""
    SELECT DISTINCT s.game_date::date AS d
    FROM {statcast_table} s
    WHERE EXTRACT(YEAR FROM s.game_date)::int = %s
      AND s.game_type = 'R'
      {range_sql}
      AND NOT EXISTS (
          SELECT 1 FROM {STATCAST_EXTRA_TABLE} e WHERE e.game_pk = s.game_pk
      )
    ORDER BY 1 {order}
    """
    if limit_days is not None and start_date is None and end_date is None:
        q += " LIMIT %s"
        params.append(limit_days)
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
    return [r[0] for r in rows]


def list_game_pks_missing_extra(
    year: int,
    *,
    game_date: date | None = None,
    database_url: str | None = None,
    statcast_table: str = STATCAST_TABLE_NAME,
) -> list[int]:
    """Distinct ``game_pk`` in ``statcast`` for regular-season rows in ``year`` with no ``statcast_extra`` rows."""
    url = database_url or get_database_url()
    q = f"""
    SELECT DISTINCT s.game_pk
    FROM {statcast_table} s
    WHERE EXTRACT(YEAR FROM s.game_date)::int = %s
      AND s.game_type = 'R'
      AND NOT EXISTS (
          SELECT 1 FROM {STATCAST_EXTRA_TABLE} e WHERE e.game_pk = s.game_pk
      )
    """
    params: list[Any] = [year]
    if game_date is not None:
        q += " AND s.game_date::date = %s"
        params.append(game_date)
    q += " ORDER BY 1"
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def count_game_pks_missing_extra(
    year: int,
    game_dates: Sequence[date],
    *,
    database_url: str | None = None,
    statcast_table: str = STATCAST_TABLE_NAME,
) -> int:
    """Count distinct ``game_pk`` missing extras on the given ``game_dates``."""
    if not game_dates:
        return 0
    url = database_url or get_database_url()
    q = f"""
    SELECT COUNT(DISTINCT s.game_pk)
    FROM {statcast_table} s
    WHERE EXTRACT(YEAR FROM s.game_date)::int = %s
      AND s.game_type = 'R'
      AND s.game_date::date = ANY(%s)
      AND NOT EXISTS (
          SELECT 1 FROM {STATCAST_EXTRA_TABLE} e WHERE e.game_pk = s.game_pk
      )
    """
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (year, list(game_dates)))
            row = cur.fetchone()
    return int(row[0]) if row else 0


def _rows_for_game(game_pk: int, feed: SavantGamefeed) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for side, pitches in (("home", feed.team_home), ("away", feed.team_away)):
        for p in pitches:
            row = p.model_dump(mode="json")
            row["game_pk"] = game_pk
            row["savant_pitch_source"] = side
            out.append(row)
    return out


def replace_game_extra_rows(
    game_pk: int,
    rows: Sequence[dict[str, Any]],
    *,
    database_url: str | None = None,
) -> int:
    """Delete existing ``statcast_extra`` rows for ``game_pk`` and insert ``rows`` (single transaction)."""
    if not rows:
        logger.warning("game_pk={}: no pitch rows to insert", game_pk)
        return 0
    url = database_url or get_database_url()
    columns = _insert_columns()
    for r in rows:
        missing = [c for c in columns if c not in r]
        if missing:
            raise ValueError(f"game_pk={game_pk}: row missing keys {missing!r}")

    fields = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))
    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(STATCAST_EXTRA_TABLE),
        fields,
        placeholders,
    )
    tuples = [tuple(r[c] for c in columns) for r in rows]

    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE game_pk = %s").format(sql.Identifier(STATCAST_EXTRA_TABLE)),
                (game_pk,),
            )
            execute_batch(cur, insert_stmt.as_string(conn), tuples, page_size=len(tuples))
        conn.commit()
    return len(tuples)


def sync_missing_gamefeeds_for_year(
    year: int | None = None,
    *,
    days: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    database_url: str | None = None,
    pause_sec: float = 0,
    on_progress: Callable[[int, int, int, int], None] | None = None,
    progress_every: int = 25,
) -> dict[str, Any]:
    """
    Load Savant ``/gf`` data for ``game_pk`` values missing from ``statcast_extra``.

    Work is done **one ``game_date`` at a time** (all games that day are loaded before the next date).
    ``start_date`` / ``end_date`` restrict to an inclusive ``game_date`` window (used with Statcast ingest).
    If no window is set, ``days`` caps how many **most recent** missing dates to process; ``None`` means all
    missing dates in ``year`` (oldest first).
    """
    y = year if year is not None else datetime.now().year
    url = database_url or get_database_url()
    ensure_statcast_extra_table(database_url=url)
    use_limit = days if (start_date is None and end_date is None) else None
    game_dates = list_game_dates_missing_extra(
        y,
        start_date=start_date,
        end_date=end_date,
        limit_days=use_limit,
        database_url=url,
    )
    total_games = count_game_pks_missing_extra(y, game_dates, database_url=url)
    logger.info(
        "statcast_extra sync year={} days={} start={} end={}: {} dates, {} game_pk values",
        y,
        days,
        start_date,
        end_date,
        len(game_dates),
        total_games,
    )
    if not game_dates:
        return {
            "year": y,
            "days": days,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "days_targeted": 0,
            "dates_processed": [],
            "games_targeted": 0,
            "games_loaded": 0,
            "rows_written": 0,
            "games_failed": 0,
            "failures": [],
        }

    ok = 0
    failed: list[tuple[int, str]] = []
    rows_written = 0
    games_done = 0
    if on_progress is not None:
        on_progress(0, total_games, 0, 0)

    for day_idx, game_date in enumerate(game_dates):
        game_pks = list_game_pks_missing_extra(y, game_date=game_date, database_url=url)
        logger.info(
            "statcast_extra date {} ({}/{}): {} games",
            game_date,
            day_idx + 1,
            len(game_dates),
            len(game_pks),
        )
        for i, gpk in enumerate(game_pks):
            try:
                feed = fetch_and_parse_gamefeed(gpk)
                rows = _rows_for_game(gpk, feed)
                n = replace_game_extra_rows(gpk, rows, database_url=url)
                rows_written += n
                ok += 1
            except ValidationError as e:
                msg = _summarize_exception(e)
                logger.error("game_pk={}: gamefeed validation failed: {}", gpk, msg)
                failed.append((gpk, msg))
            except (HTTPError, URLError, OSError, ValueError, TypeError) as e:
                logger.exception("game_pk={}: failed to fetch or load gamefeed", gpk)
                failed.append((gpk, _summarize_exception(e)))
            games_done += 1
            if on_progress is not None and (
                games_done == total_games
                or (progress_every > 0 and games_done % progress_every == 0)
            ):
                on_progress(games_done, total_games, ok, len(failed))
            elif on_progress is None and progress_every > 0 and games_done % progress_every == 0:
                logger.info("statcast_extra progress: {}/{} games", games_done, total_games)
            if pause_sec > 0 and (i + 1 < len(game_pks) or day_idx + 1 < len(game_dates)):
                time.sleep(pause_sec)

    return {
        "year": y,
        "days": days,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "days_targeted": len(game_dates),
        "dates_processed": [d.isoformat() for d in game_dates],
        "games_targeted": total_games,
        "games_loaded": ok,
        "rows_written": rows_written,
        "games_failed": len(failed),
        "failures": [{"game_pk": fpk, "error": err} for fpk, err in failed[:50]],
    }
