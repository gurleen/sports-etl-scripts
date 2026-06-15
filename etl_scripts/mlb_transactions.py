"""Load MLB Stats API transactions into ``mlb_transactions``.

Source: ``GET /api/v1/transactions?startDate=&endDate=`` (``MlbApiClient.stats.get_transactions``).
One row per transaction ``id``. ``person``, ``fromTeam`` and ``toTeam`` are stored as
their bare integer ids (``person_id`` / ``from_team_id`` / ``to_team_id``); the human
names live in the free-text ``description``.

Follows the schedule loader's shape: a range-sync primitive plus two drivers —
``backfill_transactions`` (chunked walk over a historical window) and
``update_recent_transactions`` (re-fetch the trailing N days, idempotent upsert).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Sequence

from loguru import logger

from api_clients import MlbApiClient
from etl_scripts import db
from etl_scripts.statcast import get_database_url
from models.mlb_transactions import Transaction


def _connect(database_url: str | None = None):
    """Open a connection on the configured backend (DuckDB if ``ETL_DB_BACKEND=duckdb``)."""
    if db.get_backend() == "duckdb":
        return db.connect()
    import psycopg2

    return psycopg2.connect(database_url or get_database_url())


MLB_TRANSACTIONS_TABLE = "mlb_transactions"
MLB_TRANSACTIONS_CONFLICT_COLUMNS: tuple[str, ...] = ("transaction_id",)

_INSERT_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "person_id",
    "from_team_id",
    "to_team_id",
    "transaction_date",
    "effective_date",
    "resolution_date",
    "type_code",
    "type_desc",
    "description",
)


def _mlb_transactions_ddl() -> str:
    cols = ",\n    ".join(
        [
            '"transaction_id" BIGINT NOT NULL',
            '"person_id" INTEGER',
            '"from_team_id" INTEGER',
            '"to_team_id" INTEGER',
            '"transaction_date" DATE NOT NULL',
            '"effective_date" DATE',
            '"resolution_date" DATE',
            '"type_code" TEXT NOT NULL',
            '"type_desc" TEXT NOT NULL',
            '"description" TEXT',
        ]
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {MLB_TRANSACTIONS_TABLE} (\n"
        f"    {cols},\n"
        f"    PRIMARY KEY (transaction_id)\n"
        f");"
    )


def ensure_mlb_transactions_table(*, database_url: str | None = None) -> None:
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        db.executescript(cur, _mlb_transactions_ddl())
        conn.commit()
    finally:
        conn.close()
    logger.debug("Ensured table {} exists", MLB_TRANSACTIONS_TABLE)


def _row_for_transaction(txn: Transaction) -> dict[str, Any]:
    return {
        "transaction_id": txn.id,
        "person_id": txn.person.id if txn.person else None,
        "from_team_id": txn.from_team.id if txn.from_team else None,
        "to_team_id": txn.to_team.id if txn.to_team else None,
        "transaction_date": txn.date,
        "effective_date": txn.effective_date,
        "resolution_date": txn.resolution_date,
        "type_code": txn.type_code,
        "type_desc": txn.type_desc,
        "description": txn.description,
    }


def _build_upsert_statement() -> str:
    columns = _INSERT_COLUMNS
    conflict_columns = MLB_TRANSACTIONS_CONFLICT_COLUMNS
    update_cols = [c for c in columns if c not in conflict_columns]
    p = db.placeholder()
    fields = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join([p] * len(columns))
    conflict = ", ".join(f'"{c}"' for c in conflict_columns)
    sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    return (
        f'INSERT INTO "{MLB_TRANSACTIONS_TABLE}" ({fields}) VALUES ({placeholders}) '
        f"ON CONFLICT ({conflict}) DO UPDATE SET {sets}"
    )


def _upsert_rows(rows: Sequence[dict[str, Any]], *, database_url: str | None = None) -> int:
    """Upsert ``rows`` on ``transaction_id`` (idempotent: re-runs replace prior values)."""
    if not rows:
        return 0
    ensure_mlb_transactions_table(database_url=database_url)
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        stmt = _build_upsert_statement()
        tuples = [tuple(r[c] for c in _INSERT_COLUMNS) for r in rows]
        db.insert_many(cur, stmt, tuples)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _as_iso(d: date | str) -> str:
    return d if isinstance(d, str) else d.isoformat()


def sync_transactions_for_range(
    start_date: date | str,
    end_date: date | str,
    *,
    team_id: int | None = None,
    sport_id: int | None = None,
    database_url: str | None = None,
    client: MlbApiClient | None = None,
) -> dict[str, Any]:
    """Fetch ``GET /transactions`` for ``[start_date, end_date]`` and upsert the rows.

    Dates accept ``date`` objects or ISO ``YYYY-MM-DD`` strings. The window is
    inclusive on both ends (matches the Stats API).
    """
    start, end = _as_iso(start_date), _as_iso(end_date)
    cl = client or MlbApiClient()
    response = cl.stats.get_transactions(
        start_date=start, end_date=end, team_id=team_id, sport_id=sport_id
    )
    rows = [_row_for_transaction(t) for t in response.transactions]
    written = _upsert_rows(rows, database_url=database_url)
    logger.info(
        "mlb_transactions sync {}..{}: fetched={} rows_written={}",
        start, end, len(response.transactions), written,
    )
    return {"start_date": start, "end_date": end, "fetched": len(rows), "rows_written": written}


def _date_windows(start: date, end: date, chunk_days: int):
    """Yield inclusive ``(window_start, window_end)`` chunks covering ``[start, end]``."""
    cur = start
    step = timedelta(days=max(1, chunk_days) - 1)
    while cur <= end:
        win_end = min(cur + step, end)
        yield cur, win_end
        cur = win_end + timedelta(days=1)


def backfill_transactions(
    start_date: date | str,
    end_date: date | str | None = None,
    *,
    chunk_days: int = 30,
    team_id: int | None = None,
    sport_id: int | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Backfill transactions across ``[start_date, end_date]`` in ``chunk_days`` windows.

    ``end_date`` defaults to today. The range is walked in bounded chunks so a single
    request never has to materialize years of moves at once; each chunk upserts, so
    the backfill is safe to resume after an interruption.
    """
    start = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
    end = (
        date.today()
        if end_date is None
        else (date.fromisoformat(end_date) if isinstance(end_date, str) else end_date)
    )
    if end < start:
        raise ValueError(f"end_date {end} precedes start_date {start}")

    fetched = written = windows = 0
    for win_start, win_end in _date_windows(start, end, chunk_days):
        res = sync_transactions_for_range(
            win_start, win_end, team_id=team_id, sport_id=sport_id, database_url=database_url
        )
        fetched += res["fetched"]
        written += res["rows_written"]
        windows += 1

    summary = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "chunk_days": chunk_days,
        "windows": windows,
        "fetched": fetched,
        "rows_written": written,
    }
    logger.info("mlb_transactions backfill complete: {}", summary)
    return summary


def update_recent_transactions(
    days: int = 7,
    *,
    team_id: int | None = None,
    sport_id: int | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Re-fetch the trailing ``days`` (through today) and upsert.

    Transactions get their ``effectiveDate`` / ``resolutionDate`` filled in or revised
    after the fact, so re-pulling a short trailing window keeps recent rows correct.
    """
    today = date.today()
    start = today - timedelta(days=days)
    res = sync_transactions_for_range(
        start, today, team_id=team_id, sport_id=sport_id, database_url=database_url
    )
    return {"days": days, **res}


def mlb_transactions_table_metrics(*, database_url: str | None = None) -> dict[str, Any]:
    ensure_mlb_transactions_table(database_url=database_url)
    q = f"""
    SELECT
        COUNT(*)::bigint AS row_count,
        MAX(transaction_date) AS max_transaction_date,
        MIN(transaction_date) AS min_transaction_date
    FROM {MLB_TRANSACTIONS_TABLE}
    """
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        cur.execute(q)
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {"row_count": 0, "max_transaction_date": None, "min_transaction_date": None}
    return {
        "row_count": int(row[0]),
        "max_transaction_date": row[1].isoformat() if row[1] else None,
        "min_transaction_date": row[2].isoformat() if row[2] else None,
    }
