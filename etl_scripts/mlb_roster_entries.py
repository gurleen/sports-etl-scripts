"""Load MLB Stats API roster stints into ``mlb_roster_entries``.

Source: ``GET /people/{personId}?hydrate=rosterEntries``
(``MlbApiClient.stats.get_person``). Each ``rosterEntries[]`` item is already a
*stint* — an interval of one player's membership on one team under one status —
so no daily-snapshot collapse is needed: we store one row per
``(person_id, team_id, start_date, status_code)``.

``person`` / ``team`` / ``parent_org`` are stored as bare integer ids (consistent
with ``mlb_transactions``); the human names live in the teams/players dimensions.

This is the authoritative *daily membership* side of MLB service-time math: the
companion ``mlb_transactions`` table provides the *why* (option / IL / DFA),
while these stints provide *who was on which roster, when*. Service time accrues
for active-26 days plus MLB-IL days, capped at 172/season.

Drivers mirror the transactions loader: a per-person sync primitive plus
``backfill_roster_entries`` (walk the whole person universe) and
``update_recent_roster_entries`` (re-fetch people touched recently + open stints).
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable, Sequence

from loguru import logger

from api_clients import MlbApiClient
from etl_scripts import db
from etl_scripts.statcast import get_database_url
from models.mlb_roster_entries import RosterEntry


def _connect(database_url: str | None = None):
    """Open a connection on the configured backend (DuckDB if ``ETL_DB_BACKEND=duckdb``)."""
    if db.get_backend() == "duckdb":
        return db.connect()
    import psycopg2

    return psycopg2.connect(database_url or get_database_url())


MLB_ROSTER_ENTRIES_TABLE = "mlb_roster_entries"
MLB_ROSTER_ENTRIES_CONFLICT_COLUMNS: tuple[str, ...] = (
    "person_id",
    "team_id",
    "start_date",
    "status_code",
)

# Companion table that defines the universe of players we scrape stints for.
PERSON_SOURCE_TABLE = "mlb_transactions"

_INSERT_COLUMNS: tuple[str, ...] = (
    "person_id",
    "team_id",
    "parent_org_id",
    "jersey_number",
    "position_code",
    "position_abbreviation",
    "status_code",
    "status_desc",
    "is_active",
    "is_active_forty_man",
    "start_date",
    "end_date",
    "status_date",
)


def _mlb_roster_entries_ddl() -> str:
    cols = ",\n    ".join(
        [
            '"person_id" INTEGER NOT NULL',
            '"team_id" INTEGER NOT NULL',
            '"parent_org_id" INTEGER',
            '"jersey_number" TEXT',
            '"position_code" TEXT',
            '"position_abbreviation" TEXT',
            '"status_code" TEXT NOT NULL',
            '"status_desc" TEXT',
            '"is_active" BOOLEAN',
            '"is_active_forty_man" BOOLEAN',
            '"start_date" DATE NOT NULL',
            '"end_date" DATE',
            '"status_date" DATE',
        ]
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {MLB_ROSTER_ENTRIES_TABLE} (\n"
        f"    {cols},\n"
        f"    PRIMARY KEY (person_id, team_id, start_date, status_code)\n"
        f");"
    )


def ensure_mlb_roster_entries_table(*, database_url: str | None = None) -> None:
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        db.executescript(cur, _mlb_roster_entries_ddl())
        conn.commit()
    finally:
        conn.close()
    logger.debug("Ensured table {} exists", MLB_ROSTER_ENTRIES_TABLE)


def _row_for_entry(person_id: int, entry: RosterEntry) -> dict[str, Any]:
    return {
        "person_id": person_id,
        "team_id": entry.team.id,
        "parent_org_id": entry.team.parent_org_id,
        "jersey_number": entry.jersey_number,
        "position_code": entry.position.code if entry.position else None,
        "position_abbreviation": entry.position.abbreviation if entry.position else None,
        "status_code": entry.status.code,
        "status_desc": entry.status.description,
        "is_active": entry.is_active,
        "is_active_forty_man": entry.is_active_forty_man,
        "start_date": entry.start_date,
        "end_date": entry.end_date,
        "status_date": entry.status_date,
    }


def _build_upsert_statement() -> str:
    columns = _INSERT_COLUMNS
    conflict_columns = MLB_ROSTER_ENTRIES_CONFLICT_COLUMNS
    update_cols = [c for c in columns if c not in conflict_columns]
    p = db.placeholder()
    fields = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join([p] * len(columns))
    conflict = ", ".join(f'"{c}"' for c in conflict_columns)
    sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    return (
        f'INSERT INTO "{MLB_ROSTER_ENTRIES_TABLE}" ({fields}) VALUES ({placeholders}) '
        f"ON CONFLICT ({conflict}) DO UPDATE SET {sets}"
    )


def _dedupe_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop rows that collide on the conflict key (last one wins).

    ``executemany``/``execute_batch`` cannot resolve a conflict between two rows
    inside the *same* batch, so we collapse duplicates here before upserting.
    """
    by_key: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        by_key[tuple(r[c] for c in MLB_ROSTER_ENTRIES_CONFLICT_COLUMNS)] = r
    return list(by_key.values())


def _upsert_rows(rows: Sequence[dict[str, Any]], *, database_url: str | None = None) -> int:
    """Upsert ``rows`` on ``(person_id, team_id, start_date, status_code)`` (idempotent)."""
    if not rows:
        return 0
    deduped = _dedupe_rows(rows)
    ensure_mlb_roster_entries_table(database_url=database_url)
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        stmt = _build_upsert_statement()
        tuples = [tuple(r[c] for c in _INSERT_COLUMNS) for r in deduped]
        db.insert_many(cur, stmt, tuples)
        conn.commit()
    finally:
        conn.close()
    return len(deduped)


def _fetch_person_rows(person_id: int, client: MlbApiClient) -> list[dict[str, Any]]:
    """Fetch one person's roster stints as insert-ready rows (no DB access).

    Kept side-effect-free so it can run on a worker thread; the DB write happens
    on the calling thread.
    """
    response = client.stats.get_person(person_id, hydrate="rosterEntries")
    entries: list[RosterEntry] = response.people[0].roster_entries if response.people else []
    return [_row_for_entry(person_id, e) for e in entries]


def sync_roster_entries_for_person(
    person_id: int,
    *,
    database_url: str | None = None,
    client: MlbApiClient | None = None,
) -> dict[str, Any]:
    """Fetch one person's roster stints and upsert them.

    Returns ``{"person_id", "fetched", "rows_written"}``. A person with no roster
    history (or an unknown id) simply writes zero rows.
    """
    cl = client or MlbApiClient()
    rows = _fetch_person_rows(person_id, cl)
    written = _upsert_rows(rows, database_url=database_url)
    logger.debug(
        "mlb_roster_entries person {}: fetched={} rows_written={}",
        person_id, len(rows), written,
    )
    return {"person_id": person_id, "fetched": len(rows), "rows_written": written}


def _query_ids(sql: str, *, database_url: str | None = None) -> list[int]:
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        cur.execute(sql)
        return [int(r[0]) for r in cur.fetchall() if r[0] is not None]
    finally:
        conn.close()


def distinct_person_ids(
    *,
    source_table: str = PERSON_SOURCE_TABLE,
    person_column: str = "person_id",
    database_url: str | None = None,
) -> list[int]:
    """Distinct person ids to scrape, drawn from the companion transactions table."""
    sql = (
        f"SELECT DISTINCT {person_column} FROM {source_table} "
        f"WHERE {person_column} IS NOT NULL ORDER BY {person_column}"
    )
    return _query_ids(sql, database_url=database_url)


def _person_ids_in_recent_transactions(days: int, *, database_url: str | None = None) -> list[int]:
    # ``days`` is cast to int below, so interpolating it into the interval is safe.
    sql = (
        f"SELECT DISTINCT person_id FROM {PERSON_SOURCE_TABLE} "
        f"WHERE person_id IS NOT NULL "
        f"AND transaction_date >= CURRENT_DATE - INTERVAL '{int(days)}' DAY"
    )
    return _query_ids(sql, database_url=database_url)


def _existing_person_ids(*, database_url: str | None = None) -> set[int]:
    """Person ids already present in ``mlb_roster_entries`` (for resumable backfills)."""
    sql = f"SELECT DISTINCT person_id FROM {MLB_ROSTER_ENTRIES_TABLE}"
    try:
        return set(_query_ids(sql, database_url=database_url))
    except Exception:
        return set()


def _person_ids_with_open_stints(*, database_url: str | None = None) -> list[int]:
    sql = (
        f"SELECT DISTINCT person_id FROM {MLB_ROSTER_ENTRIES_TABLE} "
        f"WHERE end_date IS NULL"
    )
    try:
        return _query_ids(sql, database_url=database_url)
    except Exception:
        # Table may not exist yet on a first run.
        return []


def _sync_people(
    person_ids: Sequence[int],
    *,
    pause_sec: float,
    max_workers: int,
    batch_people: int = 200,
    database_url: str | None,
    client: MlbApiClient | None,
) -> dict[str, Any]:
    """Fetch each person's stints concurrently, flushing upserts in batches.

    The HTTP fetch (I/O-bound) runs on a thread pool of ``max_workers``; rows are
    accumulated and upserted on the main thread every ``batch_people`` people, so
    we open far fewer connections than one-per-person. One bad id never aborts the
    run. ``pause_sec`` adds a per-request delay inside each worker to throttle the
    aggregate request rate.
    """
    cl = client or MlbApiClient()
    fetched = written = people = 0
    failed: list[int] = []

    def fetch(pid: int) -> tuple[int, list[dict[str, Any]]]:
        rows = _fetch_person_rows(pid, cl)
        if pause_sec:
            time.sleep(pause_sec)
        return pid, rows

    buffer: list[dict[str, Any]] = []
    pending_people = 0

    def flush() -> None:
        nonlocal written, pending_people
        if buffer:
            written += _upsert_rows(buffer, database_url=database_url)
            buffer.clear()
        pending_people = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, pid): pid for pid in person_ids}
        for future in as_completed(futures):
            pid = futures[future]
            try:
                _, rows = future.result()
            except Exception as exc:  # one bad id shouldn't abort a long backfill
                failed.append(pid)
                logger.warning("mlb_roster_entries person {} failed: {}", pid, exc)
                continue
            fetched += len(rows)
            people += 1
            pending_people += 1
            buffer.extend(rows)
            if pending_people >= batch_people:
                flush()
        flush()

    return {
        "people": people,
        "fetched": fetched,
        "rows_written": written,
        "failed": len(failed),
        "failed_ids": failed,
    }


def backfill_roster_entries(
    person_ids: Iterable[int] | None = None,
    *,
    pause_sec: float = 0.0,
    max_workers: int = 8,
    limit: int | None = None,
    skip_existing: bool = False,
    source_table: str = PERSON_SOURCE_TABLE,
    database_url: str | None = None,
    client: MlbApiClient | None = None,
) -> dict[str, Any]:
    """Scrape roster stints for the whole person universe (one API call per person).

    ``person_ids`` defaults to the distinct ids in ``source_table`` (the companion
    ``mlb_transactions`` table). Each person upserts independently, so the backfill
    is safe to resume after an interruption. ``limit`` caps the number of people
    (handy for smoke tests); ``pause_sec`` throttles the API.

    ``skip_existing=True`` drops person ids already present in ``mlb_roster_entries``
    so a re-run resumes where an interrupted backfill left off instead of re-pulling
    the whole universe.
    """
    ensure_mlb_roster_entries_table(database_url=database_url)
    ids = (
        list(person_ids)
        if person_ids is not None
        else distinct_person_ids(source_table=source_table, database_url=database_url)
    )
    requested = len(ids)
    skipped = 0
    if skip_existing:
        done = _existing_person_ids(database_url=database_url)
        ids = [pid for pid in ids if pid not in done]
        skipped = requested - len(ids)
    if limit is not None:
        ids = ids[:limit]
    summary = _sync_people(
        ids, pause_sec=pause_sec, max_workers=max_workers, database_url=database_url, client=client
    )
    summary = {"people_requested": requested, "people_skipped": skipped, "people_total": len(ids), **summary}
    logger.info("mlb_roster_entries backfill complete: {}", {k: v for k, v in summary.items() if k != "failed_ids"})
    return summary


def update_recent_roster_entries(
    days: int = 7,
    *,
    pause_sec: float = 0.0,
    max_workers: int = 8,
    database_url: str | None = None,
    client: MlbApiClient | None = None,
) -> dict[str, Any]:
    """Re-fetch people whose stints likely changed: recent transactions + open stints.

    A player's current stint comes back with ``end_date = NULL``; once it closes
    the API fills the date in. So we refresh (a) anyone who appears in the trailing
    ``days`` of ``mlb_transactions`` and (b) anyone we currently hold an open stint
    for, then upsert. Bounded and idempotent.
    """
    ensure_mlb_roster_entries_table(database_url=database_url)
    recent = _person_ids_in_recent_transactions(days, database_url=database_url)
    open_stints = _person_ids_with_open_stints(database_url=database_url)
    ids = sorted(set(recent) | set(open_stints))
    summary = _sync_people(
        ids, pause_sec=pause_sec, max_workers=max_workers, database_url=database_url, client=client
    )
    summary = {
        "days": days,
        "from_recent_transactions": len(recent),
        "from_open_stints": len(open_stints),
        "people_total": len(ids),
        **summary,
    }
    logger.info(
        "mlb_roster_entries update-recent complete: {}",
        {k: v for k, v in summary.items() if k != "failed_ids"},
    )
    return summary


def mlb_roster_entries_table_metrics(*, database_url: str | None = None) -> dict[str, Any]:
    ensure_mlb_roster_entries_table(database_url=database_url)
    q = f"""
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT person_id)::bigint AS person_count,
        MAX(start_date) AS max_start_date,
        MIN(start_date) AS min_start_date
    FROM {MLB_ROSTER_ENTRIES_TABLE}
    """
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        cur.execute(q)
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {
            "row_count": 0,
            "person_count": 0,
            "max_start_date": None,
            "min_start_date": None,
        }
    return {
        "row_count": int(row[0]),
        "person_count": int(row[1]),
        "max_start_date": row[2].isoformat() if row[2] else None,
        "min_start_date": row[3].isoformat() if row[3] else None,
    }
