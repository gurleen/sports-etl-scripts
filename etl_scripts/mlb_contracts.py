"""Parse team payroll CSVs into one clean row per player contract.

The ``MLB <Team> <YY>`` exports are Cot's-style sheets: a few banner rows, a wide
double-header (Labor Relations vs. Competitive Balance, 2026..2030), then one row per
player, then aggregate / notes rows at the bottom. This module pulls out just the
player rows, normalizes the money to whole dollars, and emits :class:`PlayerContract`
records that can be written back out as a tidy CSV or upserted into ``mlb_contracts``.

Follows the shape of :mod:`etl_scripts.mlb_transactions`: a parse primitive plus a
table DDL + idempotent upsert keyed on ``(source_file, source_row)``.
"""

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

from loguru import logger

from etl_scripts import db
from etl_scripts.statcast import get_database_url
from models.mlb_contracts import PlayerContract

# Column offsets within a player row (0-based). The sheet keeps two blank spacer
# columns (9 and 11/17) between logical blocks; we read around them.
COL_PLAYER = 0
COL_POS = 1
COL_YEAR = 2
COL_ROUND = 3
COL_PICK = 4
COL_AGE = 5
COL_MLS = 6
COL_OPTS = 7
COL_AGENT = 8
COL_CONTRACT = 10
COL_LR = (12, 13, 14, 15, 16)  # Labor Relations 2026..2030
COL_CB = (18, 19, 20, 21, 22)  # Competitive Balance 2026..2030
SEASONS = (2026, 2027, 2028, 2029, 2030)

# Status codes that can appear in a money cell instead of a dollar figure.
_STATUS_CODES = {"FA", "A1", "A2", "A3", "A4"}

# Keywords in the contract column that mean "not an active contract".
_STATUS_KEYWORDS = {
    "released": "released",
    "traded": "traded",
    "outrighted": "outrighted",
    "claimed": "claimed",
    "credit": "credit",
    "susp": "suspended",
    "dfa": "dfa",
}

# A player row's first cell looks like "Last, First" — the last name is a single token
# (letters, accents, and . ' - *), so spaces before the comma are disallowed. That
# excludes aggregate rows ("Pre-arbitration bonus pool", no comma), note/history rows
# (start with a digit or "*"), and prose footnotes ("Before the 2022 season, ...").
_NAME_RE = re.compile(r"^[A-Za-zÀ-ÿ][\w.'\-*À-ÿ]*,\s+\S")


def _clean(cell: str | None) -> str:
    return (cell or "").strip()


def _get(row: Sequence[str], idx: int) -> str:
    return _clean(row[idx]) if idx < len(row) else ""


def _parse_money(token: str) -> tuple[int | None, str | None]:
    """Normalize one money cell to whole dollars.

    Returns ``(dollars, note)``. Money becomes an int; a status code (``FA``, ``A1``..)
    comes back as ``(None, code)``; a blank cell is ``(None, None)``.

    Formats in the sheet:
      * full dollars use commas, no decimal:    ``$42,000,000`` -> 42000000
      * millions use a 3-place decimal, no comma: ``$42.000``  -> 42000000
      * parentheses denote a negative (credit):  ``($2.375)`` -> -2375000
    """
    t = token.strip()
    if not t:
        return None, None
    if t.upper() in _STATUS_CODES:
        return None, t.upper()

    negative = t.startswith("(") and t.endswith(")")
    t = t.strip("()").replace("$", "").strip()
    if not t:
        return None, None

    if "," in t:  # full-dollar format
        digits = t.replace(",", "")
        if not re.fullmatch(r"\d+", digits):
            return None, token.strip()
        value = int(digits)
    elif re.fullmatch(r"\d+(\.\d+)?", t):  # millions (decimal) or bare integer dollars
        value = int(round(float(t) * 1_000_000)) if "." in t else int(t)
    else:
        return None, token.strip()  # unrecognized -> keep raw as a note

    return (-value if negative else value), None


def _roster_status(contract: str) -> str:
    low = contract.lower()
    for needle, status in _STATUS_KEYWORDS.items():
        if needle in low:
            return status
    return "active"


def _team_from_banner(rows: list[list[str]]) -> tuple[str | None, str | None]:
    """City + nickname from the top banner (e.g. ``PHILADELPHIA`` / ``PHILLIES``)."""
    labels = [_get(r, 0) for r in rows[:6]]
    labels = [x for x in labels if x and x not in {",", " "}]
    city = labels[0] if labels else None
    name = labels[1] if len(labels) > 1 else None
    return city, name


def parse_file(path: str | os.PathLike[str]) -> list[PlayerContract]:
    """Parse one payroll CSV into a list of :class:`PlayerContract` rows."""
    p = Path(path)
    with p.open(newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    city, name = _team_from_banner(rows)
    contracts: list[PlayerContract] = []

    for i, row in enumerate(rows, start=1):
        player = _get(row, COL_PLAYER)
        if not _NAME_RE.match(player):
            continue

        last, _, first = player.partition(",")
        notes: list[str] = []

        salaries: dict[str, int | None] = {}
        for season, col in zip(SEASONS, COL_LR):
            val, note = _parse_money(_get(row, col))
            salaries[f"salary_{season}"] = val
            if note:
                notes.append(f"LR {season}: {note}")

        cbt: dict[str, int | None] = {}
        for season, col in zip(SEASONS, COL_CB):
            val, note = _parse_money(_get(row, col))
            cbt[f"cbt_{season}"] = val
            if note:
                notes.append(f"CB {season}: {note}")

        contract = _get(row, COL_CONTRACT) or None
        contracts.append(
            PlayerContract(
                source_file=p.name,
                source_row=i,
                team_city=city,
                team_name=name,
                player_name=player,
                last_name=last.strip() or None,
                first_name=first.strip() or None,
                position=_get(row, COL_POS) or None,
                draft_year=int(y) if (y := _get(row, COL_YEAR)).isdigit() else None,
                draft_round=_get(row, COL_ROUND) or None,
                draft_pick=_get(row, COL_PICK) or None,
                age=int(a) if (a := _get(row, COL_AGE)).isdigit() else None,
                service_time=_get(row, COL_MLS) or None,
                options_left=_get(row, COL_OPTS) or None,
                agent=_get(row, COL_AGENT) or None,
                contract=contract,
                roster_status=_roster_status(contract or ""),
                notes="; ".join(notes) or None,
                **salaries,
                **cbt,
            )
        )

    logger.info("Parsed {} player contracts from {}", len(contracts), p.name)
    return contracts


def parse_files(paths: Iterable[str | os.PathLike[str]]) -> list[PlayerContract]:
    out: list[PlayerContract] = []
    for path in paths:
        out.extend(parse_file(path))
    return out


# --- CSV output ------------------------------------------------------------------

CSV_COLUMNS: tuple[str, ...] = tuple(PlayerContract.model_fields.keys())


def to_csv_rows(contracts: Sequence[PlayerContract]) -> list[dict[str, Any]]:
    return [c.model_dump() for c in contracts]


def write_csv(contracts: Sequence[PlayerContract], out_path: str | os.PathLike[str]) -> None:
    with Path(out_path).open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(to_csv_rows(contracts))
    logger.info("Wrote {} rows -> {}", len(contracts), out_path)


# --- Database load ---------------------------------------------------------------

MLB_CONTRACTS_TABLE = "mlb_contracts"
MLB_CONTRACTS_CONFLICT_COLUMNS: tuple[str, ...] = ("source_file", "source_row")

_INSERT_COLUMNS: tuple[str, ...] = CSV_COLUMNS


def _connect(database_url: str | None = None):
    if db.get_backend() == "duckdb":
        return db.connect()
    import psycopg2

    return psycopg2.connect(database_url or get_database_url())


def _mlb_contracts_ddl() -> str:
    text_cols = (
        "team_city", "team_name", "player_name", "last_name", "first_name",
        "position", "draft_round", "draft_pick", "service_time", "options_left",
        "agent", "contract", "roster_status", "notes",
    )
    int_cols = (
        "draft_year", "age",
        "salary_2026", "salary_2027", "salary_2028", "salary_2029", "salary_2030",
        "cbt_2026", "cbt_2027", "cbt_2028", "cbt_2029", "cbt_2030",
    )
    defs = ['"source_file" TEXT NOT NULL', '"source_row" INTEGER NOT NULL']
    defs += [f'"{c}" TEXT' for c in text_cols]
    defs += [f'"{c}" BIGINT' for c in int_cols]
    cols = ",\n    ".join(defs)
    return (
        f"CREATE TABLE IF NOT EXISTS {MLB_CONTRACTS_TABLE} (\n"
        f"    {cols},\n"
        f"    PRIMARY KEY (source_file, source_row)\n"
        f");"
    )


def ensure_mlb_contracts_table(*, database_url: str | None = None) -> None:
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        db.executescript(cur, _mlb_contracts_ddl())
        conn.commit()
    finally:
        conn.close()
    logger.debug("Ensured table {} exists", MLB_CONTRACTS_TABLE)


def _build_upsert_statement() -> str:
    columns = _INSERT_COLUMNS
    conflict = MLB_CONTRACTS_CONFLICT_COLUMNS
    update_cols = [c for c in columns if c not in conflict]
    p = db.placeholder()
    fields = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join([p] * len(columns))
    conflict_sql = ", ".join(f'"{c}"' for c in conflict)
    sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    return (
        f'INSERT INTO "{MLB_CONTRACTS_TABLE}" ({fields}) VALUES ({placeholders}) '
        f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {sets}"
    )


def load_contracts(
    contracts: Sequence[PlayerContract], *, database_url: str | None = None
) -> int:
    """Upsert ``contracts`` on ``(source_file, source_row)`` (idempotent re-runs)."""
    if not contracts:
        return 0
    ensure_mlb_contracts_table(database_url=database_url)
    conn = _connect(database_url)
    try:
        cur = db.cursor(conn)
        stmt = _build_upsert_statement()
        tuples = [
            tuple(getattr(c, col) for col in _INSERT_COLUMNS) for c in contracts
        ]
        db.insert_many(cur, stmt, tuples)
        conn.commit()
    finally:
        conn.close()
    logger.info("Upserted {} rows into {}", len(contracts), MLB_CONTRACTS_TABLE)
    return len(contracts)
