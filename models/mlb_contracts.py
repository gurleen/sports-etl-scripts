"""Pydantic model for one player contract row parsed from a team payroll spreadsheet.

Source: the ``MLB <Team> <YY>`` payroll CSVs (a Cot's-style sheet) where each player
occupies one row but money is split across a header-y grid of Labor Relations and
Competitive Balance columns for 2026..2030. :class:`PlayerContract` is the flattened,
one-row-per-player shape the parser in :mod:`etl_scripts.mlb_contracts` emits.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PlayerContract(BaseModel):
    """A single player's contract line, normalized to plain dollars.

    ``salary_*`` are the Labor Relations (year-end payroll) figures; ``cbt_*`` are the
    Competitive Balance Tax figures — both in whole US dollars (negative for credits).
    A cell that held a status code instead of money (``FA``, ``A1``..``A4``) is left
    ``None`` here and recorded in :attr:`notes` so nothing is lost.
    """

    model_config = ConfigDict(extra="ignore")

    source_file: str
    source_row: int

    team_city: str | None = None
    team_name: str | None = None

    player_name: str
    last_name: str | None = None
    first_name: str | None = None
    position: str | None = None

    draft_year: int | None = None
    draft_round: str | None = None
    draft_pick: str | None = None

    age: int | None = None
    service_time: str | None = None
    options_left: str | None = None
    agent: str | None = None

    contract: str | None = None
    roster_status: str = "active"

    salary_2026: int | None = None
    salary_2027: int | None = None
    salary_2028: int | None = None
    salary_2029: int | None = None
    salary_2030: int | None = None

    cbt_2026: int | None = None
    cbt_2027: int | None = None
    cbt_2028: int | None = None
    cbt_2029: int | None = None
    cbt_2030: int | None = None

    notes: str | None = None
