"""Warehouse materialized view refresh helpers."""

from __future__ import annotations

import re
from typing import Sequence

import psycopg2
from psycopg2 import sql

# Expand this tuple when new derived views should be refreshed by default.
DEFAULT_MATERIALIZED_VIEWS: tuple[str, ...] = (
    "current_season_batting_stats",
)

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def refresh_materialized_views(database_url: str, view_names: Sequence[str]) -> list[str]:
    """Run ``REFRESH MATERIALIZED VIEW`` for each name. Names must be plain identifiers."""
    validated: list[str] = []
    for name in view_names:
        if not _IDENT.fullmatch(name):
            raise ValueError(f"Invalid materialized view name: {name!r}")
        validated.append(name)
    if not validated:
        return []

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            for name in validated:
                stmt = sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(name))
                cur.execute(stmt)
        conn.commit()
    return validated
