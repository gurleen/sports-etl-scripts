"""Prefect flow: MLB Stats API schedule into ``mlb_schedule``."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.mlb_schedule import (
    ensure_mlb_schedule_table,
    mlb_schedule_table_metrics,
    sync_mlb_schedule_for_year,
)
from etl_scripts.prefect_runtime import resolve_database_url_for_flow


@task
def mlb_schedule_sync_task(year: int, database_url: str, *, sport_id: int = 1) -> dict[str, Any]:
    return sync_mlb_schedule_for_year(year, sport_id=sport_id, database_url=database_url)


def _artifact_markdown(
    summary: dict[str, Any],
    before: dict[str, Any],
    after: dict[str, Any],
) -> str:
    delta_rows = int(after["row_count"]) - int(before["row_count"])
    types = summary.get("game_types") or {}
    type_lines = "\n".join(f"- `{k}`: **{v}**" for k, v in sorted(types.items()))
    return "\n".join(
        [
            "# MLB schedule ingest",
            "",
            f"- Season year: **{summary['year']}**",
            f"- Sport ID: **{summary.get('sport_id')}**",
            f"- API ``totalGames``: **{summary.get('api_total_games')}**",
            f"- Schedule date buckets: **{summary.get('schedule_dates')}**",
            f"- Rows written: **{summary.get('rows_written')}**",
            "",
            "## Game types",
            type_lines if type_lines else "_None_",
            "",
            "## Table snapshot (`mlb_schedule`)",
            "| Metric | Before | After |",
            "|--------|--------|-------|",
            f"| row_count | {before['row_count']} | {after['row_count']} |",
            f"| min_official_date | {before['min_official_date']} | {after['min_official_date']} |",
            f"| max_official_date | {before['max_official_date']} | {after['max_official_date']} |",
            f"| **delta row_count** | | **{delta_rows:+d}** |",
        ]
    )


@flow(name="mlb-schedule-ingest-year", log_prints=True)
def mlb_schedule_ingest_year_flow(
    year: int | None = None,
    sport_id: int = 1,
) -> dict[str, Any]:
    """
    Fetch all MLB schedule games for ``year`` (Stats API ``season``) and load ``mlb_schedule``.

    Replaces existing rows for that ``season_year`` on each run.
    """
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    y = year if year is not None else datetime.now().year
    ensure_mlb_schedule_table(database_url=database_url)
    before = mlb_schedule_table_metrics(database_url=database_url)
    summary = mlb_schedule_sync_task(y, database_url, sport_id=sport_id)
    after = mlb_schedule_table_metrics(database_url=database_url)
    create_markdown_artifact(
        key="mlb-schedule-run-summary",
        markdown=_artifact_markdown(summary, before, after),
    )
    log.info("mlb_schedule summary: %s before=%s after=%s", summary, before, after)
    return {"before": before, "after": after, "sync": summary}
