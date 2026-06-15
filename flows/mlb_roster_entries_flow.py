"""Prefect flows: MLB Stats API roster stints into ``mlb_roster_entries``."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.mlb_roster_entries import (
    backfill_roster_entries,
    ensure_mlb_roster_entries_table,
    mlb_roster_entries_table_metrics,
    update_recent_roster_entries,
)
from etl_scripts.prefect_runtime import resolve_database_url_for_flow


@task
def roster_entries_backfill_task(
    limit: int | None,
    max_workers: int,
    pause_sec: float,
    skip_existing: bool,
    source_table: str,
    database_url: str,
) -> dict[str, Any]:
    return backfill_roster_entries(
        pause_sec=pause_sec,
        max_workers=max_workers,
        limit=limit,
        skip_existing=skip_existing,
        source_table=source_table,
        database_url=database_url,
    )


@task
def roster_entries_update_recent_task(
    days: int, max_workers: int, pause_sec: float, database_url: str
) -> dict[str, Any]:
    return update_recent_roster_entries(
        days=days, max_workers=max_workers, pause_sec=pause_sec, database_url=database_url
    )


def _artifact_markdown(
    title: str,
    summary: dict[str, Any],
    before: dict[str, Any],
    after: dict[str, Any],
) -> str:
    delta_rows = int(after["row_count"]) - int(before["row_count"])
    shown = {k: v for k, v in summary.items() if k != "failed_ids"}
    summary_lines = "\n".join(f"- {k}: **{v}**" for k, v in shown.items())
    return "\n".join(
        [
            f"# {title}",
            "",
            "## Run summary",
            summary_lines,
            "",
            "## Table snapshot (`mlb_roster_entries`)",
            "| Metric | Before | After |",
            "|--------|--------|-------|",
            f"| row_count | {before['row_count']} | {after['row_count']} |",
            f"| person_count | {before['person_count']} | {after['person_count']} |",
            f"| min_start_date | {before['min_start_date']} | {after['min_start_date']} |",
            f"| max_start_date | {before['max_start_date']} | {after['max_start_date']} |",
            f"| **delta row_count** | | **{delta_rows:+d}** |",
        ]
    )


@flow(name="mlb-roster-entries-update-recent", log_prints=True)
def mlb_roster_entries_update_recent(
    days: int = 7, max_workers: int = 8, pause_sec: float = 0.0
) -> dict[str, Any]:
    """Refresh recently-moved players + open stints into ``mlb_roster_entries``."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    ensure_mlb_roster_entries_table(database_url=database_url)
    before = mlb_roster_entries_table_metrics(database_url=database_url)
    summary = roster_entries_update_recent_task(days, max_workers, pause_sec, database_url)
    after = mlb_roster_entries_table_metrics(database_url=database_url)
    create_markdown_artifact(
        key="mlb-roster-entries-update-recent-summary",
        markdown=_artifact_markdown("MLB roster entries update-recent", summary, before, after),
    )
    log.info("mlb_roster_entries update-recent: %s before=%s after=%s", summary, before, after)
    return {"before": before, "after": after, "sync": summary}


@flow(name="mlb-roster-entries-backfill", log_prints=True)
def mlb_roster_entries_backfill_flow(
    limit: int | None = None,
    max_workers: int = 8,
    pause_sec: float = 0.0,
    skip_existing: bool = False,
    source_table: str = "mlb_transactions",
) -> dict[str, Any]:
    """Backfill roster stints for every person in the companion transactions table."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    ensure_mlb_roster_entries_table(database_url=database_url)
    before = mlb_roster_entries_table_metrics(database_url=database_url)
    summary = roster_entries_backfill_task(
        limit, max_workers, pause_sec, skip_existing, source_table, database_url
    )
    after = mlb_roster_entries_table_metrics(database_url=database_url)
    create_markdown_artifact(
        key="mlb-roster-entries-backfill-summary",
        markdown=_artifact_markdown("MLB roster entries backfill", summary, before, after),
    )
    log.info("mlb_roster_entries backfill: %s before=%s after=%s", summary, before, after)
    return {"before": before, "after": after, "sync": summary}
