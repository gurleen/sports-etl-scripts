"""Prefect flows: MLB Stats API transactions into ``mlb_transactions``."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.mlb_transactions import (
    backfill_transactions,
    ensure_mlb_transactions_table,
    mlb_transactions_table_metrics,
    update_recent_transactions,
)
from etl_scripts.prefect_runtime import resolve_database_url_for_flow


@task
def transactions_backfill_task(
    start_date: str, end_date: str | None, chunk_days: int, database_url: str
) -> dict[str, Any]:
    return backfill_transactions(
        start_date, end_date, chunk_days=chunk_days, database_url=database_url
    )


@task
def transactions_update_recent_task(days: int, database_url: str) -> dict[str, Any]:
    return update_recent_transactions(days=days, database_url=database_url)


def _artifact_markdown(
    title: str,
    summary: dict[str, Any],
    before: dict[str, Any],
    after: dict[str, Any],
) -> str:
    delta_rows = int(after["row_count"]) - int(before["row_count"])
    summary_lines = "\n".join(f"- {k}: **{v}**" for k, v in summary.items())
    return "\n".join(
        [
            f"# {title}",
            "",
            "## Run summary",
            summary_lines,
            "",
            "## Table snapshot (`mlb_transactions`)",
            "| Metric | Before | After |",
            "|--------|--------|-------|",
            f"| row_count | {before['row_count']} | {after['row_count']} |",
            f"| min_transaction_date | {before['min_transaction_date']} | {after['min_transaction_date']} |",
            f"| max_transaction_date | {before['max_transaction_date']} | {after['max_transaction_date']} |",
            f"| **delta row_count** | | **{delta_rows:+d}** |",
        ]
    )


@flow(name="mlb-transactions-update-recent", log_prints=True)
def mlb_transactions_update_recent(days: int = 7) -> dict[str, Any]:
    """Re-fetch the trailing ``days`` of transactions and upsert into ``mlb_transactions``."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    ensure_mlb_transactions_table(database_url=database_url)
    before = mlb_transactions_table_metrics(database_url=database_url)
    summary = transactions_update_recent_task(days, database_url)
    after = mlb_transactions_table_metrics(database_url=database_url)
    create_markdown_artifact(
        key="mlb-transactions-update-recent-summary",
        markdown=_artifact_markdown("MLB transactions update-recent", summary, before, after),
    )
    log.info("mlb_transactions update-recent: %s before=%s after=%s", summary, before, after)
    return {"before": before, "after": after, "sync": summary}


@flow(name="mlb-transactions-backfill", log_prints=True)
def mlb_transactions_backfill_flow(
    start_date: str,
    end_date: str | None = None,
    chunk_days: int = 30,
) -> dict[str, Any]:
    """Backfill transactions across ``[start_date, end_date]`` (end defaults to today)."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    ensure_mlb_transactions_table(database_url=database_url)
    before = mlb_transactions_table_metrics(database_url=database_url)
    summary = transactions_backfill_task(start_date, end_date, chunk_days, database_url)
    after = mlb_transactions_table_metrics(database_url=database_url)
    create_markdown_artifact(
        key="mlb-transactions-backfill-summary",
        markdown=_artifact_markdown("MLB transactions backfill", summary, before, after),
    )
    log.info("mlb_transactions backfill: %s before=%s after=%s", summary, before, after)
    return {"before": before, "after": after, "sync": summary}
