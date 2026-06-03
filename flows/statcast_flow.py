"""Prefect flows for Statcast ETL (UI visibility, run artifacts, task boundaries)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.prefect_runtime import resolve_database_url_for_flow
from etl_scripts.statcast import EARLIEST_DATA_DATE, get_statcast_data, load_data_to_db, statcast_table_metrics
from flows.materialized_views_flow import refresh_materialized_views_flow
from flows.statcast_extra_flow import statcast_extra_ingest_year_flow


def _coerce_game_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _datetime_range_for_game_date(game_date: date | str) -> tuple[datetime, datetime]:
    d = _coerce_game_date(game_date)
    day = datetime.combine(d, time.min)
    return day, day


@task
def statcast_snapshot_metrics_task(database_url: str) -> dict[str, Any]:
    return statcast_table_metrics(database_url)


@task
def statcast_ingest_range_task(
    start_date: datetime,
    end_date: datetime,
    database_url: str,
) -> dict[str, Any]:
    """Fetch and load without persisting large row payloads as a task result."""
    log = get_run_logger()
    data = get_statcast_data(start_date, end_date)
    n = len(data)
    written = load_data_to_db(data, database_url=database_url)
    log.info("Ingest complete: rows_fetched=%s rows_written=%s", n, written)
    return {
        "rows_fetched": n,
        "rows_written": written,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def _artifact_markdown(
    job: str,
    before: dict[str, Any],
    after: dict[str, Any],
    ingest: dict[str, Any],
) -> str:
    delta_rows = int(after["row_count"]) - int(before["row_count"])
    return "\n".join(
        [
            f"# Statcast run: `{job}`",
            "",
            "## Ingest",
            f"- Date range: `{ingest['start_date']}` → `{ingest['end_date']}`",
            f"- Rows fetched: **{ingest['rows_fetched']}**",
            f"- Rows written (driver report): **{ingest['rows_written']}**",
            "",
            "## Table snapshot (`statcast`)",
            "| Metric | Before | After |",
            "|--------|--------|-------|",
            f"| row_count | {before['row_count']} | {after['row_count']} |",
            f"| max_game_date | {before['max_game_date']} | {after['max_game_date']} |",
            f"| **delta row_count** | | **{delta_rows:+d}** |",
        ]
    )


@flow(name="statcast-update-recent", log_prints=True)
def statcast_update_recent(days: int = 1) -> dict[str, Any]:
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    today = datetime.today()
    start = today - timedelta(days=days)
    before = statcast_snapshot_metrics_task(database_url)
    ingest = statcast_ingest_range_task(start, today, database_url)
    after = statcast_snapshot_metrics_task(database_url)
    create_markdown_artifact(
        key="statcast-run-summary",
        markdown=_artifact_markdown("update_recent", before, after, ingest),
    )
    statcast_extra = statcast_extra_ingest_year_flow(
        year=today.year,
        start_date=start.date(),
        end_date=today.date(),
    )
    materialized_views = refresh_materialized_views_flow()
    log.info(
        "Run summary: before=%s after=%s ingest=%s statcast_extra=%s materialized_views=%s",
        before,
        after,
        ingest,
        statcast_extra,
        materialized_views,
    )
    return {
        "before": before,
        "after": after,
        "ingest": ingest,
        "statcast_extra": statcast_extra,
        "materialized_views": materialized_views,
    }


@flow(name="statcast-update-date", log_prints=True)
def statcast_update_date(game_date: date | str) -> dict[str, Any]:
    """Fetch and upsert Statcast rows for a single ``game_date`` (regular season only)."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    d = _coerce_game_date(game_date)
    start, end = _datetime_range_for_game_date(d)
    before = statcast_snapshot_metrics_task(database_url)
    ingest = statcast_ingest_range_task(start, end, database_url)
    after = statcast_snapshot_metrics_task(database_url)
    create_markdown_artifact(
        key="statcast-run-summary",
        markdown=_artifact_markdown(f"update_date {d.isoformat()}", before, after, ingest),
    )
    statcast_extra = statcast_extra_ingest_year_flow(
        year=d.year,
        start_date=d,
        end_date=d,
    )
    materialized_views = refresh_materialized_views_flow()
    log.info(
        "Run summary: game_date=%s before=%s after=%s ingest=%s statcast_extra=%s materialized_views=%s",
        d.isoformat(),
        before,
        after,
        ingest,
        statcast_extra,
        materialized_views,
    )
    return {
        "game_date": d.isoformat(),
        "before": before,
        "after": after,
        "ingest": ingest,
        "statcast_extra": statcast_extra,
        "materialized_views": materialized_views,
    }


@flow(name="statcast-update-full", log_prints=True)
def statcast_update_full() -> dict[str, Any]:
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    today = datetime.today()
    before = statcast_snapshot_metrics_task(database_url)
    ingest = statcast_ingest_range_task(EARLIEST_DATA_DATE, today, database_url)
    after = statcast_snapshot_metrics_task(database_url)
    create_markdown_artifact(
        key="statcast-run-summary",
        markdown=_artifact_markdown("update_full", before, after, ingest),
    )
    log.info("Run summary: before=%s after=%s ingest=%s", before, after, ingest)
    return {"before": before, "after": after, "ingest": ingest}


@flow(name="statcast-season", log_prints=True)
def statcast_season(year: int) -> dict[str, Any]:
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    start = datetime(year, 3, 1)
    end = datetime(year, 11, 30)
    before = statcast_snapshot_metrics_task(database_url)
    ingest = statcast_ingest_range_task(start, end, database_url)
    after = statcast_snapshot_metrics_task(database_url)
    create_markdown_artifact(
        key="statcast-run-summary",
        markdown=_artifact_markdown(f"season {year}", before, after, ingest),
    )
    log.info("Run summary: before=%s after=%s ingest=%s", before, after, ingest)
    return {"before": before, "after": after, "ingest": ingest}
