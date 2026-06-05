"""Prefect flow: backfill ``statcast`` for season dates with no rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_progress_artifact, update_progress_artifact

from etl_scripts.prefect_runtime import resolve_database_url_for_flow
from etl_scripts.statcast import statcast_table_metrics
from etl_scripts.statcast_backfill import backfill_statcast_missing_dates_for_year
from flows.dbt_flow import run_dbt_rebuild_after_statcast
from flows.statcast_extra_flow import statcast_extra_ingest_year_flow


@task
def statcast_backfill_task(
    year: int,
    database_url: str,
    *,
    limit_days: int | None = None,
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    log = get_run_logger()
    progress_id = create_progress_artifact(
        0,
        key="statcast-backfill",
        description="Starting statcast backfill…",
    )

    def on_progress(dates_done: int, total_dates: int, loaded: int, failed: int) -> None:
        pct = (100.0 * dates_done / total_dates) if total_dates else 100.0
        desc = f"{dates_done}/{total_dates} dates ({loaded} games loaded, {failed} failed)"
        update_progress_artifact(progress_id, pct, description=desc)
        log.info("statcast backfill progress: %s", desc)

    return backfill_statcast_missing_dates_for_year(
        year,
        database_url=database_url,
        limit_days=limit_days,
        pause_sec=pause_sec,
        on_progress=on_progress,
    )


def _artifact_markdown(summary: dict[str, Any], before: dict[str, Any], after: dict[str, Any]) -> str:
    fails = summary.get("failures") or []
    fail_lines = "\n".join(
        f"- game_pk **{f['game_pk']}** ({f.get('game_date')}): `{f['error']}`" for f in fails[:20]
    )
    if len(fails) > 20:
        fail_lines += f"\n- … *{len(fails) - 20} more omitted*"
    date_fails = summary.get("date_fetch_failures") or []
    date_fail_lines = "\n".join(
        f"- {f['game_date']}: `{f['error']}`" for f in date_fails[:10]
    )
    delta_rows = int(after["row_count"]) - int(before["row_count"])
    return "\n".join(
        [
            "# Statcast backfill",
            "",
            f"- Year: **{summary['year']}**",
            f"- Days limit: **{summary.get('limit_days')}**",
            f"- Dates targeted (no prior rows): **{summary.get('dates_targeted', 0)}**",
            f"- Games loaded: **{summary.get('games_loaded', 0)}**",
            f"- Pitch rows written: **{summary.get('rows_written', 0)}**",
            f"- Games failed: **{summary.get('games_failed', 0)}**",
            f"- Date fetch failures: **{len(date_fails)}**",
            "",
            "## Table snapshot (`statcast`)",
            "| Metric | Before | After |",
            "|--------|--------|-------|",
            f"| row_count | {before['row_count']} | {after['row_count']} |",
            f"| max_game_date | {before['max_game_date']} | {after['max_game_date']} |",
            f"| **delta row_count** | | **{delta_rows:+d}** |",
            "",
            "## Game failures (sample)" if fails else "## Game failures",
            fail_lines if fail_lines else "_None_",
            "",
            "## Date fetch failures (sample)" if date_fails else "## Date fetch failures",
            date_fail_lines if date_fail_lines else "_None_",
        ]
    )


@flow(name="statcast-backfill", log_prints=True)
def statcast_backfill_flow(
    year: int | None = None,
    limit_days: int | None = None,
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    """
    Backfill regular-season Statcast rows for dates in ``year`` with no warehouse data.

    Fetches one ``game_date`` at a time and loads one ``game_pk`` at a time, then runs
    ``statcast_extra`` for the same year window when any dates were processed.
    """
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    y = year if year is not None else datetime.now().year
    before = statcast_table_metrics(database_url)
    summary = statcast_backfill_task(y, database_url, limit_days=limit_days, pause_sec=pause_sec)
    after = statcast_table_metrics(database_url)
    create_markdown_artifact(
        key="statcast-backfill-summary",
        markdown=_artifact_markdown(summary, before, after),
    )
    statcast_extra: dict[str, Any] | None = None
    if summary.get("dates_targeted", 0) > 0:
        processed = summary.get("dates_processed") or []
        statcast_extra = statcast_extra_ingest_year_flow(
            year=y,
            start_date=processed[0],
            end_date=processed[-1],
            pause_sec=pause_sec,
            rebuild_dbt=False,
        )
    dbt_rebuild = run_dbt_rebuild_after_statcast(
        before=before,
        after=after,
        backfill=summary,
        statcast_extra=statcast_extra,
        season_year=y,
    )
    log.info(
        "statcast backfill year=%s summary=%s statcast_extra=%s dbt=%s",
        y,
        summary,
        statcast_extra,
        dbt_rebuild,
    )
    return {
        "before": before,
        "after": after,
        "backfill": summary,
        "statcast_extra": statcast_extra,
        "dbt_rebuild": dbt_rebuild,
    }
