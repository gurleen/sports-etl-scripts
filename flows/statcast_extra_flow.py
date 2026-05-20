"""Prefect flow: Savant gamefeed extras into ``statcast_extra``."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_progress_artifact, update_progress_artifact

from etl_scripts.prefect_runtime import resolve_database_url_for_flow
from etl_scripts.statcast_extra import sync_missing_gamefeeds_for_year


def _coerce_game_date(value: date | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


@task
def statcast_extra_sync_task(
    year: int | None,
    database_url: str,
    days: int | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    log = get_run_logger()
    progress_id = create_progress_artifact(
        0,
        key="statcast-extra-ingest",
        description="Starting statcast_extra ingest…",
    )

    def on_progress(done: int, total: int, loaded: int, failed: int) -> None:
        pct = (100.0 * done / total) if total else 100.0
        desc = f"{done}/{total} games ({loaded} loaded, {failed} failed)"
        update_progress_artifact(progress_id, pct, description=desc)
        log.info("statcast_extra progress: %s", desc)

    return sync_missing_gamefeeds_for_year(
        year,
        days=days,
        start_date=_coerce_game_date(start_date),
        end_date=_coerce_game_date(end_date),
        database_url=database_url,
        pause_sec=pause_sec,
        on_progress=on_progress,
    )


def _artifact_markdown(summary: dict[str, Any]) -> str:
    fails = summary.get("failures") or []
    fail_lines = "\n".join(f"- game_pk **{f['game_pk']}**: `{f['error']}`" for f in fails[:20])
    if len(fails) > 20:
        fail_lines += f"\n- … *{len(fails) - 20} more omitted*"
    return "\n".join(
        [
            "# Statcast extra (Savant `/gf`)",
            "",
            f"- Year: **{summary['year']}**",
            f"- Date window: **{summary.get('start_date')}** → **{summary.get('end_date')}**",
            f"- Days limit (no window): **{summary.get('days')}** (most recent missing dates)",
            f"- Dates processed: **{summary.get('days_targeted', 0)}**",
            f"- Games targeted (no prior `statcast_extra` rows): **{summary['games_targeted']}**",
            f"- Games loaded: **{summary['games_loaded']}**",
            f"- Pitch rows written: **{summary['rows_written']}**",
            f"- Games failed: **{summary['games_failed']}**",
            "",
            "## Failures (sample)" if fails else "## Failures",
            fail_lines if fail_lines else "_None_",
        ]
    )


@flow(name="statcast-extra-ingest-year", log_prints=True)
def statcast_extra_ingest_year_flow(
    year: int | None = None,
    days: int | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    """
    Fetch Savant gamefeed JSON for missing ``game_pk`` values and load ``statcast_extra``.

    Processes one ``game_date`` at a time. Use ``start_date`` / ``end_date`` to match a Statcast ingest
    window; otherwise ``days`` limits to the N most recent missing dates in ``year``.
    """
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    y = year if year is not None else datetime.now().year
    summary = statcast_extra_sync_task(
        y,
        database_url,
        days=days,
        start_date=start_date,
        end_date=end_date,
        pause_sec=pause_sec,
    )
    create_markdown_artifact(
        key="statcast-extra-run-summary",
        markdown=_artifact_markdown(summary),
    )
    log.info("statcast_extra summary: %s", summary)
    return summary
