"""Prefect flow: MLB Stats API play-by-play into ``retrosheet_plays`` (source='mlbam')."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_progress_artifact, update_progress_artifact

from etl_scripts.mlbam_pbp import load_season
from etl_scripts.prefect_runtime import resolve_database_url_for_flow


@task
def mlbam_pbp_load_task(
    year: int,
    database_url: str,
    *,
    only_missing: bool,
    start_date: date | None,
    end_date: date | None,
    write_baserunning: bool,
) -> dict[str, Any]:
    log = get_run_logger()
    progress_id = create_progress_artifact(
        0, key="mlbam-pbp-ingest", description="Starting MLBAM play-by-play ingest…"
    )

    def on_progress(done: int, total: int, loaded: int, failed: int) -> None:
        pct = (100.0 * done / total) if total else 100.0
        desc = f"{done}/{total} games ({loaded} loaded, {failed} failed)"
        update_progress_artifact(progress_id, pct, description=desc)
        log.info("mlbam pbp progress: %s", desc)

    return load_season(
        year,
        database_url=database_url,
        only_missing=only_missing,
        start_date=start_date,
        end_date=end_date,
        write_baserunning=write_baserunning,
        on_progress=on_progress,
    )


def _artifact_markdown(summary: dict[str, Any]) -> str:
    fails = summary.get("failures") or []
    fail_lines = "\n".join(f"- game_pk **{f['game_pk']}**: `{f['error']}`" for f in fails[:20])
    return "\n".join(
        [
            "# MLBAM play-by-play (`retrosheet_plays` source='mlbam')",
            "",
            f"- Season: **{summary.get('season')}**",
            f"- Games targeted: **{summary['games_targeted']}**",
            f"- Games loaded: **{summary['games_loaded']}**",
            f"- Play rows written: **{summary['plays_written']}**",
            f"- Baserunning rows written: **{summary['baserunning_written']}**",
            f"- Games failed: **{summary['games_failed']}**",
            "",
            "## Failures (sample)" if fails else "## Failures",
            fail_lines if fail_lines else "_None_",
        ]
    )


@flow(name="mlbam-pbp-update-recent", log_prints=True)
def mlbam_pbp_update_recent(
    days: int = 3, year: int | None = None, write_baserunning: bool = True
) -> dict[str, Any]:
    """Re-fetch Final games from the last ``days`` days (box scores get corrected post-game)."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    today = date.today()
    y = year or today.year
    summary = mlbam_pbp_load_task(
        y, database_url, only_missing=False,
        start_date=today - timedelta(days=days), end_date=today,
        write_baserunning=write_baserunning,
    )
    create_markdown_artifact(key="mlbam-pbp-run-summary", markdown=_artifact_markdown(summary))
    log.info("mlbam pbp update-recent summary: %s", {k: v for k, v in summary.items() if k != "failures"})
    return summary


@flow(name="mlbam-pbp-ingest-year", log_prints=True)
def mlbam_pbp_ingest_year_flow(
    year: int | None = None, reload: bool = False, write_baserunning: bool = True
) -> dict[str, Any]:
    """Load Final regular-season games for ``year`` (only-missing unless ``reload``)."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    y = year if year is not None else datetime.now().year
    summary = mlbam_pbp_load_task(
        y, database_url, only_missing=not reload,
        start_date=None, end_date=None, write_baserunning=write_baserunning,
    )
    create_markdown_artifact(key="mlbam-pbp-run-summary", markdown=_artifact_markdown(summary))
    log.info("mlbam pbp ingest-year summary: %s", {k: v for k, v in summary.items() if k != "failures"})
    return summary
