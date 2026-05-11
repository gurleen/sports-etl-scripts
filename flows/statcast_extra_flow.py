"""Prefect flow: Savant gamefeed extras into ``statcast_extra``."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.prefect_runtime import resolve_database_url_for_flow
from etl_scripts.statcast_extra import sync_missing_gamefeeds_for_year


@task
def statcast_extra_sync_task(
    year: int | None,
    database_url: str,
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    return sync_missing_gamefeeds_for_year(year, database_url=database_url, pause_sec=pause_sec)


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
    pause_sec: float = 0.2,
) -> dict[str, Any]:
    """Fetch Savant gamefeed JSON for missing ``game_pk`` values and load ``statcast_extra``."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    y = year if year is not None else datetime.now().year
    summary = statcast_extra_sync_task(y, database_url, pause_sec=pause_sec)
    create_markdown_artifact(
        key="statcast-extra-run-summary",
        markdown=_artifact_markdown(summary),
    )
    log.info("statcast_extra summary: %s", summary)
    return summary
