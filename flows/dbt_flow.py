"""Prefect flow: rebuild baseball schema models via programmatic dbt."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.dbt_runner import (
    DEFAULT_SELECTOR,
    run_dbt_build,
    statcast_relevant_data_changed,
)


@task
def dbt_build_task(
    *,
    selector: str = DEFAULT_SELECTOR,
    season_year: int | None = None,
) -> dict[str, Any]:
    return run_dbt_build(selector=selector, season_year=season_year)


def _artifact_markdown(summary: dict[str, Any], *, skipped: bool, reason: str | None) -> str:
    if skipped:
        return "\n".join(
            [
                "# dbt rebuild (baseball)",
                "",
                "**Skipped** — no relevant warehouse changes detected.",
                f"- Reason: `{reason}`",
            ]
        )
    lines = [
        "# dbt rebuild (baseball)",
        "",
        f"- Selector: **`{summary.get('selector')}`**",
        f"- Success: **{summary.get('success')}**",
        f"- Nodes executed: **{summary.get('node_count', 0)}**",
        f"- Status counts: `{summary.get('status_counts')}`",
    ]
    if summary.get("season_year") is not None:
        lines.append(f"- season_year var: **{summary['season_year']}**")
    nodes = summary.get("nodes") or []
    if nodes:
        lines.extend(["", "## Nodes", ""])
        for n in nodes[:40]:
            lines.append(f"- `{n['name']}` ({n['resource_type']}): {n['status']}")
        if len(nodes) > 40:
            lines.append(f"- … *{len(nodes) - 40} more omitted*")
    return "\n".join(lines)


@flow(name="dbt-rebuild-baseball", log_prints=True)
def dbt_rebuild_baseball_flow(
    selector: str = DEFAULT_SELECTOR,
    season_year: int | None = None,
    force: bool = False,
    *,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    ingest: dict[str, Any] | None = None,
    statcast_extra: dict[str, Any] | None = None,
    backfill: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Run ``dbt build`` for baseball marts (default selector ``post_statcast_ingest``).

    When ``force`` is false, skips the build unless ``statcast_relevant_data_changed``
    reports changes from the optional before/after ingest context (for Statcast subflows).
    """
    log = get_run_logger()
    if not force and not statcast_relevant_data_changed(
        before=before,
        after=after,
        ingest=ingest,
        statcast_extra=statcast_extra,
        backfill=backfill,
    ):
        log.info("Skipping dbt rebuild: no Statcast-related warehouse changes")
        summary = {"skipped": True, "reason": "no_statcast_changes"}
        create_markdown_artifact(
            key="dbt-rebuild-summary",
            markdown=_artifact_markdown(summary, skipped=True, reason=summary["reason"]),
        )
        return summary

    summary = dbt_build_task(selector=selector, season_year=season_year)
    create_markdown_artifact(
        key="dbt-rebuild-summary",
        markdown=_artifact_markdown(summary, skipped=False, reason=None),
    )
    log.info("dbt rebuild summary: %s", summary)
    return summary


def run_dbt_rebuild_after_statcast(
    *,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    ingest: dict[str, Any] | None = None,
    statcast_extra: dict[str, Any] | None = None,
    backfill: dict[str, Any] | None = None,
    season_year: int | None = None,
    selector: str = DEFAULT_SELECTOR,
) -> dict[str, Any]:
    """Invoke the dbt rebuild subflow after Statcast ingest (skips when data unchanged)."""
    return dbt_rebuild_baseball_flow(
        selector=selector,
        season_year=season_year,
        force=False,
        before=before,
        after=after,
        ingest=ingest,
        statcast_extra=statcast_extra,
        backfill=backfill,
    )
