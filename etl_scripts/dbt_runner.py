"""Programmatic dbt invocations via ``dbt.cli.main``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dbt.cli.main import dbtRunner, dbtRunnerResult
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SELECTOR = "post_statcast_ingest"
POST_STATCAST_EXTRA_SELECTOR = "post_statcast_extra_ingest"


def repo_root() -> Path:
    return REPO_ROOT


def statcast_relevant_data_changed(
    *,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    ingest: dict[str, Any] | None = None,
    statcast_extra: dict[str, Any] | None = None,
    backfill: dict[str, Any] | None = None,
) -> bool:
    """True when a Statcast ingest run likely changed warehouse data dbt reads."""
    if ingest is not None:
        if int(ingest.get("rows_written") or 0) > 0:
            return True
        if int(ingest.get("rows_fetched") or 0) > 0:
            return True
    if backfill is not None:
        if int(backfill.get("rows_written") or 0) > 0:
            return True
        if int(backfill.get("games_loaded") or 0) > 0:
            return True
    if before is not None and after is not None:
        if int(after.get("row_count") or 0) != int(before.get("row_count") or 0):
            return True
        if (after.get("max_game_date") or "") != (before.get("max_game_date") or ""):
            return True
    if statcast_extra is not None:
        if int(statcast_extra.get("rows_written") or 0) > 0:
            return True
        if int(statcast_extra.get("games_loaded") or 0) > 0:
            return True
    return False


def _summarize_execution(result: dbtRunnerResult) -> dict[str, Any]:
    nodes: list[dict[str, str]] = []
    execution = result.result
    if execution is not None and hasattr(execution, "results"):
        for run_result in execution.results:
            node = run_result.node
            nodes.append(
                {
                    "name": node.name,
                    "resource_type": node.resource_type.value,
                    "status": str(run_result.status),
                }
            )
    status_counts: dict[str, int] = {}
    for n in nodes:
        status_counts[n["status"]] = status_counts.get(n["status"], 0) + 1
    return {
        "success": result.success,
        "node_count": len(nodes),
        "status_counts": status_counts,
        "nodes": nodes,
    }


def run_dbt_build(
    *,
    selector: str = DEFAULT_SELECTOR,
    project_dir: Path | str | None = None,
    profiles_dir: Path | str | None = None,
    season_year: int | None = None,
) -> dict[str, Any]:
    """
    Run ``dbt build`` for ``selector`` using ``dbtRunner`` (same behavior as the CLI).

    Raises ``RuntimeError`` when dbt reports failure.
    """
    root = Path(project_dir) if project_dir is not None else REPO_ROOT
    profiles = Path(profiles_dir) if profiles_dir is not None else REPO_ROOT
    args = [
        "build",
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(profiles),
        "--selector",
        selector,
    ]
    if season_year is not None:
        args.extend(["--vars", json.dumps({"season_year": season_year})])

    logger.info("dbt invoke: {}", " ".join(args))
    invoke_result: dbtRunnerResult = dbtRunner().invoke(args)
    summary = _summarize_execution(invoke_result)
    summary["selector"] = selector
    summary["project_dir"] = str(root)
    summary["profiles_dir"] = str(profiles)
    if season_year is not None:
        summary["season_year"] = season_year

    if not invoke_result.success:
        exc = invoke_result.exception
        msg = str(exc) if exc is not None else "dbt build failed"
        logger.error("dbt build failed: {}", msg)
        raise RuntimeError(msg) from exc

    logger.info(
        "dbt build complete: {} nodes, statuses={}",
        summary["node_count"],
        summary["status_counts"],
    )
    return summary
