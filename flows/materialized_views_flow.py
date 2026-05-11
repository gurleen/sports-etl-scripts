"""Prefect flow(s) for refreshing warehouse materialized views."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task

from etl_scripts.materialized_views import DEFAULT_MATERIALIZED_VIEWS, refresh_materialized_views
from etl_scripts.prefect_runtime import resolve_database_url_for_flow


@task
def refresh_materialized_views_task(database_url: str, views: tuple[str, ...]) -> list[str]:
    return refresh_materialized_views(database_url, views)


@flow(name="refresh-materialized-views", log_prints=True)
def refresh_materialized_views_flow(
    views: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Refresh each materialized view in ``views``, or the default list from ``DEFAULT_MATERIALIZED_VIEWS``."""
    log = get_run_logger()
    names = views if views is not None else DEFAULT_MATERIALIZED_VIEWS
    database_url = resolve_database_url_for_flow()
    refreshed = refresh_materialized_views_task(database_url, names)
    log.info("Refreshed materialized views: %s", refreshed)
    return {"refreshed": refreshed, "count": len(refreshed)}
