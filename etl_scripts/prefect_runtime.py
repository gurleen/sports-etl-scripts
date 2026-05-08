"""Prefect-specific runtime helpers (keeps Prefect imports out of domain modules)."""

from __future__ import annotations

import os


def resolve_database_url_for_flow() -> str:
    """Resolve DB URL: ``DATABASE_URL`` env, then Secret ``etl-database-url``, then ``POSTGRES_*``."""
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    try:
        from prefect.blocks.system import Secret

        return Secret.load("etl-database-url").get()
    except Exception:
        pass
    from etl_scripts.statcast import get_database_url

    return get_database_url()
