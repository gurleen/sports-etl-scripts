"""Publish ETL warehouse tables to a Hugging Face dataset repo as Parquet.

The HF publishing jobs (see ``etl hf`` and ``.github/workflows/hf-*.yml``) use a
throwaway local DuckDB as a staging/merge engine instead of the production
Postgres warehouse: each job **seeds** the relevant tables from the Parquet
already on the Hub (so history accumulates across runs), runs the existing
ingest against DuckDB (``ETL_DB_BACKEND=duckdb``), then **exports** each table
back to Parquet and **uploads** it to the dataset repo. One Parquet file per
table (``mlb_schedule.parquet`` etc.); ``statcast_extra`` is written
season-partitioned as ``statcast_<year>.parquet``.

Config (env vars):

* ``HF_DATASET_REPO`` — target dataset repo id (default ``gurleen/baseball``).
* ``HF_TOKEN`` — write-scoped token used for download (private repos) + upload.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from loguru import logger

DEFAULT_REPO = "gurleen/baseball"


# ---------------------------------------------------------------------------
# Config + Hub client helpers
# ---------------------------------------------------------------------------
def hf_repo() -> str:
    """The target Hugging Face dataset repo id."""
    return os.getenv("HF_DATASET_REPO", DEFAULT_REPO)


def hf_token() -> str | None:
    """Write/read token (``HF_TOKEN``; falls back to the SDK's standard var)."""
    return os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")


def _api():
    from huggingface_hub import HfApi

    return HfApi(token=hf_token())


def _not_found_errors() -> tuple[type[BaseException], ...]:
    """``(EntryNotFoundError, RepositoryNotFoundError)`` across huggingface_hub versions."""
    errs: list[type[BaseException]] = []
    try:  # huggingface_hub >= 1.0
        from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError

        errs += [EntryNotFoundError, RepositoryNotFoundError]
    except ImportError:  # older layout
        try:
            from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError

            errs += [EntryNotFoundError, RepositoryNotFoundError]
        except ImportError:
            pass
    return tuple(errs)


def ensure_repo() -> None:
    """Create the dataset repo if it doesn't exist yet (idempotent)."""
    _api().create_repo(repo_id=hf_repo(), repo_type="dataset", exist_ok=True)


def download_existing(filename: str, work_dir: Path) -> Path | None:
    """Download ``filename`` from the dataset repo, or ``None`` if it doesn't exist.

    A missing file (first run) is expected, not an error.
    """
    from huggingface_hub import hf_hub_download

    not_found = _not_found_errors()
    try:
        path = hf_hub_download(
            repo_id=hf_repo(),
            filename=filename,
            repo_type="dataset",
            token=hf_token(),
            local_dir=str(work_dir),
        )
        return Path(path)
    except not_found:
        logger.info("No existing {} on {}; starting empty", filename, hf_repo())
        return None
    except Exception as exc:  # noqa: BLE001 - treat 404 as "not there yet"
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 404:
            logger.info("No existing {} on {}; starting empty", filename, hf_repo())
            return None
        raise


def upload_parquet(local_path: Path, filename: str) -> None:
    """Upload a local Parquet file to the dataset repo as ``filename``."""
    ensure_repo()
    _api().upload_file(
        path_or_fileobj=str(local_path),
        path_in_repo=filename,
        repo_id=hf_repo(),
        repo_type="dataset",
        commit_message=f"Update {filename}",
    )
    logger.info("Uploaded {} -> {}/{}", filename, hf_repo(), filename)


# ---------------------------------------------------------------------------
# DuckDB staging helpers (seed from HF / export back to Parquet)
# ---------------------------------------------------------------------------
def _parquet_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def seed_table_from_hf(con: Any, table: str, ddl: str, filename: str, work_dir: Path) -> int:
    """Create ``table`` (via ``ddl``) and load the existing Hub Parquet into it.

    ``ddl`` may contain several ``;``-separated statements (e.g. table + indexes).
    Returns the number of rows seeded (0 on the first run, before the file exists).
    Columns are matched by name so the table's schema (which carries the primary
    key needed for later upserts) wins over the Parquet's physical column order.
    """
    for stmt in ddl.split(";"):
        if stmt.strip():
            con.execute(stmt)
    local = download_existing(filename, work_dir)
    if local is None:
        return 0
    con.execute(
        f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet('{_parquet_literal(local)}')"
    )
    n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    logger.info("Seeded {} rows into {} from {}", n, table, filename)
    return int(n)


def export_table_to_parquet(con: Any, table: str, out_path: Path, where: str | None = None) -> int:
    """``COPY`` a table (optionally filtered) to a single Parquet file. Returns rows written."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    where_sql = f" WHERE {where}" if where else ""
    con.execute(
        f"COPY (SELECT * FROM {table}{where_sql}) TO '{_parquet_literal(out_path)}' (FORMAT PARQUET)"
    )
    n = con.execute(f"SELECT count(*) FROM {table}{where_sql}").fetchone()[0]
    return int(n)


def publish_table(
    con: Any,
    table: str,
    filename: str,
    work_dir: Path,
    *,
    where: str | None = None,
    upload: bool = True,
) -> int:
    """Export ``table`` to ``work_dir/filename`` and (unless ``upload`` is False) upload it."""
    out = work_dir / filename
    n = export_table_to_parquet(con, table, out, where)
    logger.info("Exported {} rows from {} -> {}", n, table, out)
    if upload:
        upload_parquet(out, filename)
    else:
        logger.info("--no-upload: left {} on disk only", out)
    return n


# ---------------------------------------------------------------------------
# statcast_extra collector (polars; decoupled from the excluded `statcast` table)
# ---------------------------------------------------------------------------
def collect_statcast_extra(
    *,
    year: int,
    days: int,
    work_dir: Path,
    upload: bool = True,
    pause_sec: float = 0.0,
) -> dict[str, Any]:
    """Fetch missing Savant gamefeeds and (re)publish ``statcast_<year>.parquet``.

    The original ``statcast_extra`` sync discovers game_pks from the ``statcast``
    table (which is excluded from the Hub). Here we instead discover Final,
    regular-season game_pks from ``mlb_schedule.parquet`` (published by the pbp
    job), skip the ones already present in ``statcast_<year>.parquet``, fetch the
    rest via the existing gamefeed fetch/parse code, merge, dedup on
    ``(game_pk, play_id)`` and re-upload.
    """
    import polars as pl

    from etl_scripts.statcast_extra import _rows_for_game, fetch_and_parse_gamefeed

    extra_file = f"statcast_{year}.parquet"

    # 1. discover candidate game_pks from the schedule snapshot on the Hub.
    sched_path = download_existing("mlb_schedule.parquet", work_dir)
    if sched_path is None:
        raise RuntimeError(
            "mlb_schedule.parquet not found on the Hub; run the pbp job first so the "
            "schedule snapshot exists, then re-run statcast-extra."
        )
    today = date.today()
    start = today - timedelta(days=days)
    sched = pl.read_parquet(sched_path)
    candidates: list[int] = (
        sched.filter(
            (pl.col("season_year") == year)
            & (pl.col("game_type") == "R")
            & (pl.col("coded_game_state") == "F")
            & (pl.col("official_date") >= start)
            & (pl.col("official_date") <= today)
        )
        .select("game_pk")
        .unique()
        .to_series()
        .to_list()
    )

    # 2. existing rows already on the Hub for this season.
    existing_path = download_existing(extra_file, work_dir)
    existing = pl.read_parquet(existing_path) if existing_path else None
    have = set(existing["game_pk"].to_list()) if existing is not None else set()
    todo = [int(g) for g in candidates if int(g) not in have]
    logger.info(
        "statcast_extra year={} window={}..{}: {} candidate games, {} already present, {} to fetch",
        year, start, today, len(candidates), len(have), len(todo),
    )

    # 3. fetch + parse the missing gamefeeds (reuses the existing HTTP/pydantic path).
    import time

    new_rows: list[dict[str, Any]] = []
    loaded = failed = 0
    for i, gpk in enumerate(todo):
        try:
            feed = fetch_and_parse_gamefeed(gpk)
            new_rows.extend(_rows_for_game(gpk, feed))
            loaded += 1
        except Exception as exc:  # noqa: BLE001 - isolate per-game failures
            logger.error("game_pk={}: failed to fetch/parse gamefeed: {}", gpk, exc)
            failed += 1
        if pause_sec > 0 and i + 1 < len(todo):
            time.sleep(pause_sec)

    if not new_rows and existing is None:
        logger.info("Nothing to publish for {} (no new rows, no existing file)", extra_file)
        return {"year": year, "games_loaded": 0, "games_failed": failed, "rows": 0, "published": False}

    # 4. merge, dedup on the statcast_extra primary key, write + upload.
    frames = []
    if existing is not None:
        frames.append(existing)
    if new_rows:
        frames.append(pl.DataFrame(new_rows))
    combined = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")
    combined = combined.unique(subset=["game_pk", "play_id"], keep="last")

    out = work_dir / extra_file
    combined.write_parquet(out)
    logger.info("Wrote {} rows -> {}", combined.height, out)
    if upload:
        upload_parquet(out, extra_file)
    else:
        logger.info("--no-upload: left {} on disk only", out)

    return {
        "year": year,
        "games_loaded": loaded,
        "games_failed": failed,
        "rows": combined.height,
        "published": True,
    }
