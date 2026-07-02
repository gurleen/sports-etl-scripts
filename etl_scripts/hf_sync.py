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

# Dataset-viewer subsets: each Parquet table becomes its own ``config_name`` so
# the Hub Data Studio shows them separately instead of merging every file into
# one default dataset. ``statcast`` globs the season-partitioned files into a
# single subset (new seasons are picked up automatically). Order is preserved in
# the card; the first entry is the viewer's default subset.
DATASET_CONFIGS: list[tuple[str, str]] = [
    ("mlb_schedule", "mlb_schedule.parquet"),
    ("retrosheet_plays", "retrosheet_plays.parquet"),
    ("baserunning_events", "baserunning_events.parquet"),
    ("mlb_transactions", "mlb_transactions.parquet"),
    ("statcast", "statcast_*.parquet"),
]


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


def _dataset_card() -> str:
    """The ``README.md`` with a ``configs:`` block so each table is its own subset."""
    lines = ["---", "configs:"]
    for name, pattern in DATASET_CONFIGS:
        lines.append(f"- config_name: {name}")
        lines.append(f"  data_files: {pattern}")
    lines += [
        "---",
        "",
        "# Baseball data",
        "",
        "MLB datasets published as Parquet, one **subset** per table (select it in the",
        "Data Studio dropdown). Maintained by the `etl hf` GitHub Actions jobs; each run",
        "merges newly-fetched rows into the existing file (dedup on each table's primary",
        "key). The `statcast` subset combines the season-partitioned `statcast_<year>`",
        "files.",
        "",
    ]
    return "\n".join(lines)


def ensure_dataset_card(*, force: bool = False) -> bool:
    """Publish the dataset card that defines the per-table subsets.

    No-op when a ``README.md`` already exists unless ``force`` is set. Returns
    whether the card was uploaded.
    """
    api = _api()
    ensure_repo()
    exists = api.file_exists(repo_id=hf_repo(), filename="README.md", repo_type="dataset")
    if exists and not force:
        return False
    api.upload_file(
        path_or_fileobj=_dataset_card().encode("utf-8"),
        path_in_repo="README.md",
        repo_id=hf_repo(),
        repo_type="dataset",
        commit_message="Set dataset-viewer subsets (one per table)",
    )
    logger.info("{} dataset card on {} (per-table subsets)", "Updated" if exists else "Created", hf_repo())
    return True


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
    size = local_path.stat().st_size if local_path.exists() else 0
    logger.info("Uploading {} ({:.1f} MB) to {} ...", filename, size / 1024 / 1024, hf_repo())
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
    src = f"read_parquet('{_parquet_literal(local)}')"
    source_rows = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    before = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    # OR IGNORE: an existing Hub file may itself carry duplicate primary keys (e.g.
    # a table written before this dedup existed); keep the first row per key and
    # drop the rest so seeding — and thus the re-published, deduped file — never errors.
    con.execute(f"INSERT OR IGNORE INTO {table} BY NAME SELECT * FROM {src}")
    n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    dropped = int(source_rows) - (int(n) - int(before))
    if dropped > 0:
        logger.warning(
            "Seeded {} rows into {} from {} ({} duplicate-key row(s) dropped)",
            n, table, filename, dropped,
        )
    else:
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

# Columns dropped from the published statcast_<year>.parquet only (the Postgres
# statcast_extra table and PitchData model keep the full field set). Each is
# either an exact duplicate of another kept column, trivially derivable from
# one (inches = feet * 12), constant across every row in a season-partitioned
# file, or a denormalized name that's 1:1 with a *_id column.
STATCAST_EXTRA_DROP_COLUMNS: tuple[str, ...] = (
    "plate_x_poly",  # == px
    "plate_z_poly",  # == pz
    "pfx_x",  # == break_x_feet
    "pfx_x_no_abs",  # == break_x_feet
    "pfx_z",  # == break_z_induced_feet
    "pfx_z_with_gravity",  # == break_z_with_gravity_feet
    "savant_is_in_zone",  # == is_in_zone
    "result",  # == events (kept, as an Enum)
    "break_x_inches",  # == break_x_feet * 12
    "break_z_induced_inches",  # == break_z_induced_feet * 12
    "break_z_with_gravity_inches",  # == break_z_with_gravity_feet * 12
    "year",  # constant per file (statcast_<year>.parquet is already year-partitioned)
    "pitcher_name",  # 1:1 with pitcher; join a players table for display names
    "batter_name",  # 1:1 with batter
    "catcher_name",  # 1:1 with catcher
    "description",  # mostly redundant with pitch_call/call_name at pitch grain
)


def _statcast_extra_int_types(pl: Any) -> dict[str, Any]:
    """Narrowest integer width each column can hold with headroom for future growth.

    Rule-bounded game state (balls/strikes/outs/inning/zone/...) fits Int8. MLBAM
    player ids and game_pk are monotonically-increasing global counters (currently
    in the hundreds of thousands) that keep growing for decades, so they get Int32
    rather than being cut to today's exact range. Team ids (108-158) already exceed
    Int8's range, so they get Int16.
    """
    return {
        "sport_id": pl.Int8,
        "inning": pl.Int8,
        "ab_number": pl.Int16,  # extra-inning marathons can exceed Int8's 127
        "pitch_number": pl.Int8,
        "cap_index": pl.Int16,
        "outs": pl.Int8,
        "strikes": pl.Int8,
        "balls": pl.Int8,
        "pre_strikes": pl.Int8,
        "pre_balls": pl.Int8,
        "sz_width": pl.Int8,
        "zone": pl.Int8,
        "spin_rate": pl.Int16,
        "batter": pl.Int32,
        "pitcher": pl.Int32,
        "catcher": pl.Int32,
        "abs_challenge_challenging_player_id": pl.Int32,
        "team_batting_id": pl.Int16,
        "team_fielding_id": pl.Int16,
        "abs_challenge_challenge_team_id": pl.Int16,
        "game_pk": pl.Int32,
    }


def _prepare_statcast_extra_frame(df: Any) -> Any:
    """Drop redundant columns, shrink ``play_id`` to raw bytes, and narrow integer widths.

    Applied to both the existing Hub frame and newly-fetched rows before they're
    combined, so a pre-migration (wide/Int64) Hub file and freshly narrowed rows
    end up on the same schema.
    """
    import polars as pl

    df = df.drop([c for c in STATCAST_EXTRA_DROP_COLUMNS if c in df.columns])
    if "play_id" in df.columns and df.schema["play_id"] != pl.Binary:
        df = df.with_columns(
            pl.col("play_id").str.replace_all("-", "").str.decode("hex").alias("play_id")
        )
    int_types = _statcast_extra_int_types(pl)
    casts = [pl.col(c).cast(t) for c, t in int_types.items() if c in df.columns and df.schema[c] != t]
    if casts:
        df = df.with_columns(casts)
    return df


def collect_statcast_extra(
    *,
    year: int,
    days: int | None,
    work_dir: Path,
    upload: bool = True,
    pause_sec: float = 0.0,
    fetch_attempts: int = 4,
    workers: int = 1,
) -> dict[str, Any]:
    """Fetch missing Savant gamefeeds and (re)publish ``statcast_<year>.parquet``.

    The original ``statcast_extra`` sync discovers game_pks from the ``statcast``
    table (which is excluded from the Hub). Here we instead discover Final,
    regular-season game_pks from ``mlb_schedule.parquet`` (published by the pbp
    job), skip the ones already present in ``statcast_<year>.parquet``, fetch the
    rest via the existing gamefeed fetch/parse code, merge, dedup on
    ``(game_pk, play_id)`` and re-upload. ``workers`` fetches gamefeeds
    concurrently (each game is an independent HTTP call; no shared DB connection
    to bound, unlike the mlbam pbp loader).
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
    sched = pl.read_parquet(sched_path)
    flt = (
        (pl.col("season_year") == year)
        & (pl.col("game_type") == "R")
        & (pl.col("coded_game_state") == "F")
    )
    if days is None:
        window_desc = "full season"  # backfill: every Final regular-season game
    else:
        today = date.today()
        start = today - timedelta(days=days)
        flt = flt & (pl.col("official_date") >= start) & (pl.col("official_date") <= today)
        window_desc = f"{start}..{today}"
    candidates: list[int] = (
        sched.filter(flt).select("game_pk").unique().to_series().to_list()
    )

    # 2. existing rows already on the Hub for this season.
    existing_path = download_existing(extra_file, work_dir)
    existing = pl.read_parquet(existing_path) if existing_path else None
    if existing is not None:
        existing = _prepare_statcast_extra_frame(existing)
    have = set(existing["game_pk"].to_list()) if existing is not None else set()
    todo = [int(g) for g in candidates if int(g) not in have]
    logger.info(
        "statcast_extra year={} window={}: {} candidate games, {} already present, {} to fetch",
        year, window_desc, len(candidates), len(have), len(todo),
    )

    # 3. fetch + parse the missing gamefeeds (reuses the existing HTTP/pydantic path),
    # ``workers`` of them concurrently.
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_one(gpk: int) -> tuple[int, list[dict[str, Any]] | None, str | None]:
        # The Savant /gf bodies are large (1-2 MB) and occasionally arrive
        # truncated (IncompleteRead); a couple of retries clears the transient.
        last_exc: Exception | None = None
        for attempt in range(1, max(1, fetch_attempts) + 1):
            try:
                feed = fetch_and_parse_gamefeed(gpk)
                if pause_sec > 0:
                    time.sleep(pause_sec)
                return gpk, _rows_for_game(gpk, feed), None
            except Exception as exc:  # noqa: BLE001 - isolate per-game failures
                last_exc = exc
                if attempt < fetch_attempts:
                    time.sleep(min(2.0 * attempt, 5.0))
        return gpk, None, str(last_exc)

    new_rows: list[dict[str, Any]] = []
    loaded = failed = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_fetch_one, gpk) for gpk in todo]
        for i, fut in enumerate(as_completed(futures), 1):
            gpk, rows, err = fut.result()
            if err is not None:
                logger.error(
                    "game_pk={}: failed to fetch/parse gamefeed after {} attempts: {}",
                    gpk, fetch_attempts, err,
                )
                failed += 1
            else:
                new_rows.extend(rows or [])
                loaded += 1
            if i % 50 == 0 or i == len(todo):
                logger.info("Fetched {}/{} games ({} loaded, {} failed)", i, len(todo), loaded, failed)

    if not new_rows and existing is None:
        logger.info("Nothing to publish for {} (no new rows, no existing file)", extra_file)
        return {"year": year, "games_loaded": 0, "games_failed": failed, "rows": 0, "published": False}

    # 4. merge, dedup on the statcast_extra primary key, write + upload.
    frames = []
    if existing is not None:
        frames.append(existing)
    if new_rows:
        frames.append(_prepare_statcast_extra_frame(pl.DataFrame(new_rows)))
    combined = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")
    combined = combined.unique(subset=["game_pk", "play_id"], keep="last")

    # events (PA-terminal outcome, e.g. "Home Run"/"Strikeout") is the one column
    # from the removed events/description/result trio worth keeping — as a small,
    # fixed-vocabulary Enum instead of a repeated plain string.
    if "events" in combined.columns:
        categories = sorted(v for v in combined["events"].unique().to_list() if v is not None)
        combined = combined.with_columns(pl.col("events").cast(pl.Enum(categories)))

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
