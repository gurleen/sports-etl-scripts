"""Build a play-by-play fact table from Retrosheet's pre-parsed ``plays`` CSVs.

This is a **local, ad-hoc** tool (run via ``etl retrosheet``), not one of the
nightly cron jobs. It downloads Retrosheet's parsed play-by-play files
(https://retrosheet.org/downloads/plays.html), cleans them with Polars into a
single play-grain schema designed for batting/pitching aggregation, maps
Retrosheet player IDs to MLBAM IDs via the Chadwick Bureau Register, and writes
Parquet.

Design goals
------------
* One row per play (event). ``pa``/``ab``/hit flags are 0/1 integers so stats
  like AVG, OBP, K%, WHIP, FIP come from plain ``GROUP BY`` + ``SUM``.
* Handedness is exposed for matchup splits (e.g. LHP vs RHB). ``bat_side``
  resolves switch hitters (Retrosheet ``bathand='B'``) to the side actually used
  (opposite the pitcher's throwing hand), matching how Statcast's ``stand`` works.
* **Earned-run accounting is exact.** Each play carries up to four run "slots"
  (the batter and three baserunners). For every run that scored we record the
  *responsible* pitcher (Retrosheet ``prun*``) and whether it was earned
  (``ur*``), so ERA can charge inherited runs to the correct pitcher rather than
  the pitcher who happened to be facing the batter.
* The schema is source-agnostic: a ``source`` discriminator and nullable MLBAM /
  Retro ID pairs let current-season MLBAM play-by-play load into the same table
  later.

The :data:`PG_SCHEMA` list is the single source of truth for both the Polars
projection (:func:`build_select_exprs`) and the Postgres DDL
(:func:`create_table_ddl`); :func:`validate_schema_consistency` asserts they stay
in sync.
"""

from __future__ import annotations

import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import polars as pl
from loguru import logger

TABLE_NAME = "retrosheet_plays"
"""Default warehouse table the emitted Parquet/DDL map to."""

SOURCE_LABEL = "retrosheet"
"""Value written to the ``source`` discriminator column."""

PLAYS_URL = "https://www.retrosheet.org/downloads/plays/{year}plays.zip"
FULL_PLAYS_URL = "https://www.retrosheet.org/downloads/plays/plays.zip"
"""Single archive with every play Retrosheet publishes (all seasons in one CSV)."""
EARLIEST_SEASON = 1903
"""First season Retrosheet publishes a per-year parsed plays file for."""

_USER_AGENT = (
    "Mozilla/5.0 (compatible; etl-scripts/1.0; +https://github.com/) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_DOWNLOAD_TIMEOUT_SEC = 300

DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "retrosheet_cache"

# Run "slots" on a play: the batter plus the three baserunners. Each tuple maps a
# slot key to its (scorer-id, responsible-pitcher, unearned-flag) raw columns.
_RUN_SLOTS: dict[str, tuple[str, str, str]] = {
    "b": ("run_b", "prun_b", "ur_b"),
    "1": ("run1", "prun1", "ur1"),
    "2": ("run2", "prun2", "ur2"),
    "3": ("run3", "prun3", "ur3"),
}


# ---------------------------------------------------------------------------
# Schema (single source of truth for projection + DDL)
# ---------------------------------------------------------------------------
def _run_slot_schema() -> list[tuple[str, str]]:
    cols: list[tuple[str, str]] = []
    for slot in _RUN_SLOTS:
        cols.extend(
            [
                (f"run_{slot}_runner_retro", "TEXT"),
                (f"run_{slot}_runner_mlbam", "INTEGER"),
                (f"run_{slot}_earned", "SMALLINT"),
                (f"run_{slot}_pitcher_retro", "TEXT"),
                (f"run_{slot}_pitcher_mlbam", "INTEGER"),
            ]
        )
    return cols


PG_SCHEMA: list[tuple[str, str]] = [
    # --- grain / source -----------------------------------------------------
    ("source", "TEXT NOT NULL"),
    ("game_id", "TEXT NOT NULL"),
    ("play_number", "INTEGER NOT NULL"),
    ("pbp_type", "TEXT"),
    # --- game context -------------------------------------------------------
    ("game_date", "DATE"),
    ("season", "INTEGER"),
    ("game_type", "TEXT"),
    ("inning", "INTEGER"),
    ("inning_topbot", "TEXT"),
    ("bat_home", "BOOLEAN"),
    ("bat_team", "TEXT"),
    ("pit_team", "TEXT"),
    ("site", "TEXT"),
    ("score_bat", "INTEGER"),
    ("score_pit", "INTEGER"),
    # --- players / handedness ----------------------------------------------
    ("batter_mlbam", "INTEGER"),
    ("batter_retro", "TEXT"),
    ("pitcher_mlbam", "INTEGER"),
    ("pitcher_retro", "TEXT"),
    ("bat_hand", "TEXT"),
    ("bat_side", "TEXT"),
    ("pit_hand", "TEXT"),
    ("lineup_pos", "SMALLINT"),
    ("bat_field_pos", "SMALLINT"),
    ("count_balls", "SMALLINT"),
    ("count_strikes", "SMALLINT"),
    # --- plate-appearance outcome flags (0/1) -------------------------------
    ("pa", "SMALLINT"),
    ("ab", "SMALLINT"),
    ("hit", "SMALLINT"),
    ("single", "SMALLINT"),
    ("double", "SMALLINT"),
    ("triple", "SMALLINT"),
    ("home_run", "SMALLINT"),
    ("walk", "SMALLINT"),
    ("intent_walk", "SMALLINT"),
    ("hit_by_pitch", "SMALLINT"),
    ("strikeout", "SMALLINT"),
    ("sac_fly", "SMALLINT"),
    ("sac_bunt", "SMALLINT"),
    ("reached_on_error", "SMALLINT"),
    ("fielders_choice", "SMALLINT"),
    ("reached_on_interference", "SMALLINT"),
    ("gdp", "SMALLINT"),
    ("other_dp", "SMALLINT"),
    ("triple_play", "SMALLINT"),
    ("other_out", "SMALLINT"),
    # --- batted ball --------------------------------------------------------
    ("ball_in_play", "SMALLINT"),
    ("bunt", "SMALLINT"),
    ("ground_ball", "SMALLINT"),
    ("fly_ball", "SMALLINT"),
    ("line_drive", "SMALLINT"),
    ("hit_type", "TEXT"),
    ("hit_location", "TEXT"),
    # --- outs ---------------------------------------------------------------
    ("outs_pre", "SMALLINT"),
    ("outs_post", "SMALLINT"),
    ("outs_on_play", "SMALLINT"),
    # --- base state at start of play (for RISP / situational splits) ---------
    ("on_1b", "BOOLEAN"),
    ("on_2b", "BOOLEAN"),
    ("on_3b", "BOOLEAN"),
    # --- runs (play level) --------------------------------------------------
    ("runs_on_play", "SMALLINT"),
    ("earned_runs", "SMALLINT"),
    ("unearned_runs", "SMALLINT"),
    ("team_unearned_runs", "SMALLINT"),
    ("rbi", "SMALLINT"),
    # --- per-run responsible-pitcher attribution (exact ERA) ----------------
    *_run_slot_schema(),
    # --- baserunning / misc event flags ------------------------------------
    ("wild_pitch", "SMALLINT"),
    ("passed_ball", "SMALLINT"),
    ("balk", "SMALLINT"),
    ("stolen_base_2b", "SMALLINT"),
    ("stolen_base_3b", "SMALLINT"),
    ("stolen_base_home", "SMALLINT"),
    ("caught_stealing_2b", "SMALLINT"),
    ("caught_stealing_3b", "SMALLINT"),
    ("caught_stealing_home", "SMALLINT"),
    ("pickoff_1b", "SMALLINT"),
    ("pickoff_2b", "SMALLINT"),
    ("pickoff_3b", "SMALLINT"),
    ("defensive_indifference", "SMALLINT"),
    ("other_advance", "SMALLINT"),
    ("error_count", "SMALLINT"),
    ("foul_error", "SMALLINT"),
    # --- raw passthrough ----------------------------------------------------
    ("event_raw", "TEXT"),
]

SCHEMA_COLUMNS: list[str] = [name for name, _ in PG_SCHEMA]


# ---------------------------------------------------------------------------
# Polars expression helpers
# ---------------------------------------------------------------------------
def _i(name: str) -> pl.Expr:
    """Cast a raw (string) column to a small integer, nulling unparseable values."""
    return pl.col(name).cast(pl.Int16, strict=False)


def _txt(name: str) -> pl.Expr:
    """Trim a raw text column, mapping empty strings to null."""
    return pl.col(name).replace("", None)


def _mlbam(retro_expr: pl.Expr, id_map: dict[str, int]) -> pl.Expr:
    """Map a Retrosheet-id expression to an MLBAM id (null when unknown)."""
    return retro_expr.replace_strict(id_map, default=None, return_dtype=pl.Int32)


def build_select_exprs(id_map: dict[str, int]) -> list[pl.Expr]:
    """Projection turning raw Retrosheet ``plays`` columns into :data:`PG_SCHEMA`.

    ``id_map`` maps ``key_retro`` -> ``key_mlbam`` (see :func:`chadwick_id_map`).
    """
    bat_side = (
        pl.when(pl.col("bathand") == "L")
        .then(pl.lit("L"))
        .when(pl.col("bathand") == "R")
        .then(pl.lit("R"))
        .when(pl.col("bathand") == "B")
        # Switch hitter: bats opposite the pitcher's throwing hand.
        .then(pl.when(pl.col("pithand") == "L").then(pl.lit("R")).otherwise(pl.lit("L")))
        .otherwise(None)
    )
    bat_home = pl.col("vis_home") == "1"

    exprs: list[pl.Expr] = [
        # grain / source
        pl.lit(SOURCE_LABEL).alias("source"),
        _txt("gid").alias("game_id"),
        _i("pn").cast(pl.Int32).alias("play_number"),
        _txt("pbp").alias("pbp_type"),
        # game context
        pl.col("date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias("game_date"),
        pl.col("date").str.strptime(pl.Date, "%Y%m%d", strict=False).dt.year().cast(pl.Int32).alias("season"),
        _txt("gametype").alias("game_type"),
        _i("inning").cast(pl.Int32).alias("inning"),
        pl.when(pl.col("top_bot") == "0").then(pl.lit("Top")).when(pl.col("top_bot") == "1").then(pl.lit("Bot")).otherwise(None).alias("inning_topbot"),
        bat_home.alias("bat_home"),
        _txt("batteam").alias("bat_team"),
        _txt("pitteam").alias("pit_team"),
        _txt("site").alias("site"),
        pl.when(bat_home).then(_i("score_h")).otherwise(_i("score_v")).cast(pl.Int32).alias("score_bat"),
        pl.when(bat_home).then(_i("score_v")).otherwise(_i("score_h")).cast(pl.Int32).alias("score_pit"),
        # players / handedness
        _mlbam(_txt("batter"), id_map).alias("batter_mlbam"),
        _txt("batter").alias("batter_retro"),
        _mlbam(_txt("pitcher"), id_map).alias("pitcher_mlbam"),
        _txt("pitcher").alias("pitcher_retro"),
        _txt("bathand").alias("bat_hand"),
        bat_side.alias("bat_side"),
        _txt("pithand").alias("pit_hand"),
        _i("lp").alias("lineup_pos"),
        _i("bat_f").alias("bat_field_pos"),
        _i("balls").alias("count_balls"),
        _i("strikes").alias("count_strikes"),
        # PA outcome flags
        _i("pa").alias("pa"),
        _i("ab").alias("ab"),
        (_i("single").fill_null(0) + _i("double").fill_null(0) + _i("triple").fill_null(0) + _i("hr").fill_null(0)).alias("hit"),
        _i("single").alias("single"),
        _i("double").alias("double"),
        _i("triple").alias("triple"),
        _i("hr").alias("home_run"),
        # Retrosheet `walk` includes IBB (`iw` is a subset); expose unintentional only.
        (_i("walk").fill_null(0) - _i("iw").fill_null(0)).alias("walk"),
        _i("iw").alias("intent_walk"),
        _i("hbp").alias("hit_by_pitch"),
        _i("k").alias("strikeout"),
        _i("sf").alias("sac_fly"),
        _i("sh").alias("sac_bunt"),
        _i("roe").alias("reached_on_error"),
        _i("fc").alias("fielders_choice"),
        _i("xi").alias("reached_on_interference"),
        _i("gdp").alias("gdp"),
        _i("othdp").alias("other_dp"),
        _i("tp").alias("triple_play"),
        _i("oth").alias("other_out"),
        # batted ball
        _i("bip").alias("ball_in_play"),
        _i("bunt").alias("bunt"),
        _i("ground").alias("ground_ball"),
        _i("fly").alias("fly_ball"),
        _i("line").alias("line_drive"),
        _txt("hittype").alias("hit_type"),
        _txt("loc").alias("hit_location"),
        # outs
        _i("outs_pre").alias("outs_pre"),
        _i("outs_post").alias("outs_post"),
        (_i("outs_post").fill_null(0) - _i("outs_pre").fill_null(0)).alias("outs_on_play"),
        # base state at start of play (runner present on each base)
        _txt("br1_pre").is_not_null().alias("on_1b"),
        _txt("br2_pre").is_not_null().alias("on_2b"),
        _txt("br3_pre").is_not_null().alias("on_3b"),
        # runs (play level)
        _i("runs").alias("runs_on_play"),
        # Pitcher-earned runs (the MLB-rules ERA basis) = team-earned `er` plus
        # `tur`, the runs unearned to the *team* but charged earned to a relief
        # pitcher (MLB Rule 9.16). Retrosheet encodes both, so this matches MLB's
        # official per-pitcher earned runs exactly — no MLB API needed.
        (_i("er").fill_null(0) + _i("tur").fill_null(0)).alias("earned_runs"),
        # Pitcher-unearned runs = runs not earned to the charged pitcher.
        (_i("runs").fill_null(0) - _i("er").fill_null(0) - _i("tur").fill_null(0)).alias("unearned_runs"),
        # Team-unearned runs (unearned to the team) = runs - team-earned `er`.
        (_i("runs").fill_null(0) - _i("er").fill_null(0)).alias("team_unearned_runs"),
        _i("rbi").alias("rbi"),
    ]

    # Per-run responsible-pitcher attribution (the basis for exact ERA). A scored
    # run is earned to its responsible pitcher unless its `ur*` flag is set; summed
    # over slots this equals `er + tur` (pitcher-earned), matching MLB Rule 9.16.
    for slot, (run_c, prun_c, ur_c) in _RUN_SLOTS.items():
        runner = _txt(run_c)
        scored = runner.is_not_null()
        earned = pl.when(scored).then(pl.lit(1) - _i(ur_c).fill_null(0)).otherwise(pl.lit(0)).cast(pl.Int16)
        # Responsible pitcher: explicit `prun*` if present, else the facing pitcher.
        resp_retro = pl.when(scored).then(pl.coalesce([_txt(prun_c), _txt("pitcher")])).otherwise(None)
        exprs.extend(
            [
                runner.alias(f"run_{slot}_runner_retro"),
                _mlbam(runner, id_map).alias(f"run_{slot}_runner_mlbam"),
                earned.alias(f"run_{slot}_earned"),
                resp_retro.alias(f"run_{slot}_pitcher_retro"),
                _mlbam(resp_retro, id_map).alias(f"run_{slot}_pitcher_mlbam"),
            ]
        )

    exprs.extend(
        [
            # baserunning / misc
            _i("wp").alias("wild_pitch"),
            _i("pb").alias("passed_ball"),
            _i("bk").alias("balk"),
            _i("sb2").alias("stolen_base_2b"),
            _i("sb3").alias("stolen_base_3b"),
            _i("sbh").alias("stolen_base_home"),
            _i("cs2").alias("caught_stealing_2b"),
            _i("cs3").alias("caught_stealing_3b"),
            _i("csh").alias("caught_stealing_home"),
            _i("pko1").alias("pickoff_1b"),
            _i("pko2").alias("pickoff_2b"),
            _i("pko3").alias("pickoff_3b"),
            _i("di").alias("defensive_indifference"),
            _i("oa").alias("other_advance"),
            sum((_i(f"e{n}").fill_null(0) for n in range(1, 10)), pl.lit(0)).cast(pl.Int16).alias("error_count"),
            _i("fle").alias("foul_error"),
            # raw passthrough
            _txt("event").alias("event_raw"),
        ]
    )
    return exprs


def validate_schema_consistency() -> None:
    """Assert :func:`build_select_exprs` emits exactly :data:`SCHEMA_COLUMNS`, in order."""
    names = [e.meta.output_name() for e in build_select_exprs({})]
    if names != SCHEMA_COLUMNS:
        extra = set(names) - set(SCHEMA_COLUMNS)
        missing = set(SCHEMA_COLUMNS) - set(names)
        raise AssertionError(
            "Polars projection drifted from PG_SCHEMA.\n"
            f"  order_mismatch={names != SCHEMA_COLUMNS and not (extra or missing)}\n"
            f"  in_projection_not_schema={sorted(extra)}\n"
            f"  in_schema_not_projection={sorted(missing)}"
        )


# ---------------------------------------------------------------------------
# Download / cache
# ---------------------------------------------------------------------------
def season_url(year: int) -> str:
    return PLAYS_URL.format(year=year)


def _extract_csv_from_zip(zip_path: Path, csv_path: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"{zip_path.name} contains no CSV")
        with zf.open(members[0]) as src:
            csv_path.write_bytes(src.read())
    logger.debug("Extracted {}", csv_path.name)
    return csv_path


_DOWNLOAD_CHUNK_BYTES = 1024 * 1024


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KB"
    if n < 1024**3:
        return f"{n / 1024**2:.1f} MB"
    return f"{n / 1024**3:.2f} GB"


def _print_download_progress(label: str, done: int, total: int | None) -> None:
    """Overwrite one stderr line with download progress (no extra dependencies)."""
    if total:
        pct = min(100.0, 100.0 * done / total)
        msg = f"\r  {label}: {pct:5.1f}% ({_format_bytes(done)} / {_format_bytes(total)})"
    else:
        msg = f"\r  {label}: {_format_bytes(done)}"
    sys.stderr.write(msg)
    sys.stderr.flush()


def _finish_download_progress() -> None:
    sys.stderr.write("\n")
    sys.stderr.flush()


def _download_url(url: str, dest: Path) -> None:
    req = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(req, timeout=_DOWNLOAD_TIMEOUT_SEC) as resp:
        total_raw = resp.headers.get("Content-Length")
        total = int(total_raw) if total_raw else None
        downloaded = 0
        label = dest.name
        with dest.open("wb") as out:
            while True:
                chunk = resp.read(_DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                _print_download_progress(label, downloaded, total)
        if downloaded:
            _finish_download_progress()


def download_all_plays(cache_dir: Path = DEFAULT_CACHE_DIR) -> Path:
    """Download + unzip Retrosheet's all-season ``plays.zip``; return the CSV path.

    Cached files are reused. The extracted CSV is written to ``plays.csv`` beside
    the zip under ``cache_dir``.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / "plays.csv"
    if csv_path.exists():
        logger.debug("Using cached {}", csv_path.name)
        return csv_path

    zip_path = cache_dir / "plays.zip"
    if not zip_path.exists():
        logger.info("Downloading {}", FULL_PLAYS_URL)
        _download_url(FULL_PLAYS_URL, zip_path)

    return _extract_csv_from_zip(zip_path, csv_path)


def download_season(year: int, cache_dir: Path = DEFAULT_CACHE_DIR) -> Path | None:
    """Download + unzip one season's ``{year}plays.csv``; return its path.

    Cached files are reused. Returns ``None`` if Retrosheet has no file for
    ``year`` (HTTP 404) so callers can skip in-progress / pre-1903 seasons.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / f"{year}plays.csv"
    if csv_path.exists():
        logger.debug("Using cached {}", csv_path.name)
        return csv_path

    zip_path = cache_dir / f"{year}plays.zip"
    if not zip_path.exists():
        url = season_url(year)
        logger.info("Downloading {}", url)
        try:
            _download_url(url, zip_path)
        except HTTPError as exc:
            if exc.code == 404:
                logger.warning("No Retrosheet plays file for {} (404); skipping", year)
                return None
            raise

    return _extract_csv_from_zip(zip_path, csv_path)


# ---------------------------------------------------------------------------
# Chadwick Bureau Register ID map (Retrosheet -> MLBAM)
# ---------------------------------------------------------------------------
def chadwick_id_map() -> dict[str, int]:
    """``key_retro`` -> ``key_mlbam`` from the Chadwick Bureau Register.

    Uses :func:`pybaseball.chadwick_register` (already a project dependency),
    which downloads + caches the register. Rows without an MLBAM id (the register
    fills these with ``-1``) are dropped, so players predating MLBAM ids simply
    map to null.
    """
    from pybaseball import chadwick_register  # heavy import; keep local

    reg = chadwick_register(save=True)
    frame = pl.from_pandas(reg[["key_retro", "key_mlbam"]])
    frame = frame.filter(
        pl.col("key_retro").is_not_null()
        & (pl.col("key_retro") != "")
        & pl.col("key_mlbam").is_not_null()
        & (pl.col("key_mlbam") != -1)
    )
    id_map = dict(zip(frame["key_retro"].to_list(), frame["key_mlbam"].cast(pl.Int64).to_list()))
    logger.info("Loaded {} Retrosheet->MLBAM id mappings from Chadwick register", len(id_map))
    return id_map


# ---------------------------------------------------------------------------
# Clean / build
# ---------------------------------------------------------------------------
def clean_season(
    csv_path: Path,
    id_map: dict[str, int],
    *,
    game_types: Iterable[str] | None = ("regular",),
) -> pl.LazyFrame:
    """Lazily read one season CSV and project it onto :data:`PG_SCHEMA`.

    ``game_types`` filters Retrosheet ``gametype`` (e.g. ``regular``, ``worldseries``);
    pass ``None`` to keep every game type.
    """
    # infer_schema_length=0 reads every column as Utf8, so the 177-column file
    # never trips type inference; build_select_exprs casts explicitly.
    lf = pl.scan_csv(csv_path, infer_schema_length=0)
    lf = lf.select(build_select_exprs(id_map))
    if game_types is not None:
        lf = lf.filter(pl.col("game_type").is_in(list(game_types)))
    return lf


def build_dataset(
    years: Iterable[int] | None = None,
    *,
    use_full_bundle: bool = False,
    id_map: dict[str, int] | None = None,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    game_types: Iterable[str] | None = ("regular",),
) -> pl.LazyFrame:
    """Download + clean plays and return one concatenated frame.

    With ``use_full_bundle``, downloads Retrosheet's all-season ``plays.zip``
    instead of per-year archives. ``years`` is ignored in that mode.
    """
    validate_schema_consistency()
    resolved_map = id_map if id_map is not None else chadwick_id_map()
    if use_full_bundle:
        csv_path = download_all_plays(cache_dir=cache_dir)
        return clean_season(csv_path, resolved_map, game_types=game_types)
    if years is None:
        raise ValueError("years is required when use_full_bundle is False")
    frames: list[pl.LazyFrame] = []
    for year in years:
        csv_path = download_season(year, cache_dir=cache_dir)
        if csv_path is None:
            continue
        frames.append(clean_season(csv_path, resolved_map, game_types=game_types))
    if not frames:
        raise ValueError("No season data was available for the requested years")
    return pl.concat(frames, how="vertical")


def write_dataset(
    lf: pl.LazyFrame,
    output: Path,
    *,
    partition_by_season: bool = False,
) -> Path:
    """Materialize ``lf`` to Parquet (streaming). Returns the output path.

    With ``partition_by_season`` the output is a Hive-partitioned directory
    (``season=YYYY/``) instead of a single file — useful for very large runs.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    if partition_by_season:
        df = lf.collect(streaming=True)
        df.write_parquet(output, partition_by=["season"])
        logger.info("Wrote {} rows to {} (partitioned by season)", df.height, output)
    else:
        lf.sink_parquet(output)
        logger.info("Wrote dataset to {}", output)
    return output


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
def create_table_ddl(table_name: str = TABLE_NAME) -> str:
    """``CREATE TABLE`` matching the emitted Parquet, derived from :data:`PG_SCHEMA`."""
    lines = [f'    "{name}" {pgtype}' for name, pgtype in PG_SCHEMA]
    body = ",\n".join(lines)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"{body},\n"
        f"    PRIMARY KEY (source, game_id, play_number)\n"
        f");\n"
    )


# Secondary indexes for the common access patterns: per-season pitcher/batter
# aggregates and handedness-matchup splits.
INDEX_SPECS: list[tuple[str, str]] = [
    ("season_pitcher", "season, pitcher_mlbam"),
    ("season_batter", "season, batter_mlbam"),
    ("matchup", "bat_side, pit_hand"),
]


def create_index_ddl(table_name: str = TABLE_NAME) -> str:
    """``CREATE INDEX`` statements for :data:`INDEX_SPECS`."""
    return "".join(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{suffix} ON {table_name} ({cols});\n"
        for suffix, cols in INDEX_SPECS
    )


def full_ddl(table_name: str = TABLE_NAME) -> str:
    """Table DDL followed by the secondary index DDL."""
    return create_table_ddl(table_name) + "\n" + create_index_ddl(table_name)


# ---------------------------------------------------------------------------
# Warehouse load
# ---------------------------------------------------------------------------
# Supported load destinations. ``postgres`` is the production warehouse (psycopg2
# + ADBC); ``duckdb`` covers both a local DuckDB file and MotherDuck, which is
# just DuckDB reached over an ``md:`` connection string (token via the
# ``motherduck_token`` env var). The DDL in :data:`PG_SCHEMA` is engine-portable —
# DuckDB accepts the same ``TEXT``/``SMALLINT``/``BOOLEAN`` types and ``PRIMARY KEY``.
POSTGRES_BACKEND = "postgres"
DUCKDB_BACKEND = "duckdb"


def _run_sql(database_url: str, statements: str) -> None:
    import psycopg2

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(statements)
        conn.commit()


def _duckdb_connect(connection: str) -> Any:
    """Open a DuckDB connection. ``connection`` is a file path or an ``md:`` URI.

    For MotherDuck (``md:`` / ``md:dbname``) DuckDB reads the access token from the
    ``motherduck_token`` environment variable and auto-loads the extension.
    """
    import duckdb

    return duckdb.connect(connection)


def _parquet_source_sql(parquet_path: Path) -> str:
    """``read_parquet(...)`` expression for a single file or a season-partitioned dir."""
    literal = str(parquet_path).replace("'", "''")
    if parquet_path.is_dir():
        # Hive-partitioned (``season=YYYY/``) output: restore ``season`` from the path.
        return f"read_parquet('{literal}/**/*.parquet', hive_partitioning = true)"
    return f"read_parquet('{literal}')"


def ensure_table(
    connection: str, *, table_name: str = TABLE_NAME, backend: str = POSTGRES_BACKEND
) -> None:
    """Create the table (without indexes) if it does not yet exist."""
    ddl = create_table_ddl(table_name)
    if backend == DUCKDB_BACKEND:
        con = _duckdb_connect(connection)
        try:
            con.execute(ddl)
        finally:
            con.close()
    else:
        _run_sql(connection, ddl)
    logger.info("Ensured table {} exists", table_name)


def create_indexes(
    connection: str, *, table_name: str = TABLE_NAME, backend: str = POSTGRES_BACKEND
) -> None:
    """Create the secondary indexes (idempotent).

    Indexes back the per-season pitcher/batter scans on the 2 GB Postgres VPS. The
    columnar DuckDB/MotherDuck engine doesn't need them, so they are skipped there.
    """
    if backend == DUCKDB_BACKEND:
        logger.info("Skipping secondary indexes for DuckDB/MotherDuck target {}", table_name)
        return
    _run_sql(connection, create_index_ddl(table_name))
    logger.info("Ensured indexes on {}: {}", table_name, ", ".join(s for s, _ in INDEX_SPECS))


def load_parquet_to_db(
    parquet_path: Path,
    connection: str,
    *,
    table_name: str = TABLE_NAME,
    replace_source: str | None = None,
    backend: str = POSTGRES_BACKEND,
) -> int:
    """Append a Parquet dataset into ``table_name``. Returns rows written.

    The table must already exist (see :func:`ensure_table`); column order/types
    match :data:`PG_SCHEMA`. ``replace_source`` first deletes existing rows with that
    ``source`` value (e.g. ``"retrosheet"``) so a corrected rebuild cleanly replaces
    the prior load.

    Postgres loads via Polars + ADBC (``adbc-driver-postgresql``) one season at a
    time (bounded memory). DuckDB/MotherDuck reads the Parquet natively in a single
    ``INSERT ... SELECT`` (``connection`` is a file path or ``md:`` URI).
    """
    if backend == DUCKDB_BACKEND:
        return _load_parquet_duckdb(
            parquet_path, connection, table_name=table_name, replace_source=replace_source
        )
    if replace_source is not None:
        import psycopg2

        with psycopg2.connect(connection) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table_name} WHERE source = %s", (replace_source,)
                )
            conn.commit()
        logger.info("Deleted existing {} rows with source={!r}", table_name, replace_source)
    lf = pl.scan_parquet(parquet_path)
    seasons = lf.select(pl.col("season").unique()).collect().to_series().sort().to_list()
    total = 0
    for season in seasons:
        df = lf.filter(pl.col("season") == season).collect()
        df.write_database(table_name, connection=connection, engine="adbc", if_table_exists="append")
        total += df.height
        logger.info("Loaded season {} ({} rows; {} total)", season, df.height, total)
    return total


def _load_parquet_duckdb(
    parquet_path: Path,
    connection: str,
    *,
    table_name: str = TABLE_NAME,
    replace_source: str | None = None,
) -> int:
    """Load a Parquet into a DuckDB/MotherDuck table via native ``read_parquet``."""
    source_sql = _parquet_source_sql(parquet_path)
    # Select by explicit column name so physical Parquet order (and a restored
    # ``season`` partition column) can't shift values into the wrong columns.
    cols = ", ".join(f'"{c}"' for c in SCHEMA_COLUMNS)
    con = _duckdb_connect(connection)
    try:
        if replace_source is not None:
            con.execute(f"DELETE FROM {table_name} WHERE source = ?", [replace_source])
            logger.info("Deleted existing {} rows with source={!r}", table_name, replace_source)
        con.execute(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM {source_sql}")
        total = con.execute(f"SELECT count(*) FROM {source_sql}").fetchone()[0]
    finally:
        con.close()
    logger.info("Loaded {} rows into {}", total, table_name)
    return total


def season_range(start: int | None, end: int | None) -> list[int]:
    """Inclusive ``[start, end]`` clamped to the available Retrosheet range."""
    start = EARLIEST_SEASON if start is None else max(start, EARLIEST_SEASON)
    end = datetime.now().year if end is None else end
    if end < start:
        raise ValueError(f"end-year ({end}) precedes start-year ({start})")
    return list(range(start, end + 1))
