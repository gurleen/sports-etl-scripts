"""Polars port of the dbt PBP marts, run against the HF-published Parquet files.

Ports everything under ``dbt/models/{staging,intermediate,marts}`` that depends on
``retrosheet_plays``/``mlb_schedule`` EXCEPT ``statcast_events`` and
``abs_challenges`` (those stay dbt-only; see ``dbt/models/marts/``). ``games`` /
``game_coverage`` / ``daily_game_coverage`` originally compared the schedule
against the raw ``statcast`` pitch table, which predates the Hub-published
``retrosheet_plays`` pbp data and isn't published to the Hub at all. The coverage
marts here compare the schedule against pbp coverage instead (season- and
day-grain game counts — historical Retrosheet-sourced rows don't carry an MLBAM
``game_pk``, so per-game_pk matching only works for mlbam-sourced seasons and
isn't attempted here).

Reference data:

* ``weights`` (per-season wOBA/FIP constants) — read from ``weights.parquet`` on
  the Hub, published by ``etl hf weights`` from a manually-downloaded FanGraphs
  guts-data CSV. FanGraphs' guts API sits behind a Cloudflare challenge that
  blocks datacenter IPs (including GitHub Actions runners), so this can't be
  fetched automatically in CI -- see ``weights_from_fangraphs_csv`` below.
* ``players`` (id -> full_name) — read from the Chadwick Bureau Register via
  ``pybaseball.chadwick_register()`` (already used by ``etl_scripts.retrosheet``
  for id mapping), not a Postgres ``players`` table.

Entry point: ``run_all(work_dir, upload=...)``, wired up as ``etl hf marts``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

# FanGraphs guts-data CSV column -> weights schema column (see
# https://www.fangraphs.com/guts.aspx?type=cn).
_WEIGHTS_CSV_RENAME: dict[str, str] = {
    "Season": "game_year",
    "wOBA": "league_woba",
    "wOBAScale": "woba_scale",
    "wBB": "w_bb",
    "wHBP": "w_hbp",
    "w1B": "w_single",
    "w2B": "w_double",
    "w3B": "w_triple",
    "wHR": "w_home_run",
    "runSB": "run_sb",
    "runCS": "run_cs",
    "R/PA": "runs_per_pa",
    "R/W": "r_w_ratio",
    "cFIP": "c_fip",
}


def weights_from_fangraphs_csv(csv_path: Path) -> pl.DataFrame:
    """Parse a FanGraphs guts-data CSV export into the ``weights`` schema.

    Download the CSV yourself from https://www.fangraphs.com/guts.aspx?type=cn
    (the underlying API is Cloudflare-gated against datacenter IPs, so this step
    is manual) and pass it to ``etl hf weights``. Re-run whenever a new season's
    constants are published -- the export is cumulative (every season back to
    1871), so each run fully replaces ``weights.parquet``.
    """
    df = pl.read_csv(csv_path)
    missing = set(_WEIGHTS_CSV_RENAME) - set(df.columns)
    if missing:
        raise ValueError(f"weights CSV missing expected column(s): {sorted(missing)}")
    return (
        df.rename(_WEIGHTS_CSV_RENAME)
        .select(list(_WEIGHTS_CSV_RENAME.values()))
        .with_columns(pl.col("game_year").cast(pl.Int64))
        .sort("game_year")
    )


def load_weights(work_dir: Path) -> pl.DataFrame:
    """Per-season wOBA/FIP constants from ``weights.parquet`` on the Hub."""
    from etl_scripts import hf_sync

    path = hf_sync.download_existing("weights.parquet", work_dir)
    if path is None:
        raise RuntimeError(
            "weights.parquet not found on the Hub; run `etl hf weights <csv>` first "
            "(download the CSV from https://www.fangraphs.com/guts.aspx?type=cn)."
        )
    return pl.read_parquet(path)


def load_players() -> pl.DataFrame:
    """``player_id`` (MLBAM) -> ``full_name``, from the Chadwick Bureau Register."""
    from pybaseball import chadwick_register

    reg = chadwick_register(save=True)
    df = pl.from_pandas(reg[["key_mlbam", "name_first", "name_last"]]).drop_nulls("key_mlbam")
    df = (
        df.with_columns(
            pl.col("key_mlbam").cast(pl.Int64).alias("player_id"),
            (pl.col("name_first").fill_null("") + " " + pl.col("name_last").fill_null(""))
            .str.strip_chars()
            .alias("full_name"),
        )
        .select("player_id", "full_name")
        .unique(subset=["player_id"], keep="last")
    )
    return df


_WEIGHT_COLUMNS = [
    "game_year", "w_bb", "w_hbp", "w_single", "w_double", "w_triple", "w_home_run",
    "league_woba", "woba_scale", "runs_per_pa", "c_fip",
]


# ---------------------------------------------------------------------------
# Staging: stg_pbp__events.sql
# ---------------------------------------------------------------------------


def load_pbp_events(pbp: pl.DataFrame) -> pl.DataFrame:
    """One row per play, regular season only, source-deduped and split dims derived.

    ``retrosheet_plays`` holds both 'retrosheet' and 'mlbam' sources and they
    overlap on completed seasons, so pick ONE source per season: retrosheet for
    any season it covers (the finalized historical record), mlbam only for
    seasons retrosheet doesn't (the in-progress current season).
    """
    raw = pbp.filter(pl.col("game_type") == "regular")
    retro_seasons = raw.filter(pl.col("source") == "retrosheet")["season"].unique().to_list()
    src = raw.filter((pl.col("source") == "retrosheet") | (~pl.col("season").is_in(retro_seasons)))

    return src.select(
        pl.col("source"),
        pl.col("game_id"),
        pl.col("play_number"),
        pl.col("game_date"),
        pl.col("season"),
        pl.col("game_date").dt.month().alias("game_month"),
        pl.col("batter_mlbam"),
        pl.col("pitcher_mlbam"),
        pl.col("bat_team"),
        pl.col("bat_side"),
        pl.col("pit_hand"),
        pl.when(pl.col("bat_home")).then(pl.lit("home")).otherwise(pl.lit("away")).alias("home_away"),
        pl.col("outs_pre"),
        (pl.col("count_balls").cast(pl.Utf8) + "-" + pl.col("count_strikes").cast(pl.Utf8)).alias("count_state"),
        pl.col("inning"),
        pl.when(pl.col("inning") >= 10)
        .then(pl.lit("10+"))
        .when(pl.col("inning") >= 7)
        .then(pl.lit("7-9"))
        .when(pl.col("inning") >= 4)
        .then(pl.lit("4-6"))
        .otherwise(pl.lit("1-3"))
        .alias("inning_bucket"),
        (
            (pl.col("inning") >= 7)
            & ((pl.col("score_bat").fill_null(0) - pl.col("score_pit").fill_null(0)).abs() <= 1)
        ).alias("late_close"),
        pl.when(pl.col("on_1b") & pl.col("on_2b") & pl.col("on_3b"))
        .then(pl.lit("loaded"))
        .when(pl.col("on_2b") | pl.col("on_3b"))
        .then(pl.lit("risp"))
        .when(pl.col("on_1b"))
        .then(pl.lit("men_on"))
        .otherwise(pl.lit("empty"))
        .alias("base_state"),
        pl.col("pa"),
        pl.col("ab"),
        pl.col("hit").alias("h"),
        pl.col("single").alias("singles"),
        pl.col("double").alias("doubles"),
        pl.col("triple").alias("triples"),
        pl.col("home_run").alias("hr"),
        pl.col("walk").alias("ubb"),
        (pl.col("walk") + pl.col("intent_walk")).alias("bb"),
        pl.col("intent_walk").alias("ibb"),
        pl.col("hit_by_pitch").alias("hbp"),
        pl.col("strikeout").alias("so"),
        pl.col("sac_fly").alias("sf"),
        pl.col("sac_bunt").alias("sh"),
        pl.col("outs_on_play").alias("outs"),
        pl.col("runs_on_play").alias("runs"),
        pl.col("earned_runs").alias("er"),
        pl.col("run_b_pitcher_mlbam"),
        pl.col("run_b_earned"),
        pl.col("run_1_pitcher_mlbam"),
        pl.col("run_1_earned"),
        pl.col("run_2_pitcher_mlbam"),
        pl.col("run_2_earned"),
        pl.col("run_3_pitcher_mlbam"),
        pl.col("run_3_earned"),
    )


# ---------------------------------------------------------------------------
# Intermediate: int_pbp__team_league.sql / int_pitching__responsible_er.sql
# ---------------------------------------------------------------------------

_AL_STABLE = [
    "ANA", "ATH", "OAK", "BAL", "BOS", "CHA", "CLE", "DET", "KCA",
    "MIN", "NYA", "SEA", "TBA", "TEX", "TOR",
]
_NL_STABLE = [
    "ARI", "ATL", "CHN", "CIN", "COL", "LAN", "MIA", "FLO", "MON", "NYN",
    "PHI", "PIT", "SDN", "SFN", "SLN", "WAS",
]


def build_team_league(events: pl.DataFrame) -> pl.DataFrame:
    """Season-aware Retrosheet-franchise-code -> league (AL/NL) bridge.

    HOU switched NL->AL in 2013, MIL switched AL->NL in 1998; both handled
    explicitly. OAK/ATH (Athletics, relocated 2025), FLO/MIA (Marlins, renamed
    2012), and MON/WAS (Expos->Nationals, relocated 2005) are the same
    franchise under an old and a new code -- both codes map to the one league
    each franchise has always played in, so no season split is needed. Codes
    not enumerated resolve to null (propagates to a null wrc_plus rather than
    a wrong league) -- add them here as older seasons are loaded.
    """
    team_seasons = (
        events.filter(pl.col("bat_team").is_not_null())
        .select(pl.col("bat_team").alias("team"), "season")
        .unique()
    )
    league = (
        pl.when((pl.col("team") == "HOU") & (pl.col("season") >= 2013))
        .then(pl.lit("AL"))
        .when(pl.col("team") == "HOU")
        .then(pl.lit("NL"))
        .when((pl.col("team") == "MIL") & (pl.col("season") >= 1998))
        .then(pl.lit("NL"))
        .when(pl.col("team") == "MIL")
        .then(pl.lit("AL"))
        .when(pl.col("team").is_in(_AL_STABLE))
        .then(pl.lit("AL"))
        .when(pl.col("team").is_in(_NL_STABLE))
        .then(pl.lit("NL"))
        .otherwise(None)
        .alias("league")
    )
    return team_seasons.with_columns(league)


def build_responsible_er(events: pl.DataFrame) -> pl.DataFrame:
    """Earned runs charged to the responsible pitcher, per (pitcher, season, month).

    Summing run_*_earned by run_*_pitcher_mlbam matches MLB's official
    per-pitcher ER (er + tur / Rule 9.16 inherited runners).
    """
    slots = pl.concat(
        [
            events.select(
                "season", "game_month",
                pl.col(f"run_{s}_pitcher_mlbam").alias("pitcher_mlbam"),
                pl.col(f"run_{s}_earned").alias("er"),
            )
            for s in ("b", "1", "2", "3")
        ]
    )
    return (
        slots.filter(pl.col("pitcher_mlbam").is_not_null())
        .group_by("pitcher_mlbam", "season", "game_month")
        .agg(pl.col("er").sum())
    )


# ---------------------------------------------------------------------------
# Shared rate-stat expressions: macros/pbp_rate_stats.sql
# ---------------------------------------------------------------------------


def _batting_rate_exprs() -> list[pl.Expr]:
    """Expects ab, h, bb, hbp, sf, singles, doubles, triples, hr, pa, so, ubb,
    and the w_* / weight columns already joined in."""
    ab, h, bb, hbp, sf = pl.col("ab"), pl.col("h"), pl.col("bb"), pl.col("hbp"), pl.col("sf")
    singles, doubles, triples, hr = pl.col("singles"), pl.col("doubles"), pl.col("triples"), pl.col("hr")
    pa, so, ubb = pl.col("pa"), pl.col("so"), pl.col("ubb")
    obp_denom = ab + bb + hbp + sf
    obp_val = (h + bb + hbp) / obp_denom
    slg_val = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab
    woba_denom = ab + ubb + sf + hbp
    woba_num = (
        pl.col("w_bb") * ubb + pl.col("w_hbp") * hbp + pl.col("w_single") * singles
        + pl.col("w_double") * doubles + pl.col("w_triple") * triples + pl.col("w_home_run") * hr
    )
    return [
        pl.when(ab > 0).then(h / ab).round(3).alias("avg"),
        pl.when(obp_denom > 0).then(obp_val).round(3).alias("obp"),
        pl.when(ab > 0).then(slg_val).round(3).alias("slg"),
        pl.when(ab > 0)
        .then(slg_val + pl.when(obp_denom > 0).then(obp_val).otherwise(0.0))
        .round(3)
        .alias("ops"),
        pl.when(ab > 0).then((doubles + 2 * triples + 3 * hr) / ab).round(3).alias("iso"),
        pl.when((ab - so - hr + sf) > 0).then((h - hr) / (ab - so - hr + sf)).round(3).alias("babip"),
        pl.when(pa > 0).then(bb / pa).round(3).alias("bb_pct"),
        pl.when(pa > 0).then(so / pa).round(3).alias("k_pct"),
        pl.when(woba_denom > 0).then(woba_num / woba_denom).round(3).alias("woba"),
    ]


def _pitching_rate_exprs() -> list[pl.Expr]:
    """Expects bf, ab, outs, h, bb, ubb, hbp, hr, so, er, and c_fip joined in."""
    ab, h, bb, ubb, hbp, hr, so, bf, outs, er = (
        pl.col("ab"), pl.col("h"), pl.col("bb"), pl.col("ubb"), pl.col("hbp"),
        pl.col("hr"), pl.col("so"), pl.col("bf"), pl.col("outs"), pl.col("er"),
    )
    ip = outs / 3.0
    return [
        ip.round(1).alias("ip"),
        pl.when(ab > 0).then(h / ab).round(3).alias("baa"),
        pl.when(outs > 0).then(9.0 * er / ip).round(2).alias("era"),
        pl.when(outs > 0).then((bb + h) / ip).round(3).alias("whip"),
        pl.when(outs > 0).then(9.0 * so / ip).round(2).alias("k9"),
        pl.when(outs > 0).then(9.0 * bb / ip).round(2).alias("bb9"),
        pl.when(outs > 0).then(9.0 * hr / ip).round(2).alias("hr9"),
        pl.when(bf > 0).then(so / bf).round(3).alias("k_pct"),
        pl.when(bf > 0).then(bb / bf).round(3).alias("bb_pct"),
        pl.when(outs > 0)
        .then(((13 * hr + 3 * (ubb + hbp) - 2 * so) / ip) + pl.col("c_fip"))
        .round(2)
        .alias("fip"),
    ]


def _games_per_team(events: pl.DataFrame) -> pl.DataFrame:
    """``round(2.0 * distinct games / 30.0)`` per season, matching the dbt marts' estimate."""
    return events.group_by("season").agg(
        (2.0 * pl.col("game_id").n_unique() / 30.0).round(0).alias("games_per_team")
    )


# ---------------------------------------------------------------------------
# Marts: batting_stats_season.sql / pitching_stats_season.sql
# ---------------------------------------------------------------------------


def batting_stats_season(events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Standard + sabermetric batting line per player per season, with wRC+.

    wRC+ is computed against a per-league (AL/NL), per-season baseline; a
    traded player's own line is compared against the league where he took the
    most PA. Park factor is dropped (defaults to league-average), so this is
    the park-neutral wRC+ = (wRC/PA) / (lg wRC/PA) * 100.
    """
    tl = build_team_league(events)
    events_lg = events.join(tl, left_on=["bat_team", "season"], right_on=["team", "season"], how="left")
    w = weights.select(_WEIGHT_COLUMNS)

    totals = (
        events_lg.filter(pl.col("batter_mlbam").is_not_null())
        .group_by("batter_mlbam", "season")
        .agg(
            pl.col("pa").sum(), pl.col("ab").sum(), pl.col("h").sum(),
            pl.col("singles").sum(), pl.col("doubles").sum(), pl.col("triples").sum(), pl.col("hr").sum(),
            pl.col("ubb").sum(), pl.col("bb").sum(), pl.col("ibb").sum(), pl.col("hbp").sum(),
            pl.col("so").sum(), pl.col("sf").sum(),
        )
        .rename({"batter_mlbam": "player_id"})
    )

    season_games = _games_per_team(events_lg)

    per_league_pa = (
        events_lg.filter(pl.col("batter_mlbam").is_not_null())
        .group_by("batter_mlbam", "season", "league")
        .agg(pl.col("pa").sum().alias("league_pa"))
    )
    player_league = (
        per_league_pa.sort(
            by=["batter_mlbam", "season", "league_pa", "league"],
            descending=[False, False, True, False],
        )
        .group_by(["batter_mlbam", "season"], maintain_order=True)
        .agg(pl.col("league").first())
        .rename({"batter_mlbam": "player_id"})
    )

    def _wrc_expr() -> pl.Expr:
        denom = pl.col("ab") + pl.col("ubb") + pl.col("sf") + pl.col("hbp")
        num = (
            pl.col("w_bb") * pl.col("ubb") + pl.col("w_hbp") * pl.col("hbp") + pl.col("w_single") * pl.col("singles")
            + pl.col("w_double") * pl.col("doubles") + pl.col("w_triple") * pl.col("triples")
            + pl.col("w_home_run") * pl.col("hr")
        )
        return pl.when((pl.col("pa") > 0) & (denom > 0)).then(
            ((num / denom - pl.col("league_woba")) / pl.col("woba_scale") + pl.col("runs_per_pa")) * pl.col("pa")
        )

    wrc_df = (
        totals.join(w, left_on="season", right_on="game_year", how="left")
        .with_columns(_wrc_expr().alias("wrc_raw"))
        .select("player_id", "season", "wrc_raw")
    )

    league_totals = (
        events_lg.filter(pl.col("league").is_not_null())
        .group_by("season", "league")
        .agg(
            pl.col("pa").sum(), pl.col("ab").sum(), pl.col("singles").sum(), pl.col("doubles").sum(),
            pl.col("triples").sum(), pl.col("hr").sum(), pl.col("ubb").sum(), pl.col("hbp").sum(),
            pl.col("sf").sum(),
        )
    )
    # League baseline: runs created per PA (same formula as _wrc_expr but as a
    # per-PA rate on league totals, not a scaled total -- no trailing "* pa").
    league_wrc = league_totals.join(w, left_on="season", right_on="game_year", how="left")
    league_wrc = league_wrc.with_columns(
        pl.when((pl.col("ab") + pl.col("ubb") + pl.col("sf") + pl.col("hbp")) > 0)
        .then(
            (
                (
                    pl.col("w_bb") * pl.col("ubb") + pl.col("w_hbp") * pl.col("hbp")
                    + pl.col("w_single") * pl.col("singles") + pl.col("w_double") * pl.col("doubles")
                    + pl.col("w_triple") * pl.col("triples") + pl.col("w_home_run") * pl.col("hr")
                )
                / (pl.col("ab") + pl.col("ubb") + pl.col("sf") + pl.col("hbp"))
                - pl.col("league_woba")
            )
            / pl.col("woba_scale")
            + pl.col("runs_per_pa")
        )
        .alias("wrc_per_pa")
    ).select("season", "league", "wrc_per_pa")

    out = (
        totals.join(w, left_on="season", right_on="game_year", how="left")
        .join(season_games, on="season", how="left")
        .join(player_league, on=["player_id", "season"], how="left")
        .join(wrc_df, on=["player_id", "season"], how="left")
    )
    out = out.with_columns(
        (pl.col("pa") >= 3.1 * pl.col("games_per_team")).alias("qualified"),
        *_batting_rate_exprs(),
    )
    out = out.join(league_wrc, on=["season", "league"], how="left")
    out = out.with_columns(
        pl.col("wrc_raw").round(0).cast(pl.Int64).alias("wrc"),
        pl.when((pl.col("pa") > 0) & pl.col("wrc_per_pa").is_not_null() & (pl.col("wrc_per_pa") != 0))
        .then((pl.col("wrc_raw") / pl.col("pa") / pl.col("wrc_per_pa") * 100).round(0).cast(pl.Int64))
        .alias("wrc_plus"),
    )
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "league", "full_name",
        "pa", "ab", "h", "singles", "doubles", "triples", "hr", "bb", "ubb", "ibb", "hbp", "so", "sf",
        "qualified", "avg", "obp", "slg", "ops", "iso", "babip", "bb_pct", "k_pct", "woba", "wrc", "wrc_plus",
    )


def pitching_stats_season(
    events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame, responsible_er: pl.DataFrame
) -> pl.DataFrame:
    """Standard + sabermetric pitching line per pitcher per season.

    Counting stats use the facing pitcher; ER/ERA use responsible-pitcher
    attribution via the run_*_earned slots (official MLB basis).
    """
    w = weights.select(_WEIGHT_COLUMNS)

    totals = (
        events.filter(pl.col("pitcher_mlbam").is_not_null())
        .group_by("pitcher_mlbam", "season")
        .agg(
            pl.col("pa").sum().alias("bf"), pl.col("ab").sum(), pl.col("h").sum(), pl.col("hr").sum(),
            pl.col("ubb").sum(), pl.col("bb").sum(), pl.col("ibb").sum(), pl.col("hbp").sum(),
            pl.col("so").sum(), pl.col("outs").sum(), pl.col("runs").sum().alias("r"),
        )
        .rename({"pitcher_mlbam": "player_id"})
    )
    er_totals = (
        responsible_er.group_by("pitcher_mlbam", "season")
        .agg(pl.col("er").sum())
        .rename({"pitcher_mlbam": "player_id"})
    )
    season_games = _games_per_team(events)

    out = (
        totals.join(er_totals, on=["player_id", "season"], how="left")
        .with_columns(pl.col("er").fill_null(0))
        .join(w, left_on="season", right_on="game_year", how="left")
        .join(season_games, on="season", how="left")
    )
    out = out.with_columns(
        (pl.col("outs") / 3.0 >= pl.col("games_per_team")).alias("qualified"),
        *_pitching_rate_exprs(),
    )
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "full_name",
        "bf", "h", "hr", "bb", "ubb", "ibb", "hbp", "so", "r", "er", "qualified",
        "ip", "baa", "era", "whip", "k9", "bb9", "hr9", "k_pct", "bb_pct", "fip",
    )


# ---------------------------------------------------------------------------
# Marts: batting_stats_monthly.sql / pitching_stats_monthly.sql
# ---------------------------------------------------------------------------


def batting_stats_monthly(events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Batting line per player per season per calendar month."""
    w = weights.select(_WEIGHT_COLUMNS)
    totals = (
        events.filter(pl.col("batter_mlbam").is_not_null())
        .group_by("batter_mlbam", "season", "game_month")
        .agg(
            pl.col("pa").sum(), pl.col("ab").sum(), pl.col("h").sum(),
            pl.col("singles").sum(), pl.col("doubles").sum(), pl.col("triples").sum(), pl.col("hr").sum(),
            pl.col("ubb").sum(), pl.col("bb").sum(), pl.col("ibb").sum(), pl.col("hbp").sum(),
            pl.col("so").sum(), pl.col("sf").sum(),
        )
        .rename({"batter_mlbam": "player_id"})
    )
    out = totals.join(w, left_on="season", right_on="game_year", how="left").with_columns(*_batting_rate_exprs())
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "game_month", "full_name",
        "pa", "ab", "h", "doubles", "triples", "hr", "bb", "so",
        "avg", "obp", "slg", "ops", "iso", "babip", "bb_pct", "k_pct", "woba",
    )


def pitching_stats_monthly(
    events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame, responsible_er: pl.DataFrame
) -> pl.DataFrame:
    """Pitching line per pitcher per season per calendar month.

    ER uses responsible-pitcher attribution; R uses facing pitcher.
    """
    w = weights.select(_WEIGHT_COLUMNS)
    totals = (
        events.filter(pl.col("pitcher_mlbam").is_not_null())
        .group_by("pitcher_mlbam", "season", "game_month")
        .agg(
            pl.col("pa").sum().alias("bf"), pl.col("ab").sum(), pl.col("h").sum(), pl.col("hr").sum(),
            pl.col("ubb").sum(), pl.col("bb").sum(), pl.col("ibb").sum(), pl.col("hbp").sum(),
            pl.col("so").sum(), pl.col("outs").sum(), pl.col("runs").sum().alias("r"),
        )
        .rename({"pitcher_mlbam": "player_id"})
    )
    er = responsible_er.rename({"pitcher_mlbam": "player_id"})
    out = (
        totals.join(er, on=["player_id", "season", "game_month"], how="left")
        .with_columns(pl.col("er").fill_null(0))
        .join(w, left_on="season", right_on="game_year", how="left")
    )
    out = out.with_columns(*_pitching_rate_exprs())
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "game_month", "full_name",
        "bf", "h", "hr", "bb", "so", "r", "er",
        "ip", "baa", "era", "whip", "k9", "bb9", "hr9", "k_pct", "bb_pct", "fip",
    )


# ---------------------------------------------------------------------------
# Marts: batting_splits_season.sql / pitching_splits_season.sql
# ---------------------------------------------------------------------------

_SPLIT_AGG_COLS = ("pa", "ab", "h", "singles", "doubles", "triples", "hr", "ubb", "bb", "ibb", "hbp", "so", "sf")

_BATTING_SPLITS: list[tuple[str, pl.Expr, pl.Expr]] = [
    ("overall", pl.lit("all"), pl.lit(True)),
    ("vs_hand", pl.col("pit_hand"), pl.col("pit_hand").is_not_null()),
    ("home_away", pl.col("home_away"), pl.lit(True)),
    ("outs", pl.col("outs_pre").cast(pl.Utf8), pl.col("outs_pre").is_not_null()),
    ("count", pl.col("count_state"), pl.lit(True)),
    ("inning", pl.col("inning_bucket"), pl.lit(True)),
    ("late_close", pl.lit("late_close"), pl.col("late_close")),
    ("base_state", pl.col("base_state"), pl.lit(True)),
]

_PITCHING_SPLITS: list[tuple[str, pl.Expr, pl.Expr]] = [
    ("overall", pl.lit("all"), pl.lit(True)),
    ("vs_hand", pl.col("bat_side"), pl.col("bat_side").is_not_null()),
    (
        "home_away",
        pl.when(pl.col("home_away") == "home").then(pl.lit("away")).otherwise(pl.lit("home")),
        pl.lit(True),
    ),
    ("outs", pl.col("outs_pre").cast(pl.Utf8), pl.col("outs_pre").is_not_null()),
    ("count", pl.col("count_state"), pl.lit(True)),
    ("inning", pl.col("inning_bucket"), pl.lit(True)),
    ("late_close", pl.lit("late_close"), pl.col("late_close")),
    ("base_state", pl.col("base_state"), pl.lit(True)),
]


def batting_splits_season(events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Tidy-long batting splits: one row per player x season x split_type x split_value."""
    base = events.filter(pl.col("batter_mlbam").is_not_null())
    frames = []
    for split_type, value_expr, filt in _BATTING_SPLITS:
        frames.append(
            base.filter(filt)
            .with_columns(value_expr.alias("split_value"))
            .group_by("batter_mlbam", "season", "split_value")
            .agg(pl.lit(split_type).alias("split_type"), *[pl.col(c).sum() for c in _SPLIT_AGG_COLS])
        )
    raw = pl.concat(frames, how="vertical_relaxed").rename({"batter_mlbam": "player_id"})

    w = weights.select(_WEIGHT_COLUMNS)
    out = raw.join(w, left_on="season", right_on="game_year", how="left").with_columns(*_batting_rate_exprs())
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "split_type", "split_value", "full_name",
        "pa", "ab", "h", "hr", "bb", "so",
        "avg", "obp", "slg", "ops", "iso", "babip", "bb_pct", "k_pct", "woba",
    )


def pitching_splits_season(events: pl.DataFrame, weights: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Tidy-long pitching splits: one row per pitcher x season x split_type x split_value.

    vs_hand is the batter's side; home_away is the pitcher's team perspective
    (flipped from the batting-team-relative home_away column). Runs/ER here are
    attributed to the facing pitcher (see pitching_stats_season for the
    responsible-pitcher basis used in the season/monthly marts).
    """
    base = events.filter(pl.col("pitcher_mlbam").is_not_null())
    frames = []
    for split_type, value_expr, filt in _PITCHING_SPLITS:
        frames.append(
            base.filter(filt)
            .with_columns(value_expr.alias("split_value"))
            .group_by("pitcher_mlbam", "season", "split_value")
            .agg(
                pl.lit(split_type).alias("split_type"),
                pl.col("pa").sum().alias("bf"), pl.col("ab").sum(), pl.col("h").sum(),
                pl.col("hr").sum(), pl.col("ubb").sum(), pl.col("bb").sum(), pl.col("ibb").sum(),
                pl.col("hbp").sum(), pl.col("so").sum(), pl.col("outs").sum(),
                pl.col("runs").sum().alias("r"), pl.col("er").sum(),
            )
        )
    raw = pl.concat(frames, how="vertical_relaxed").rename({"pitcher_mlbam": "player_id"})

    w = weights.select(_WEIGHT_COLUMNS)
    out = raw.join(w, left_on="season", right_on="game_year", how="left").with_columns(*_pitching_rate_exprs())
    out = out.join(players, on="player_id", how="left")
    return out.select(
        "player_id", "season", "split_type", "split_value", "full_name",
        "bf", "h", "hr", "bb", "so", "r", "er",
        "ip", "baa", "era", "whip", "k9", "bb9", "hr9", "k_pct", "bb_pct", "fip",
    )


# ---------------------------------------------------------------------------
# Marts: game_coverage.sql / daily_game_coverage.sql (redesigned: schedule vs pbp)
# ---------------------------------------------------------------------------


def game_coverage(schedule: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Scheduled vs. pbp-covered regular-season game counts, per season.

    Historical Retrosheet-sourced rows don't carry an MLBAM game_pk, so this is
    a count comparison (distinct games per season on each side), not a
    per-game_pk join -- that only works for the current mlbam-sourced season.
    """
    sched = (
        schedule.filter((pl.col("game_type") == "R") & (pl.col("detailed_state") != "Cancelled"))
        .group_by("season_year")
        .agg(pl.col("game_pk").n_unique().alias("games_scheduled"))
    )
    pbp = events.group_by("season").agg(pl.col("game_id").n_unique().alias("games_with_pbp_data"))
    out = sched.join(pbp, left_on="season_year", right_on="season", how="full", coalesce=True)
    out = out.with_columns(
        pl.col("games_scheduled").fill_null(0).cast(pl.Int64),
        pl.col("games_with_pbp_data").fill_null(0).cast(pl.Int64),
    ).with_columns(
        (pl.col("games_scheduled") - pl.col("games_with_pbp_data")).alias("missing_games"),
        pl.when(pl.col("games_scheduled") > 0)
        .then((pl.col("games_with_pbp_data") / pl.col("games_scheduled") * 100).round(1))
        .alias("coverage_pct"),
    )
    return out.sort("season_year")


def daily_game_coverage(schedule: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Scheduled vs. pbp-covered regular-season game counts, per calendar day."""
    sched = (
        schedule.filter((pl.col("game_type") == "R") & (pl.col("detailed_state") != "Cancelled"))
        .group_by("official_date", "season_year")
        .agg(pl.col("game_pk").n_unique().alias("games_scheduled"))
        .rename({"official_date": "calendar_date"})
    )
    pbp = (
        events.group_by("game_date", "season")
        .agg(pl.col("game_id").n_unique().alias("games_with_pbp_data"))
        .rename({"game_date": "calendar_date", "season": "season_year"})
    )
    out = sched.join(pbp, on=["calendar_date", "season_year"], how="full", coalesce=True)
    out = out.with_columns(
        pl.col("games_scheduled").fill_null(0).cast(pl.Int64),
        pl.col("games_with_pbp_data").fill_null(0).cast(pl.Int64),
    ).with_columns((pl.col("games_with_pbp_data") - pl.col("games_scheduled")).alias("data_minus_scheduled"))
    return out.sort("calendar_date")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

MART_FILES: tuple[str, ...] = (
    "mart_batting_stats_season.parquet",
    "mart_pitching_stats_season.parquet",
    "mart_batting_stats_monthly.parquet",
    "mart_pitching_stats_monthly.parquet",
    "mart_batting_splits_season.parquet",
    "mart_pitching_splits_season.parquet",
    "mart_game_coverage.parquet",
    "mart_daily_game_coverage.parquet",
)


def run_all(work_dir: Path, *, upload: bool = True) -> dict[str, int]:
    """Build every mart and write it to ``work_dir`` (and upload unless ``upload=False``).

    Requires ``mlb_schedule.parquet`` / ``retrosheet_plays.parquet`` / ``weights.parquet``
    on the Hub (run ``etl hf pbp`` and ``etl hf weights`` first).
    """
    from etl_scripts import hf_sync

    sched_path = hf_sync.download_existing("mlb_schedule.parquet", work_dir)
    pbp_path = hf_sync.download_existing("retrosheet_plays.parquet", work_dir)
    if sched_path is None or pbp_path is None:
        raise RuntimeError(
            "mlb_schedule.parquet / retrosheet_plays.parquet not found on the Hub; "
            "run `etl hf pbp` first so they exist, then re-run the marts."
        )

    schedule = pl.read_parquet(sched_path)
    pbp = pl.read_parquet(pbp_path)
    events = load_pbp_events(pbp)

    logger.info("Loading weights (Hub) and players (Chadwick Register)...")
    weights = load_weights(work_dir)
    players = load_players()
    responsible_er = build_responsible_er(events)

    marts: dict[str, pl.DataFrame] = {
        "mart_batting_stats_season.parquet": batting_stats_season(events, weights, players),
        "mart_pitching_stats_season.parquet": pitching_stats_season(events, weights, players, responsible_er),
        "mart_batting_stats_monthly.parquet": batting_stats_monthly(events, weights, players),
        "mart_pitching_stats_monthly.parquet": pitching_stats_monthly(events, weights, players, responsible_er),
        "mart_batting_splits_season.parquet": batting_splits_season(events, weights, players),
        "mart_pitching_splits_season.parquet": pitching_splits_season(events, weights, players),
        "mart_game_coverage.parquet": game_coverage(schedule, events),
        "mart_daily_game_coverage.parquet": daily_game_coverage(schedule, events),
    }

    row_counts: dict[str, int] = {}
    for filename, df in marts.items():
        out = work_dir / filename
        df.write_parquet(out)
        row_counts[filename] = df.height
        logger.info("Wrote {} rows -> {}", df.height, out)
        if upload:
            hf_sync.upload_parquet(out, filename)
        else:
            logger.info("--no-upload: left {} on disk only", out)
    return row_counts
