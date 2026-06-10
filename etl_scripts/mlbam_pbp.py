"""Load current-season play-by-play from the MLB Stats API into ``retrosheet_plays``.

Populates the shared play-grain table (see :mod:`etl_scripts.retrosheet`) with
``source='mlbam'`` rows, matching Retrosheet's grain: one row per plate
appearance *plus* a ``pa=0`` row for each mid-PA baserunning event (steal,
pickoff, wild pitch, …). Per-runner baserunning detail is also written to a
dedicated :data:`BASERUNNING_TABLE` table.

Source: ``GET /api/v1.1/game/{pk}/feed/live`` (``MlbApiClient.stats.get_game``).
The feed gives, per scoring runner, the responsible pitcher and ``earned`` /
``teamUnearned`` flags, so the per-run-slot earned-run attribution that powers
exact ERA maps directly (no ``tur`` redistribution needed, unlike Retrosheet).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Iterable, Sequence

import psycopg2
from loguru import logger
from psycopg2 import sql
from psycopg2.extras import execute_batch

from api_clients import MlbApiClient
from etl_scripts.retrosheet import SCHEMA_COLUMNS, TABLE_NAME
from etl_scripts.statcast import get_database_url

SOURCE_LABEL = "mlbam"
BASERUNNING_TABLE = "baserunning_events"

# MLB team id -> Retrosheet team code, so bat_team/pit_team are uniform across
# sources. The Athletics (id 133) use Retrosheet code ATH in the current era.
MLB_TEAMID_TO_RETRO: dict[int, str] = {
    108: "ANA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHN", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KCA", 119: "LAN",
    120: "WAS", 121: "NYN", 133: "ATH", 134: "PIT", 135: "SDN", 136: "SEA",
    137: "SFN", 138: "SLN", 139: "TBA", 140: "TEX", 141: "TOR", 142: "MIN",
    143: "PHI", 144: "ATL", 145: "CHA", 146: "MIA", 147: "NYA", 158: "MIL",
}

# Plate-appearance result eventType -> outcome flags. Mirrors the CASE logic in
# dbt stg_statcast__{batting,pitching}_events (same MLB eventType vocabulary).
# Each entry lists the SMALLINT flag columns to set to 1; `ab` is added below.
_PA_FLAGS: dict[str, tuple[str, ...]] = {
    "single": ("single", "hit"),
    "double": ("double", "hit"),
    "triple": ("triple", "hit"),
    "home_run": ("home_run", "hit"),
    "walk": ("walk",),
    "intent_walk": ("intent_walk",),
    "hit_by_pitch": ("hit_by_pitch",),
    "strikeout": ("strikeout",),
    "strikeout_double_play": ("strikeout", "other_dp"),
    "field_out": (),
    "force_out": (),
    "other_out": ("other_out",),
    "fielders_choice": ("fielders_choice",),
    "fielders_choice_out": ("fielders_choice",),
    "grounded_into_double_play": ("gdp",),
    "double_play": ("other_dp",),
    "triple_play": ("triple_play",),
    "sac_fly": ("sac_fly",),
    "sac_fly_double_play": ("sac_fly", "other_dp"),
    "sac_bunt": ("sac_bunt",),
    "sac_bunt_double_play": ("sac_bunt", "other_dp"),
    "field_error": ("reached_on_error",),
    "catcher_interf": ("reached_on_interference",),
    "batter_interference": ("reached_on_interference",),
    "fan_interference": ("reached_on_error",),
}
# PA outcomes that are NOT at-bats (mirrors the dbt `at_bat` exclusion list).
_NON_AB_EVENTS = {
    "walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_fly_double_play",
    "sac_bunt", "sac_bunt_double_play", "catcher_interf", "batter_interference",
}

# Mid-PA action eventType -> the baserunning flag column it sets. Action events
# not listed here (mound_visit, substitutions, game_advisory, …) are skipped.
_BASERUNNING_FLAG: dict[str, str] = {
    "stolen_base_2b": "stolen_base_2b",
    "stolen_base_3b": "stolen_base_3b",
    "stolen_base_home": "stolen_base_home",
    "caught_stealing_2b": "caught_stealing_2b",
    "caught_stealing_3b": "caught_stealing_3b",
    "caught_stealing_home": "caught_stealing_home",
    "pickoff_1b": "pickoff_1b",
    "pickoff_2b": "pickoff_2b",
    "pickoff_3b": "pickoff_3b",
    "pickoff_caught_stealing_2b": "caught_stealing_2b",
    "pickoff_caught_stealing_3b": "caught_stealing_3b",
    "pickoff_caught_stealing_home": "caught_stealing_home",
    "pickoff_error_1b": "pickoff_1b",
    "pickoff_error_2b": "pickoff_2b",
    "pickoff_error_3b": "pickoff_3b",
    "wild_pitch": "wild_pitch",
    "passed_ball": "passed_ball",
    "balk": "balk",
    "forced_balk": "balk",
    "defensive_indifference": "defensive_indifference",
    "defensive_indiff": "defensive_indifference",  # the feed's abbreviated form
    "other_advance": "other_advance",
    "runner_double_play": "other_advance",
    "stolen_base": "other_advance",
    "error": "other_advance",  # runner advance/out on an error (non-PA action)
}

_SLOT_BY_ORIGIN = {None: "b", "1B": "1", "2B": "2", "3B": "3"}

# Result `event` names that are NOT plate appearances. MLB tags both of these with
# eventType 'other_out', so we discriminate on the human `event` name:
#   - "Runner Out": a baserunning out (often inning-ending) while a batter was up;
#     the batter never completes a PA. Recorded pa=0 (matches Retrosheet).
#   - "Game Advisory": replay/advisory noise; emit no row at all.
_RUNNER_OUT_EVENTS = {"Runner Out"}
_SKIP_RESULT_EVENTS = {"Game Advisory"}

# Columns that default to 0 (summable flags/counts); everything else defaults to
# None. Derived from the schema so it can't drift.
_NONE_COLS = {
    "source", "game_id", "play_number", "pbp_type", "game_date", "season",
    "game_type", "inning", "inning_topbot", "bat_home", "bat_team", "pit_team",
    "site", "score_bat", "score_pit", "batter_mlbam", "batter_retro",
    "pitcher_mlbam", "pitcher_retro", "bat_hand", "bat_side", "pit_hand",
    "lineup_pos", "bat_field_pos", "count_balls", "count_strikes", "hit_type",
    "hit_location", "on_1b", "on_2b", "on_3b",
}
for _s in ("b", "1", "2", "3"):
    _NONE_COLS |= {f"run_{_s}_runner_retro", f"run_{_s}_runner_mlbam",
                  f"run_{_s}_pitcher_retro", f"run_{_s}_pitcher_mlbam"}
_ZERO_COLS = [c for c in SCHEMA_COLUMNS if c not in _NONE_COLS]


def _blank_row() -> dict[str, Any]:
    row: dict[str, Any] = {c: None for c in SCHEMA_COLUMNS}
    for c in _ZERO_COLS:
        row[c] = 0
    row["source"] = SOURCE_LABEL
    return row


def pa_flags(event_type: str | None) -> dict[str, int]:
    """Outcome flags for a plate-appearance ``eventType`` (unknown types -> just an out)."""
    flags = {k: 0 for k in (
        "ab", "hit", "single", "double", "triple", "home_run", "walk",
        "intent_walk", "hit_by_pitch", "strikeout", "sac_fly", "sac_bunt",
        "reached_on_error", "fielders_choice", "reached_on_interference",
        "gdp", "other_dp", "triple_play", "other_out",
    )}
    if event_type in _PA_FLAGS:
        for col in _PA_FLAGS[event_type]:
            flags[col] = 1
    elif event_type not in _NON_AB_EVENTS:
        # Unknown out-like result: count it as an at-bat out, log for follow-up.
        flags["other_out"] = 1
        logger.warning("Unmapped PA eventType {!r}; counted as other_out", event_type)
    if event_type not in _NON_AB_EVENTS:
        flags["ab"] = 1
    return flags


def _runner_slots(row: dict[str, Any], movements: Sequence[dict[str, Any]]) -> None:
    """Fill run-slot, runs, earned, and outs columns from a set of runner movements."""
    runs = earned = team_unearned = outs = 0
    for r in movements:
        mv = r.get("movement", {})
        det = r.get("details", {})
        if mv.get("isOut"):
            outs += 1
        if mv.get("end") != "score":
            continue
        runs += 1
        is_earned = bool(det.get("earned"))
        is_team_unearned = bool(det.get("teamUnearned"))
        if is_earned:
            earned += 1
        if is_team_unearned:
            team_unearned += 1
        slot = _SLOT_BY_ORIGIN.get(mv.get("originBase"), "b")
        resp = (det.get("responsiblePitcher") or {}).get("id")
        runner_id = (det.get("runner") or {}).get("id")
        row[f"run_{slot}_runner_mlbam"] = runner_id
        row[f"run_{slot}_pitcher_mlbam"] = resp
        row[f"run_{slot}_earned"] = 1 if is_earned else 0
    row["outs_on_play"] = outs
    row["runs_on_play"] = runs
    row["earned_runs"] = earned
    row["unearned_runs"] = runs - earned
    row["team_unearned_runs"] = team_unearned


def _advance_bases(
    bases: tuple[bool, bool, bool], movements: Sequence[dict[str, Any]]
) -> tuple[bool, bool, bool]:
    """Apply runner movements to a base-occupancy tuple (clear origins, then set ends).

    Used to carry base state through mid-PA events (steals, WP) so the PA-completion
    row reflects runners that advanced into scoring position during the at-bat.
    """
    on = {"1B": bases[0], "2B": bases[1], "3B": bases[2]}
    for r in movements:
        o = (r.get("movement") or {}).get("originBase")
        if o in on:
            on[o] = False
    for r in movements:
        e = (r.get("movement") or {}).get("end")
        if e in on:
            on[e] = True
    return (on["1B"], on["2B"], on["3B"])


def _apply_br_flags(row: dict[str, Any], movements: Sequence[dict[str, Any]]) -> None:
    """Set baserunning flag columns from *each* runner movement's eventType.

    A single play can hold several baserunning movements (e.g. a double steal of
    2nd and home), each with its own eventType — so flags must come per movement,
    not from the play's single primary eventType.
    """
    for r in movements:
        flag = _BASERUNNING_FLAG.get((r.get("details") or {}).get("eventType"))
        if flag:
            row[flag] = 1


class _GameState:
    """Running per-game counters: play number, half-inning outs, and score."""

    def __init__(self) -> None:
        self.play_number = 0
        self.cur_outs = 0
        self.half_key: tuple[int, str] | None = None
        self.away_score = 0
        self.home_score = 0
        # Base occupancy at the start of the current play (reset each half-inning).
        self.bases = (False, False, False)

    def next_play_number(self) -> int:
        self.play_number += 1
        return self.play_number

    def enter_half(self, inning: int, half: str) -> None:
        key = (inning, half)
        if key != self.half_key:
            self.half_key = key
            self.cur_outs = 0
            self.bases = (False, False, False)


def parse_game(feed: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse a feed/live payload into (retrosheet_plays rows, baserunning_events rows)."""
    gd = feed["gameData"]
    game_pk = gd["game"]["pk"]
    game_id = str(game_pk)
    game_type_raw = gd["game"].get("type")
    game_type = "regular" if game_type_raw == "R" else game_type_raw
    official_date = gd.get("datetime", {}).get("officialDate")
    game_date = date.fromisoformat(official_date) if official_date else None
    season = int(gd["game"]["season"]) if gd["game"].get("season") else (game_date.year if game_date else None)
    site = gd.get("venue", {}).get("name")
    away_retro = MLB_TEAMID_TO_RETRO.get(gd["teams"]["away"]["id"])
    home_retro = MLB_TEAMID_TO_RETRO.get(gd["teams"]["home"]["id"])

    plays = feed["liveData"]["plays"]["allPlays"]
    state = _GameState()
    play_rows: list[dict[str, Any]] = []
    br_rows: list[dict[str, Any]] = []

    for p in plays:
        about = p["about"]
        inning = about["inning"]
        is_top = about["isTopInning"]
        half = "top" if is_top else "bot"
        state.enter_half(inning, half)
        bat_team, pit_team = (away_retro, home_retro) if is_top else (home_retro, away_retro)

        matchup = p.get("matchup", {})
        batter_id = (matchup.get("batter") or {}).get("id")
        pitcher_id = (matchup.get("pitcher") or {}).get("id")
        bat_side = (matchup.get("batSide") or {}).get("code")
        pit_hand = (matchup.get("pitchHand") or {}).get("code")

        runners = p.get("runners", [])
        runners_by_idx: dict[Any, list[dict[str, Any]]] = {}
        for r in runners:
            runners_by_idx.setdefault(r.get("details", {}).get("playIndex"), []).append(r)

        # Non-pitch action events (in playEvent order). Every action's movements
        # advance base state (steals, DI, runner_placed ghost runner, errors), so a
        # later same-play PA row sees the right base state; recognized baserunning
        # events also get a pa=0 row. action_indices keeps these movements out of
        # the terminal PA's run/out attribution.
        action_items: list[tuple[Any, str | None]] = []
        action_indices: set[Any] = set()
        for e in p.get("playEvents", []):
            if e.get("isPitch") or e.get("type") != "action":
                continue
            action_indices.add(e.get("index"))
            action_items.append((e.get("index"), e.get("details", {}).get("eventType")))

        def base_row(pn: int) -> dict[str, Any]:
            row = _blank_row()
            row.update(
                game_id=game_id, play_number=pn, pbp_type="full", game_date=game_date,
                season=season, game_type=game_type, inning=inning,
                inning_topbot=("Top" if is_top else "Bot"), bat_home=(not is_top),
                bat_team=bat_team, pit_team=pit_team, site=site,
                batter_mlbam=batter_id, pitcher_mlbam=pitcher_id,
                bat_hand=bat_side, bat_side=bat_side, pit_hand=pit_hand,
                on_1b=state.bases[0], on_2b=state.bases[1], on_3b=state.bases[2],
            )
            return row

        def set_scores(row: dict[str, Any]) -> None:
            # Score at the *start* of the row (Retrosheet semantics), then advance.
            if is_top:
                row["score_bat"], row["score_pit"] = state.away_score, state.home_score
            else:
                row["score_bat"], row["score_pit"] = state.home_score, state.away_score
            scored = row["runs_on_play"]
            if is_top:
                state.away_score += scored
            else:
                state.home_score += scored

        # Process actions in order: emit pa=0 rows for recognized baserunning
        # events, and advance base state for *every* action.
        for idx, et in action_items:
            mvs = runners_by_idx.get(idx, [])
            if et == "runner_placed":
                # Extra-innings automatic runner: placed on 2nd base, with no
                # runner movement in the feed — so set the base directly.
                state.bases = (state.bases[0], True, state.bases[2])
                continue
            flag = _BASERUNNING_FLAG.get(et)
            if flag is not None:
                pn = state.next_play_number()
                row = base_row(pn)
                row[flag] = 1
                row["event_raw"] = et
                row["outs_pre"] = state.cur_outs
                _apply_br_flags(row, mvs)  # capture all runners (e.g. double steals)
                _runner_slots(row, mvs)
                state.cur_outs += row["outs_on_play"]
                row["outs_post"] = state.cur_outs
                set_scores(row)
                play_rows.append(row)
                br_rows.extend(_baserunning_event_rows(row, et, pitcher_id, mvs))
            # Carry base state forward so a later same-play PA row sees runners
            # that advanced (steal, DI, ghost runner placed in extras, etc.).
            state.bases = _advance_bases(state.bases, mvs)

        # Emit the terminal plate-appearance (pa=1) row.
        result = p.get("result", {})
        et = result.get("eventType")
        ev = result.get("event")
        # Base occupancy after this play (-> the next play's pre-state).
        post_bases = (
            bool(matchup.get("postOnFirst")),
            bool(matchup.get("postOnSecond")),
            bool(matchup.get("postOnThird")),
        )
        if ev in _SKIP_RESULT_EVENTS:
            state.bases = post_bases
            continue  # advisory noise, not a play
        if (et in _BASERUNNING_FLAG and et not in _PA_FLAGS) or ev in _RUNNER_OUT_EVENTS:
            # Play ended on a baserunning out mid-AB (inning-ending CS or "Runner Out"):
            # the batter never completed a PA, so record pa=0 like Retrosheet.
            pn = state.next_play_number()
            row = base_row(pn)
            row[_BASERUNNING_FLAG.get(et, "other_advance")] = 1
            row["event_raw"] = ev or et
            row["outs_pre"] = state.cur_outs
            terminal_mvs = [r for r in runners if r.get("details", {}).get("playIndex") not in action_indices]
            _apply_br_flags(row, terminal_mvs)
            _runner_slots(row, terminal_mvs)
            state.cur_outs += row["outs_on_play"]
            row["outs_post"] = state.cur_outs
            set_scores(row)
            play_rows.append(row)
            br_rows.extend(_baserunning_event_rows(row, et, pitcher_id, terminal_mvs))
            state.bases = post_bases
            continue

        pn = state.next_play_number()
        row = base_row(pn)
        row["pa"] = 1
        row["rbi"] = int(result.get("rbi") or 0)
        row["event_raw"] = ev
        flags = pa_flags(et)
        # MLB encodes awarded-base plays (catcher's interference, defensive shift
        # violations) as `field_error`, but scoring rules (and Retrosheet) count
        # these as a PA, not an at-bat.
        desc = (result.get("description") or "").lower()
        if et == "field_error" and ("interference" in desc or "violation" in desc):
            flags = {**flags, "reached_on_error": 0, "reached_on_interference": 1, "ab": 0}
        for col, val in flags.items():
            row[col] = val
        cnt = p.get("count", {})
        row["count_balls"] = cnt.get("balls")
        row["count_strikes"] = cnt.get("strikes")
        _apply_hit_data(row, p)
        row["outs_pre"] = state.cur_outs
        terminal_mvs = [r for r in runners if r.get("details", {}).get("playIndex") not in action_indices]
        _runner_slots(row, terminal_mvs)
        state.cur_outs += row["outs_on_play"]
        row["outs_post"] = state.cur_outs
        set_scores(row)
        # Steals / caught-stealing on the same play as the PA result (e.g. K+SB):
        # Retrosheet flags these on the PA row, so mirror that here.
        _apply_br_flags(row, terminal_mvs)
        br_mvs = [r for r in terminal_mvs if _BASERUNNING_FLAG.get((r.get("details") or {}).get("eventType"))]
        play_rows.append(row)
        if br_mvs:
            br_rows.extend(_baserunning_event_rows(row, None, pitcher_id, br_mvs))
        state.bases = post_bases

    return play_rows, br_rows


_TRAJECTORY_FLAG = {
    "ground_ball": "ground_ball", "line_drive": "line_drive",
    "fly_ball": "fly_ball", "popup": "fly_ball",
}


def _apply_hit_data(row: dict[str, Any], play: dict[str, Any]) -> None:
    """Set batted-ball flags from the in-play pitch's hitData, if present."""
    for e in reversed(play.get("playEvents", [])):
        hd = e.get("hitData")
        if hd:
            traj = hd.get("trajectory")
            row["ball_in_play"] = 1
            if traj in _TRAJECTORY_FLAG:
                row[_TRAJECTORY_FLAG[traj]] = 1
            row["hit_type"] = traj
            row["hit_location"] = hd.get("location")
            return


def _baserunning_event_rows(
    play_row: dict[str, Any], event_type: str | None, pitcher_id: int | None,
    movements: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """One baserunning_events row per runner movement in a baserunning play.

    ``event_type=None`` uses each movement's own ``details.eventType`` (for steals
    folded into a PA-completing play).
    """
    out: list[dict[str, Any]] = []
    for seq, r in enumerate(movements):
        mv = r.get("movement", {})
        det = r.get("details", {})
        out.append({
            "source": SOURCE_LABEL,
            "game_id": play_row["game_id"],
            "play_number": play_row["play_number"],
            "event_seq": seq,
            "season": play_row["season"],
            "game_date": play_row["game_date"],
            "inning": play_row["inning"],
            "inning_topbot": play_row["inning_topbot"],
            "bat_home": play_row["bat_home"],
            "event_type": event_type or det.get("eventType"),
            "runner_mlbam": (det.get("runner") or {}).get("id"),
            "start_base": mv.get("originBase"),
            "end_base": mv.get("end"),
            "is_out": bool(mv.get("isOut")),
            "pitcher_mlbam": pitcher_id,
            "responsible_pitcher_mlbam": (det.get("responsiblePitcher") or {}).get("id"),
            "is_scoring": mv.get("end") == "score",
            "earned": bool(det.get("earned")),
            "team_unearned": bool(det.get("teamUnearned")),
            "rbi": bool(det.get("rbi")),
        })
    return out


# ---------------------------------------------------------------------------
# baserunning_events DDL
# ---------------------------------------------------------------------------
BASERUNNING_SCHEMA: list[tuple[str, str]] = [
    ("source", "TEXT NOT NULL"),
    ("game_id", "TEXT NOT NULL"),
    ("play_number", "INTEGER NOT NULL"),
    ("event_seq", "INTEGER NOT NULL"),
    ("runner_mlbam", "INTEGER"),
    ("season", "INTEGER"),
    ("game_date", "DATE"),
    ("inning", "INTEGER"),
    ("inning_topbot", "TEXT"),
    ("bat_home", "BOOLEAN"),
    ("event_type", "TEXT"),
    ("start_base", "TEXT"),
    ("end_base", "TEXT"),
    ("is_out", "BOOLEAN"),
    ("pitcher_mlbam", "INTEGER"),
    ("responsible_pitcher_mlbam", "INTEGER"),
    ("is_scoring", "BOOLEAN"),
    ("earned", "BOOLEAN"),
    ("team_unearned", "BOOLEAN"),
    ("rbi", "BOOLEAN"),
]
BASERUNNING_COLUMNS = [c for c, _ in BASERUNNING_SCHEMA]


def baserunning_ddl(table_name: str = BASERUNNING_TABLE) -> str:
    body = ",\n".join(f'    "{c}" {t}' for c, t in BASERUNNING_SCHEMA)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n{body},\n"
        f"    PRIMARY KEY (source, game_id, play_number, event_seq)\n);\n"
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def _replace_game_rows(
    cur, table: str, columns: Sequence[str], game_id: str, rows: Sequence[dict[str, Any]]
) -> int:
    cur.execute(
        sql.SQL("DELETE FROM {} WHERE source = %s AND game_id = %s").format(sql.Identifier(table)),
        (SOURCE_LABEL, game_id),
    )
    if not rows:
        return 0
    fields = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))
    stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(sql.Identifier(table), fields, placeholders)
    tuples = [tuple(r.get(c) for c in columns) for r in rows]
    execute_batch(cur, stmt.as_string(cur.connection), tuples, page_size=500)
    return len(tuples)


def load_game(
    game_pk: int,
    *,
    database_url: str | None = None,
    client: MlbApiClient | None = None,
    conn: "psycopg2.extensions.connection | None" = None,
    write_baserunning: bool = True,
    ensure_baserunning_table: bool | None = None,
) -> dict[str, int]:
    """Fetch one game, parse it, and replace its ``mlbam`` rows in both tables.

    Pass an open ``conn`` and ``client`` to reuse them across games (avoids a fresh
    connection per game). When ``conn`` is omitted a connection is opened and closed
    for this call. ``ensure_baserunning_table`` defaults to True only when this call
    owns the connection — bulk callers create the table once up front instead, so
    concurrent workers don't race on ``CREATE TABLE``.
    """
    cl = client or MlbApiClient()
    feed = cl.stats.get_game(game_pk)
    play_rows, br_rows = parse_game(feed)
    game_id = str(game_pk)
    own_conn = conn is None
    c = conn or psycopg2.connect(database_url or get_database_url())
    if ensure_baserunning_table is None:
        ensure_baserunning_table = own_conn
    try:
        with c.cursor() as cur:
            n_plays = _replace_game_rows(cur, TABLE_NAME, SCHEMA_COLUMNS, game_id, play_rows)
            n_br = 0
            if write_baserunning:
                if ensure_baserunning_table:
                    cur.execute(baserunning_ddl())
                n_br = _replace_game_rows(cur, BASERUNNING_TABLE, BASERUNNING_COLUMNS, game_id, br_rows)
        c.commit()
    finally:
        if own_conn:
            c.close()
    return {"plays": n_plays, "baserunning": n_br}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def list_final_regular_game_pks(
    season: int,
    *,
    database_url: str | None = None,
    only_missing: bool = True,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[int]:
    """Final regular-season game_pks for ``season`` from ``mlb_schedule``.

    With ``only_missing`` (default), skips games already loaded as ``mlbam`` rows.
    ``start_date`` / ``end_date`` restrict by ``official_date`` (recent re-fetch).
    """
    url = database_url or get_database_url()
    clauses = ["season_year = %s", "game_type = 'R'", "coded_game_state = 'F'"]
    params: list[Any] = [season]
    if start_date is not None:
        clauses.append("official_date >= %s"); params.append(start_date)
    if end_date is not None:
        clauses.append("official_date <= %s"); params.append(end_date)
    if only_missing:
        clauses.append(
            "NOT EXISTS (SELECT 1 FROM retrosheet_plays p "
            "WHERE p.source = 'mlbam' AND p.game_id = mlb_schedule.game_pk::text)"
        )
    q = f"SELECT game_pk FROM mlb_schedule WHERE {' AND '.join(clauses)} ORDER BY official_date, game_pk"
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(q, params)
        return [int(r[0]) for r in cur.fetchall()]


def load_games(
    game_pks: Iterable[int],
    *,
    database_url: str | None = None,
    write_baserunning: bool = True,
    max_workers: int = 8,
    progress_every: int = 50,
    on_progress: "Callable[[int, int, int, int], None] | None" = None,
) -> dict[str, Any]:
    """Load many games concurrently, continuing past per-game failures.

    Each of the ``max_workers`` threads keeps its own DB connection and API client
    (so connections are reused across games and the per-game open cost is paid once
    per worker, not once per game). Bounded so the loader can't saturate the DB.
    ``on_progress(done, total, loaded, failed)`` fires every ``progress_every`` games.
    """
    url = database_url or get_database_url()
    pks = list(game_pks)
    if not pks:
        logger.info("mlbam pbp load: no games to load")
        return {"games_targeted": 0, "games_loaded": 0, "plays_written": 0,
                "baserunning_written": 0, "games_failed": 0, "failures": []}

    # Create the baserunning table once up front so concurrent workers don't race
    # on CREATE TABLE (which can deadlock in Postgres).
    if write_baserunning:
        with psycopg2.connect(url) as c0:
            with c0.cursor() as cur:
                cur.execute(baserunning_ddl())
            c0.commit()

    tls = threading.local()
    conns: list[Any] = []
    conns_lock = threading.Lock()

    def _ctx():
        if not hasattr(tls, "conn"):
            tls.conn = psycopg2.connect(url)
            tls.client = MlbApiClient()  # per-thread: get_game mutates base_url
            with conns_lock:
                conns.append(tls.conn)
        return tls.conn, tls.client

    def _work(gp: int):
        conn, client = _ctx()
        try:
            res = load_game(
                gp, client=client, conn=conn,
                write_baserunning=write_baserunning, ensure_baserunning_table=False,
            )
            return gp, res, None
        except Exception as exc:  # noqa: BLE001 - isolate per-game failures
            try:
                conn.rollback()  # clear the aborted txn so the worker can continue
            except Exception:
                pass
            return gp, None, str(exc)

    ok = plays = br = 0
    failures: list[tuple[int, str]] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_work, gp) for gp in pks]
            for i, fut in enumerate(as_completed(futures), 1):
                gp, res, err = fut.result()
                if err is not None:
                    logger.error("Failed to load game_pk={}: {}", gp, err)
                    failures.append((gp, err))
                else:
                    plays += res["plays"]; br += res["baserunning"]; ok += 1
                if progress_every and (i % progress_every == 0 or i == len(pks)):
                    logger.info("Loaded {}/{} games ({} rows)", i, len(pks), plays)
                    if on_progress is not None:
                        on_progress(i, len(pks), ok, len(failures))
    finally:
        for c in conns:
            try:
                c.close()
            except Exception:
                pass

    summary = {
        "games_targeted": len(pks), "games_loaded": ok, "plays_written": plays,
        "baserunning_written": br, "games_failed": len(failures),
        "failures": [{"game_pk": g, "error": e} for g, e in failures[:50]],
    }
    logger.info("mlbam pbp load: {}", {k: v for k, v in summary.items() if k != "failures"})
    return summary


def load_season(
    season: int,
    *,
    database_url: str | None = None,
    only_missing: bool = True,
    start_date: date | None = None,
    end_date: date | None = None,
    write_baserunning: bool = True,
    max_workers: int = 8,
    on_progress: "Callable[[int, int, int, int], None] | None" = None,
) -> dict[str, Any]:
    """Load all (or only-missing) Final regular-season games for ``season``."""
    url = database_url or get_database_url()
    pks = list_final_regular_game_pks(
        season, database_url=url, only_missing=only_missing,
        start_date=start_date, end_date=end_date,
    )
    logger.info("mlbam pbp season {}: {} games to load (only_missing={}, workers={})",
                season, len(pks), only_missing, max_workers)
    return load_games(
        pks, database_url=url, write_baserunning=write_baserunning,
        max_workers=max_workers, on_progress=on_progress,
    ) | {"season": season}
