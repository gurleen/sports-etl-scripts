# Current-season play-by-play (MLB Stats API → `retrosheet_plays`)

Fills the shared play-grain table ([docs/retrosheet.md](retrosheet.md)) with
**current/recent-season** rows (`source='mlbam'`) from the MLB Stats API, in the
same schema as the historical Retrosheet load — so the same `GROUP BY` /
handedness-split queries span both sources. Per-runner baserunning detail also
lands in a dedicated `baserunning_events` table.

- Logic: [`etl_scripts/mlbam_pbp.py`](../etl_scripts/mlbam_pbp.py)
- CLI: [`update_mlbam_pbp.py`](../update_mlbam_pbp.py)
- Prefect: [`flows/mlbam_pbp_flow.py`](../flows/mlbam_pbp_flow.py) (deployments `mlbam-pbp-update-recent`, `mlbam-pbp-ingest-year`)

## Source

`GET /api/v1.1/game/{game_pk}/feed/live` (`MlbApiClient.stats.get_game`). The
driver lists Final (`coded_game_state='F'`) regular-season `game_pk`s for a
season from the `mlb_schedule` table, so **run the schedule sync first** if a
season isn't loaded there (`mlb-schedule-ingest-year` / `sync_mlb_schedule_for_year`).

## Usage

```bash
# One game
uv run python update_mlbam_pbp.py update-game 776135

# A whole season (skips games already loaded)
uv run python update_mlbam_pbp.py season 2026
uv run python update_mlbam_pbp.py season 2025 --reload   # re-fetch every game

# Nightly: re-fetch games finalized in the last N days (box scores get corrected)
uv run python update_mlbam_pbp.py update-recent --days 3
```

Loads are **idempotent**: each game deletes its existing `mlbam` rows in both
tables, then re-inserts (single transaction per game). Per-game failures are
logged and skipped; the run continues.

## Mapping notes

- **Grain matches Retrosheet:** one `pa=1` row per plate appearance, plus a `pa=0` row per mid-PA baserunning event (steal, pickoff, WP, PB, balk, DI). `play_number` is a per-game running counter (like Retrosheet `pn`), not `atBatIndex`.
- **Outcome flags** come from `result.eventType` (same vocabulary as Statcast `events`); the mapping in `pa_flags()` mirrors the dbt `stg_statcast__{batting,pitching}_events` CASE logic. Unmapped event types are logged and counted as a generic out.
- **Handedness:** `bat_side` / `pit_hand` from `matchup.batSide`/`pitchHand` (switch hitters already resolved by the API).
- **Earned runs are exact, no redistribution:** each scoring runner carries `details.responsiblePitcher`, `earned`, and `teamUnearned`, which map straight to the `run_{b,1,2,3}_*` slots and `team_unearned_runs`. (Retrosheet required redistributing `tur`; the API gives per-run flags directly.)
- **Team codes** are mapped MLB team id → Retrosheet code (`MLB_TEAMID_TO_RETRO`) so `bat_team`/`pit_team` are uniform across sources (e.g. `147`→`NYA`, `133`→`ATH`).
- **Not populated for `mlbam` rows:** `*_retro` ids (NULL), `lineup_pos`/`bat_field_pos` (NULL). Pure substitution / no-play markers are skipped (Retrosheet keeps `NP` rows; they carry no stats).
- **Mid-AB pinch hitter (Rule 9.15(b)):** when a batter leaves with two strikes and the substitute completes a strikeout, `batter_mlbam` / `bat_side` on the `pa=1` row are the original batter; any other PA completion stays on `matchup.batter`.

## `baserunning_events`

One row per runner movement on a baserunning play: `runner_mlbam`, `start_base`,
`end_base`, `is_out`, `responsible_pitcher_mlbam`, `is_scoring`, `earned`,
`team_unearned`, `rbi`. PK `(source, game_id, play_number, event_seq)`. Created
automatically on first load; `emit-baserunning-ddl` prints the DDL.

## Automation

`mlbam-pbp-update-recent` runs nightly (cron `30 10 * * *` UTC) re-fetching the
last 3 days of finals; `mlbam-pbp-ingest-year` backfills a full season on demand.
