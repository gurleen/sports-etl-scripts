# Hugging Face publishing

Datasets are published as Parquet to a Hugging Face **dataset** repo (default
`gurleen/baseball`, override with `HF_DATASET_REPO`) by scheduled GitHub Actions.
This replaces the old Postgres cron pipeline — Hugging Face is the system of
record. The jobs never touch a production database: each one stages data in a
throwaway local DuckDB (`ETL_DB_BACKEND=duckdb`), then exports to Parquet.

## Datasets

One Parquet file per table, except `statcast_extra`, which is season-partitioned.

| File on the Hub | Source table | Published by |
| --- | --- | --- |
| `mlb_schedule.parquet` | `mlb_schedule` | `etl hf pbp` |
| `retrosheet_plays.parquet` | `retrosheet_plays` (`mlbam` + `retrosheet` sources) | `etl hf pbp` (mlbam) / `etl hf retrosheet` (historical) |
| `baserunning_events.parquet` | `baserunning_events` | `etl hf pbp` |
| `mlb_transactions.parquet` | `mlb_transactions` | `etl hf transactions` |
| `statcast_<year>.parquet` | `statcast_extra` | `etl hf statcast-extra` |
| `weights.parquet` | FanGraphs guts data (per-season wOBA/FIP constants) | `etl hf weights` (manual) |
| `mart_*.parquet` (batting/pitching stats + splits, game coverage) | Polars port of the dbt PBP marts | `etl hf marts` |

**Not published** (intentionally out of scope): the original `statcast` pitch
table, `mlb_roster_entries`, and `mlb_contracts`.

### Marts

`etl hf marts` (implemented in [`etl_scripts/marts.py`](../etl_scripts/marts.py))
is a Polars port of the dbt PBP marts, run entirely against Parquet already on the
Hub — no Postgres warehouse involved. It writes 8 `mart_*.parquet` files
(season/monthly batting and pitching stats, season splits, and schedule-vs-pbp
coverage). It needs `mlb_schedule.parquet` / `retrosheet_plays.parquet` (run
`etl hf pbp` first) and `weights.parquet` (run `etl hf weights` first) to already
exist on the Hub; player names come from the Chadwick Bureau Register.
`statcast_events` and `abs_challenges` stay dbt-only and are not part of this port.

### Weights (FanGraphs guts data)

`weights.parquet` holds the per-season wOBA/FIP constants (`w_bb`, `w_single`,
`league_woba`, `c_fip`, etc.) that the marts need for wRC+/FIP. It comes from
FanGraphs' [guts page](https://www.fangraphs.com/guts.aspx?type=cn), not from
`etl hf pbp` — and unlike every other dataset here, it's **not fetched in CI**.
FanGraphs' guts API sits behind a Cloudflare challenge that blocks datacenter
IPs (GitHub Actions runners included; confirmed by testing — even a plain
homepage request 403s), so there's no reliable automated path.

Instead, refresh it manually whenever a new season's constants are published:

1. Download the CSV from https://www.fangraphs.com/guts.aspx?type=cn (the page
   has an "Export Data" / CSV link).
2. Run `uv run --extra hf etl hf weights path/to/the.csv`.

The export is cumulative (every season back to 1871), so each run fully
replaces `weights.parquet` — there's no accumulation/merge step like the other
datasets.

### Subsets in the Data Studio

The dataset card (`README.md`) carries a `configs:` block so the Hub viewer shows
each table as its own **subset** instead of merging every Parquet file into one
default dataset. The season-partitioned `statcast_<year>.parquet` files are
globbed into a single `statcast` subset. The card is created automatically on the
first upload (and is left alone afterwards); to (re)publish it manually — e.g.
after adding a new table — run:

```bash
uv run --extra hf etl hf card        # overwrites the card with the current subsets
```

The subset config lives in `hf_sync.DATASET_CONFIGS`.

## How accumulation works

Each run:

1. **Seeds** the staging DuckDB from the Parquet already on the Hub (a missing
   file on the first run is treated as empty), so the table keeps the table's
   primary key and prior history.
2. **Ingests** newly-fetched rows with the existing `etl` loaders (idempotent
   upsert / per-game replace), exactly as they wrote to Postgres before.
3. **Exports** the full table back to Parquet and **uploads** it to the Hub.

`statcast_extra` is the exception: its original sync discovered game_pks from the
`statcast` table (which is not published). The `etl hf statcast-extra` collector
instead discovers Final, regular-season game_pks from `mlb_schedule.parquet`
(published by the PBP job) and merges with `polars`, deduping on
`(game_pk, play_id)`. **Run the PBP job first** so the schedule snapshot exists.

## CLI

```bash
# nightly-style jobs (recent window)
uv run --extra hf etl hf pbp --days 3
uv run --extra hf etl hf transactions --days 7
uv run --extra hf etl hf statcast-extra --days 3 [--year 2026]

# full-season backfill (every not-yet-present Final regular-season game)
uv run --extra hf etl hf pbp --backfill --year 2026
uv run --extra hf etl hf statcast-extra --backfill --year 2026

# one-off historical Retrosheet backfill (replaces the 'retrosheet' source rows)
uv run --extra hf etl hf retrosheet --full
uv run --extra hf etl hf retrosheet --start-year 2000 --end-year 2024

# rebuild the PBP marts (run after pbp/retrosheet publish so the schedule/pbp
# snapshot is current, and after `etl hf weights` at least once)
uv run --extra hf etl hf marts

# refresh weights.parquet from a manually-downloaded FanGraphs guts CSV
uv run --extra hf etl hf weights ~/Downloads/fangraphs-guts-data.csv

# local dry run: write the Parquet but don't upload to the Hub
uv run --extra hf etl hf transactions --days 1 --no-upload
```

`--backfill` ignores `--days` and loads/fetches every Final regular-season game
for the season that isn't already present on the Hub (idempotent — safe to re-run;
already-loaded games are skipped). `statcast-extra --backfill` still needs an
up-to-date `mlb_schedule.parquet`, so run the pbp backfill first.

Useful env vars: `HF_DATASET_REPO` (target repo), `HF_TOKEN` (write token),
`DUCKDB_PATH` (staging file), `HF_WORK_DIR` (scratch dir for downloads/exports).

## Scheduling (GitHub Actions)

| Workflow | Trigger | Output |
| --- | --- | --- |
| [`hf-pbp.yml`](../.github/workflows/hf-pbp.yml) | daily 09:00 UTC (5:00 AM Eastern) + manual | schedule, retrosheet_plays, baserunning_events, then `mart_*` |
| [`hf-transactions.yml`](../.github/workflows/hf-transactions.yml) | daily 09:30 UTC (5:30 AM Eastern) + manual | transactions |
| [`hf-statcast-extra.yml`](../.github/workflows/hf-statcast-extra.yml) | daily 10:00 UTC (6:00 AM Eastern) + manual | statcast_\<year\> |
| [`hf-retrosheet-historical.yml`](../.github/workflows/hf-retrosheet-historical.yml) | manual only | retrosheet_plays (`retrosheet` source), then `mart_*` |
| [`hf-backfill.yml`](../.github/workflows/hf-backfill.yml) | manual only | full-season pbp + statcast_\<year\>, then `mart_*` |

The statcast-extra job is scheduled after the PBP job so it reads a fresh
`mlb_schedule.parquet`. Every workflow that can change `mlb_schedule.parquet` or
`retrosheet_plays.parquet` ends with an `etl hf marts` step so the `mart_*`
outputs never drift from the pbp data they're built from; `hf-transactions.yml`
and `hf-statcast-extra.yml` don't touch those tables, so they skip it. There's no
scheduled workflow for `etl hf weights` — it's a manual step (see
[Weights](#weights-fangraphs-guts-data) above).

### Full-season backfill workflow

`hf-backfill.yml` runs `etl hf pbp --backfill`, then `etl hf statcast-extra
--backfill`, then `etl hf marts` for one season. Trigger it from the Actions tab
(or the API) with inputs `year` (defaults to current) and `repo` (target dataset
repo, defaults to `gurleen/baseball` — set it to `gurleen/baseball-test` to
backfill the test repo). It's a heavy one-off; the nightly workflows keep things
current afterward.

## Setup

Add a repository secret **`HF_TOKEN`** — a write-scoped Hugging Face access token
([hf.co/settings/tokens](https://huggingface.co/settings/tokens)) with write
access to the target dataset repo. The dataset repo is created automatically on
first upload if it doesn't exist. None of the `hf-*` jobs touch Postgres.
