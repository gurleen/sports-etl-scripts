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

**Not published** (intentionally out of scope): the original `statcast` pitch
table, `mlb_roster_entries`, and `mlb_contracts`.

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
# nightly-style jobs
uv run --extra hf etl hf pbp --days 3
uv run --extra hf etl hf transactions --days 7
uv run --extra hf etl hf statcast-extra --days 3 [--year 2026]

# one-off historical Retrosheet backfill (replaces the 'retrosheet' source rows)
uv run --extra hf etl hf retrosheet --full
uv run --extra hf etl hf retrosheet --start-year 2000 --end-year 2024

# local dry run: write the Parquet but don't upload to the Hub
uv run --extra hf etl hf transactions --days 1 --no-upload
```

Useful env vars: `HF_DATASET_REPO` (target repo), `HF_TOKEN` (write token),
`DUCKDB_PATH` (staging file), `HF_WORK_DIR` (scratch dir for downloads/exports).

## Scheduling (GitHub Actions)

| Workflow | Trigger | Output |
| --- | --- | --- |
| [`hf-pbp.yml`](../.github/workflows/hf-pbp.yml) | daily 14:00 UTC + manual | schedule, retrosheet_plays, baserunning_events |
| [`hf-transactions.yml`](../.github/workflows/hf-transactions.yml) | daily 14:30 UTC + manual | transactions |
| [`hf-statcast-extra.yml`](../.github/workflows/hf-statcast-extra.yml) | daily 15:00 UTC + manual | statcast_\<year\> |
| [`hf-retrosheet-historical.yml`](../.github/workflows/hf-retrosheet-historical.yml) | manual only | retrosheet_plays (`retrosheet` source) |

The statcast-extra job is scheduled after the PBP job so it reads a fresh
`mlb_schedule.parquet`.

## Setup

Add a repository secret **`HF_TOKEN`** — a write-scoped Hugging Face access token
([hf.co/settings/tokens](https://huggingface.co/settings/tokens)) with write
access to the target dataset repo. The dataset repo is created automatically on
first upload if it doesn't exist.
