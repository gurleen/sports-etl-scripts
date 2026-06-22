# dbt (warehouse transforms)

Derived tables and materialized views live as [dbt](https://docs.getdbt.com/) models under `dbt/models/`. The `etl` ingest CLIs load raw data into `public`; dbt builds and refreshes the marts in the `baseball` schema.

dbt paths live under `dbt/` so they do not collide with the Python package `models/` (Savant ingest).

## Layout

| Path | Role |
|------|------|
| `dbt/models/staging/` | Cleaned grains on `{{ source('warehouse', ...) }}` |
| `dbt/models/intermediate/` | Reusable logic (ported from `query.prql` CTEs) |
| `dbt/models/marts/` | Consumer-facing tables; default `materialized_view` (`games` is a table) |
| `dbt/macros/get_season_year.sql` | Current calendar year, or `--vars '{"season_year": 2025}'` |

Season-stat marts (`batting_stats_season`, `pitching_stats_season`, plus the monthly and splits variants) are built from the unified play-by-play fact (`retrosheet_plays`) via `stg_pbp__events`. The Statcast source separately feeds the coverage/events marts (`games`, `game_coverage`, `daily_game_coverage`, `statcast_events`, `abs_challenges`).

## Setup

1. Install dbt (Postgres adapter) with the project venv:

   ```bash
   uv sync --extra dbt
   ```

2. Configure connection (same credentials as Statcast ETL). The repo ships `profiles.yml` using `POSTGRES_*` env vars; set `POSTGRES_PASSWORD` in `.env` or the shell (loaded by Statcast ETL via `python-dotenv` for local runs). dbt builds models into the `baseball` schema (`schema` in `profiles.yml`); raw ingest tables remain in `public` via `sources` (`schema: public`). Do not also set `+schema: baseball` in `dbt_project.yml`—that duplicates the profile schema and Postgres will create `baseball_baseball`. With [pg_duckdb](https://github.com/duckdb/pg_duckdb), each run executes `SET duckdb.force_execution = true` via `on-run-start` and per-node `pre-hook` in `dbt_project.yml`.

3. Install packages and verify parsing:

   ```bash
   uv run dbt deps
   uv run dbt parse
   ```

4. Refresh warehouse source definitions from Postgres (requires `POSTGRES_*` or `DATABASE_URL`):

   ```bash
   # Default: a fixed list of ingest tables
   bash scripts/generate_dbt_sources.sh

   # Every table in schema public (dbt-codegen introspection)
   DBT_SOURCE_TABLES=all bash scripts/generate_dbt_sources.sh

   # Optional filters (SQL LIKE patterns)
   DBT_SOURCE_TABLE_PATTERN='statcast%' bash scripts/generate_dbt_sources.sh
   DBT_SOURCE_EXCLUDE='pg_%' DBT_SOURCE_TABLES=all bash scripts/generate_dbt_sources.sh
   ```

   Do **not** redirect bare `dbt run-operation` output into `_sources.generated.yml`—dbt logs (including ANSI color codes) mix into stdout/stderr and will corrupt the YAML. The script sends logs to `logs/dbt_generate_source.log` and writes only macro output to `dbt/models/_sources.generated.yml`.

   Under the hood, omitting `table_names` in [dbt-codegen `generate_source`](https://github.com/dbt-labs/dbt-codegen) calls `dbt_utils.get_relations_by_pattern` on `schema_name` (`public` here).

## Commands

```bash
# Rebuild the Statcast-derived marts (games, coverage, statcast_events, abs_challenges)
uv run dbt build --selector post_statcast_ingest

# Build one mart and its upstream deps (required on first run or after adding models)
uv run dbt build --select pitching_stats_season+

# Compile SQL without touching the warehouse
uv run dbt compile --select batting_stats_season
uv run dbt compile --select pitching_stats_season

# Preview rows (needs warehouse access)
uv run dbt show --select batting_stats_season --limit 20
uv run dbt show --select pitching_stats_season --limit 20
```

## CLI integration (cron)

dbt rebuilds run automatically inside the ingest CLIs — there is no separate orchestrator:

- `etl statcast update-recent` ingests Statcast + `statcast_extra`, then runs `dbt build --selector post_statcast_ingest` via `dbt.cli.main.dbtRunner` (`etl_scripts/dbt_runner.py`), **skipping** when no Statcast rows changed (`statcast_relevant_data_changed`).
- `etl pbp update-recent` rebuilds the PBP season-stat marts (`run_mlbam_pbp_season_stats_dbt`) whenever it loads games.

Both run nightly from [`crontab.sh`](../crontab.sh); pass `--extra dbt` so the dbt adapters are importable. Manual rebuild:

```bash
uv run --extra dbt etl dbt build --selector post_statcast_ingest
```

## Adding another mart

1. Add SQL under `dbt/models/marts/` (reuse `ref()` on intermediate models).
2. If it's Statcast-derived, tag it `post_statcast_ingest` (via a `config: tags:` block in `_marts.yml`) so `etl statcast update-recent` rebuilds it. PBP season marts are instead added to `MLBAM_PBP_SEASON_STATS_MODELS` in `etl_scripts/dbt_runner.py`.
3. Run `dbt build --select your_model+` to validate upstream deps.
