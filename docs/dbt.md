# dbt (warehouse transforms)

Derived tables and materialized views move from hand-maintained SQL / PRQL (`query.prql`) into [dbt](https://docs.getdbt.com/) models under `dbt/models/`. Prefect continues to load raw data; dbt builds and refreshes marts.

dbt paths live under `dbt/` so they do not collide with the Python package `models/` (Savant ingest).

## Layout

| Path | Role |
|------|------|
| `dbt/models/staging/` | Cleaned grains on `{{ source('warehouse', ...) }}` |
| `dbt/models/intermediate/` | Reusable logic (ported from `query.prql` CTEs) |
| `dbt/models/marts/` | Consumer-facing tables; default `materialized_view` (`games` is a table) |
| `dbt/macros/get_season_year.sql` | Current calendar year, or `--vars '{"season_year": 2025}'` |

The mart `current_season_batting_stats` replaces the warehouse object of the same name listed in `etl_scripts/materialized_views.py`.

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

## Commands

```bash
# Build the current-season mart (creates/refreshes the materialized view)
uv run dbt build --selector post_statcast_ingest

# Compile SQL without touching the warehouse
uv run dbt compile --select current_season_batting_stats

# Preview rows (needs warehouse access)
uv run dbt show --select current_season_batting_stats --limit 20
```

## Prefect integration

After Statcast ingest, run `dbt build --selector post_statcast_ingest` (manually, cron, or a future Prefect task). Statcast flows no longer call `refresh_materialized_views`; dbt owns mart refresh. Drop legacy warehouse MVs once dbt manages them, then remove `etl_scripts/materialized_views.py` if unused.

`query.prql` remains useful for parameterized API queries (`$1`–`$7` filters). Shared logic should live in dbt; PRQL can target `{{ ref('current_season_batting_stats') }}` via compiled tables or thin filter layers.

## Adding another mart

1. Add SQL under `dbt/models/marts/` (reuse `ref()` on intermediate models).
2. Tag with `post_statcast_ingest` if it should run after ingest.
3. Run `dbt build --select your_model+` to validate upstream deps.
