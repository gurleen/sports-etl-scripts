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

The marts `current_season_batting_stats` and `current_season_pitching_stats` replace warehouse objects of the same names listed in `etl_scripts/materialized_views.py`.

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
# Build the current-season marts (creates/refreshes materialized views + upstream staging/intermediate)
uv run dbt build --selector post_statcast_ingest

# Build one mart and its upstream deps (required on first run or after adding models)
uv run dbt build --select current_season_pitching_stats+

# Compile SQL without touching the warehouse
uv run dbt compile --select current_season_batting_stats
uv run dbt compile --select current_season_pitching_stats

# Preview rows (needs warehouse access)
uv run dbt show --select current_season_batting_stats --limit 20
uv run dbt show --select current_season_pitching_stats --limit 20
```

## Prefect integration

Statcast flows (`statcast-update-recent`, `statcast-update-date`, `statcast-update-full`, `statcast-season`, `statcast-backfill`) call the **`dbt-rebuild-baseball`** subflow after ingest. It runs `dbt build --selector post_statcast_ingest` via `dbt.cli.main.dbtRunner` in `etl_scripts/dbt_runner.py`, and **skips** when no Statcast-related rows changed (unless you trigger with `force: true`).

Manual rebuild:

```bash
uv run prefect deployment run 'dbt-rebuild-baseball/dbt-rebuild-baseball' --param force=true
```

Drop legacy warehouse MVs once dbt manages them, then remove `etl_scripts/materialized_views.py` if unused.

`query.prql` remains useful for parameterized API queries (`$1`–`$7` filters). Shared logic should live in dbt; PRQL can target `{{ ref('current_season_batting_stats') }}` via compiled tables or thin filter layers.

## Adding another mart

1. Add SQL under `dbt/models/marts/` (reuse `ref()` on intermediate models).
2. Tag with `post_statcast_ingest` if it should run after ingest.
3. Run `dbt build --select your_model+` to validate upstream deps.
