# etl-scripts

Baseball ETL: ingest MLB data into a Postgres warehouse (running
[pg_duckdb](https://github.com/duckdb/pg_duckdb)) and transform it into analytics
marts with [dbt](https://docs.getdbt.com/). Managed with [uv](https://docs.astral.sh/uv/).

There is **one orchestrator (cron)** and **one CLI entrypoint (`etl`)**.

## The `etl` CLI

All ingest lives behind a single Typer app ([`etl_scripts/cli.py`](etl_scripts/cli.py),
sub-commands under [`etl_scripts/commands/`](etl_scripts/commands/)):

```bash
uv run etl --help

uv run --extra dbt etl statcast update-recent --days 1   # Statcast + statcast_extra + dbt marts
uv run --extra dbt etl pbp update-recent --days 3        # MLB Stats API play-by-play + PBP season stats
uv run etl schedule season                               # MLB schedule
uv run etl transactions update-recent --days 7           # transactions
uv run etl roster update-recent --days 7                 # roster stints
uv run etl contracts load payroll.csv                    # payroll CSV -> mlb_contracts
uv run etl retrosheet build                              # historical Retrosheet play-by-play (local)
uv run --extra dbt etl dbt build                         # ad-hoc dbt rebuild
```

The `--extra dbt` flag installs the dbt adapters; only the commands that rebuild
marts (`statcast`/`pbp` recent updates, `etl dbt`) need it.

## Scheduling

Cron is the scheduler — see [`crontab.sh`](crontab.sh) (install with `crontab -e`).
Two ingest jobs trigger dbt themselves after loading: `etl statcast update-recent`
rebuilds the Statcast marts (`post_statcast_ingest` selector), and
`etl pbp update-recent` rebuilds the PBP season-stat marts. Verify the cron jobs
by hand with [`scripts/test_crontab_jobs.sh`](scripts/test_crontab_jobs.sh).

## dbt

Models live under [`dbt/`](dbt/); raw ingest tables stay in `public`, marts build
into `baseball`. Season stats come from the play-by-play fact (`retrosheet_plays`
→ `stg_pbp__events`); the Statcast source feeds the coverage/events marts. See
[docs/dbt.md](docs/dbt.md) and [docs/lineage.md](docs/lineage.md).

## Local development

A local DuckDB mirror (`dev.duckdb`) lets you run the PBP pipeline offline:

```bash
DUCKDB_PATH=./dev.duckdb uv run python -m etl_scripts.validate_local_pipeline run 2026
```

## Docs

- [docs/dbt.md](docs/dbt.md) — warehouse transforms, selectors, CLI/dbt integration
- [docs/lineage.md](docs/lineage.md) — ingest → warehouse → dbt lineage
- [docs/mlbam_pbp.md](docs/mlbam_pbp.md), [docs/retrosheet.md](docs/retrosheet.md) — play-by-play
- [docs/mlb_roster_entries.md](docs/mlb_roster_entries.md) — roster stints / service time
