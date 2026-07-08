# etl-scripts

Baseball ETL: ingest MLB data into a Postgres warehouse (running
[pg_duckdb](https://github.com/duckdb/pg_duckdb)) and transform it into analytics
marts with [dbt](https://docs.getdbt.com/). Managed with [uv](https://docs.astral.sh/uv/).

Datasets are also published as Parquet to a [Hugging Face](https://huggingface.co/datasets/gurleen/baseball)
dataset repo by scheduled GitHub Actions (see [Hugging Face publishing](#hugging-face-publishing)).
There is **one CLI entrypoint (`etl`)**.

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

## Hugging Face publishing

The scheduled jobs now publish Parquet to the Hugging Face dataset repo
`gurleen/baseball` (one file per table) instead of writing to a Postgres
warehouse. They run in GitHub Actions ([`.github/workflows/hf-*.yml`](.github/workflows/))
and stage everything in a throwaway local DuckDB — no database is touched. Each
run seeds from the Parquet already on the Hub, merges newly-fetched rows, and
re-uploads, so history accumulates.

```bash
uv run --extra hf etl hf pbp --days 3          # mlb_schedule / retrosheet_plays / baserunning_events
uv run --extra hf etl hf transactions --days 7 # mlb_transactions.parquet
uv run --extra hf etl hf statcast-extra --days 3   # statcast_<year>.parquet (Savant gamefeed)
uv run --extra hf etl hf retrosheet --full     # one-off historical Retrosheet backfill
uv run --extra hf etl hf marts                 # Polars port of the dbt PBP marts -> mart_*.parquet
uv run --extra hf etl hf weights guts.csv       # FanGraphs guts CSV -> weights.parquet (manual, see below)

uv run --extra hf etl hf pbp --backfill --year 2026          # full-season backfill (skips loaded games)
uv run --extra hf etl hf statcast-extra --backfill --year 2026
```

`--backfill` loads every not-yet-present Final regular-season game for the season
(there's also a manual `hf-backfill.yml` workflow that runs both).
Add `--no-upload` to export the Parquet locally without pushing to the Hub. The
workflows only need a write-scoped `HF_TOKEN` repository secret — no database
credentials. Every workflow that touches `mlb_schedule.parquet` /
`retrosheet_plays.parquet` runs `etl hf marts` afterward so the marts stay in
sync; `etl hf marts` also needs `weights.parquet` on the Hub, which is **not**
fetched automatically — FanGraphs' guts API is Cloudflare-gated against
datacenter IPs (GitHub Actions included), so refresh it yourself with
`etl hf weights <csv>` whenever a new season's constants are published. See
[docs/huggingface.md](docs/huggingface.md) for details, scheduling, and the
datasets that are intentionally **not** published (`statcast`,
`mlb_roster_entries`, `mlb_contracts`).

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

- [docs/huggingface.md](docs/huggingface.md) — Parquet publishing to Hugging Face (GitHub Actions)
- [docs/dbt.md](docs/dbt.md) — warehouse transforms, selectors, CLI/dbt integration
- [docs/lineage.md](docs/lineage.md) — ingest → warehouse → dbt lineage
- [docs/mlbam_pbp.md](docs/mlbam_pbp.md), [docs/retrosheet.md](docs/retrosheet.md) — play-by-play
- [docs/mlb_roster_entries.md](docs/mlb_roster_entries.md) — roster stints / service time
