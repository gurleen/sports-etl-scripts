# Retrosheet play-by-play (`retrosheet_plays`)

A **local, manual** pipeline that turns Retrosheet's pre-parsed play-by-play
([retrosheet.org/downloads/plays.html](https://retrosheet.org/downloads/plays.html))
into a single play-grain Parquet/warehouse table, designed so batting and
pitching stats fall out of plain `GROUP BY` + `SUM` under arbitrary filters
(e.g. LHP vs RHB).

It is intentionally **not** a Prefect flow. Run it on your machine, then load the
Parquet into the warehouse yourself. The schema is source-agnostic — a `source`
discriminator plus nullable MLBAM / Retrosheet id pairs let current-season MLBAM
play-by-play load into the same table later.

- Logic: [`etl_scripts/retrosheet.py`](../etl_scripts/retrosheet.py)
- CLI: [`build_retrosheet.py`](../build_retrosheet.py)

## Usage

```bash
# Modern era (default 2000..current year) -> data/retrosheet_plays_2000_<year>.parquet
uv run python build_retrosheet.py build

# A specific range, postseason included, Hive-partitioned by season
uv run python build_retrosheet.py build --start-year 2015 --end-year 2024 \
    --game-types regular --game-types worldseries --partition-by-season

# Every game type (regular + post + allstar)
uv run python build_retrosheet.py build --game-types all

# Emit the CREATE TABLE statement that matches the Parquet columns
uv run python build_retrosheet.py emit-ddl --output data/retrosheet_plays_schema.sql
```

Season zips/CSVs are cached under `data/retrosheet_cache/` (gitignored) and
reused on later runs. Seasons Retrosheet has not published yet (e.g. an
in-progress year) 404 and are skipped with a warning.

## Grain & schema

One row per **play (event)**. PA-outcome columns (`pa`, `ab`, `hit`, `single`…
`home_run`, `walk`, `intent_walk`, `hit_by_pitch`, `strikeout`, `sac_fly`,
`sac_bunt`, `reached_on_error`, `fielders_choice`, `gdp`, …) are `0/1` integers,
so they sum directly. This mirrors the Statcast-derived
[`stg_statcast__pitching_events`](../dbt/models/staging/stg_statcast__pitching_events.sql)
/ `stg_statcast__batting_events` flag layout, with two deliberate normalizations:

- `walk` excludes intentional walks (Retrosheet's `walk` includes them); `intent_walk` holds the IBB count — matching the dbt `walk` / `intent_walk` split.
- `hit = single + double + triple + home_run`.

[`PG_SCHEMA`](../etl_scripts/retrosheet.py) is the single source of truth for both
the Polars projection and the DDL (kept in sync by
`validate_schema_consistency()`). Primary key: `(source, game_id, play_number)`.

### Handedness (matchup splits)

- `bat_hand` / `pit_hand` are the raw Retrosheet handedness (`bat_hand` can be `B` for switch hitters).
- **`bat_side`** is the side actually used: `L`/`R` directly, and for switch hitters the side opposite the pitcher's throwing hand. Filter on `bat_side` (not `bat_hand`) for splits — it behaves like Statcast's `stand`.

### Earned runs & exact ERA

Earned-run accounting is exact and charges runs to the **responsible** pitcher
(correct for inherited runners), not whoever was facing the batter:

- Play level: `runs_on_play`, `earned_runs` (**pitcher-earned, the MLB-rules ERA basis**), `unearned_runs` (pitcher-unearned), `team_unearned_runs`.
- Per run, four "slots" (the batter + three baserunners) carry the scorer and the responsible pitcher: `run_{b,1,2,3}_runner_mlbam`, `run_{b,1,2,3}_pitcher_mlbam` (+ `_retro`), and `run_{b,1,2,3}_earned`.

`earned_runs` is **`er + tur`**, not Retrosheet's `er` alone: `er` is runs earned
to the *team*, while `tur` is the runs unearned to the team but charged earned to
a relief pitcher (MLB Rule 9.16). Their sum is MLB's official per-pitcher earned
total — verified to match MLB's Stats API exactly, so historical ERA needs no MLB
API. Per-slot `*_earned` = scored runs whose `ur*` flag is unset, which sums to
`er + tur`. Outs (innings pitched) belong to the facing pitcher; earned runs
belong to the responsible pitcher — so exact ERA unpivots the four slots:

```sql
with ip as (                       -- outs -> the pitcher who recorded them
    select pitcher_mlbam, sum(outs_on_play) / 3.0 as ip
    from retrosheet_plays
    group by pitcher_mlbam
),
er as (                            -- earned runs -> the responsible pitcher
    select pitcher_mlbam, sum(earned) as er
    from (
        select run_b_pitcher_mlbam as pitcher_mlbam, run_b_earned as earned from retrosheet_plays
        union all select run_1_pitcher_mlbam, run_1_earned from retrosheet_plays
        union all select run_2_pitcher_mlbam, run_2_earned from retrosheet_plays
        union all select run_3_pitcher_mlbam, run_3_earned from retrosheet_plays
    ) s
    where pitcher_mlbam is not null
    group by pitcher_mlbam
)
select ip.pitcher_mlbam, round(ip.ip, 1) as ip, coalesce(er.er, 0) as er,
       round(9.0 * coalesce(er.er, 0) / nullif(ip.ip, 0), 2) as era
from ip left join er using (pitcher_mlbam)
order by era;
```

For handedness-split rate stats the conventional treatment charges the facing
pitcher — for those, `earned_runs` on the play is the right column. Use the
responsible-pitcher unpivot above only for full-season official ERA.

## MLBAM ids

Retrosheet ids (`*_retro`) are mapped to MLBAM ids (`*_mlbam`) via the Chadwick
Bureau Register through `pybaseball.chadwick_register()` (cached). Players
predating MLBAM ids map to `NULL` — the Retrosheet id is always retained, so no
play is dropped. Coverage is ~100% for the modern era (the default range).

## Loading into the warehouse

The `load` command creates the table, appends the Parquet (via Polars + ADBC,
one season at a time so memory stays bounded), and builds the secondary indexes —
reading `DATABASE_URL` / `POSTGRES_*` from the environment or repo `.env`, same as
the Statcast ETL:

```bash
uv run python build_retrosheet.py load data/retrosheet_plays_2000_2025.parquet
```

Indexes are created **after** the bulk load (much faster than maintaining them
during insert). `emit-ddl` prints the equivalent `CREATE TABLE` + `CREATE INDEX`
if you prefer to run the SQL yourself.

### Indexes

| Index | Columns | For |
|-------|---------|-----|
| `idx_retrosheet_plays_season_pitcher` | `(season, pitcher_mlbam)` | per-season pitching aggregates |
| `idx_retrosheet_plays_season_batter` | `(season, batter_mlbam)` | per-season batting aggregates |
| `idx_retrosheet_plays_matchup` | `(bat_side, pit_hand)` | handedness splits |

Alternatively, since the warehouse runs
[pg_duckdb](https://github.com/duckdb/pg_duckdb), you can load server-side if the
Parquet is reachable from the database host:

```sql
insert into retrosheet_plays
select * from read_parquet('/path/to/retrosheet_plays_2000_2025.parquet');
```
