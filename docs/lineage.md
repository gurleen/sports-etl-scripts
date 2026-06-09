# Lineage (Prefect flows + dbt marts)

This repo has two orchestration layers:

1. **Prefect flows** load raw warehouse tables in Postgres (`public`).
2. **dbt** builds derived objects in the `baseball` schema from those sources.

There is no single auto-generated diagram for both layers. Use this doc for the static picture; use **dbt docs** and the **Prefect UI** for interactive, up-to-date views (see [Viewing lineage interactively](#viewing-lineage-interactively)).

Related: [orchestration.md](orchestration.md), [dbt.md](dbt.md).

## End-to-end picture

```mermaid
flowchart LR
  subgraph prefect["Prefect ingest"]
    SC[statcast flows]
    SE[statcast_extra flow]
    MLB[mlb_schedule flow]
  end

  subgraph public["public schema"]
    T1[(statcast)]
    T2[(statcast_extra)]
    T3[(mlb_schedule)]
    T4[(weights, players, teams, park_factors, …)]
  end

  subgraph dbt["dbt (baseball schema)"]
    M[marts]
  end

  SC --> T1
  SE --> T2
  MLB --> T3
  T1 --> M
  T2 --> M
  T3 --> M
  T4 --> M
```

---

## Prefect flow call graph

Deployments are declared in [`prefect.yaml`](../prefect.yaml). Nested flows are invoked from Python in [`flows/`](../flows/); Prefect shows them as child runs in the UI.

```mermaid
flowchart TD
  recent["statcast-update-recent"]
  date["statcast-update-date"]
  backfill["statcast-backfill"]
  extra_dep["statcast-extra-ingest-year"]
  full["statcast-update-full"]
  season["statcast-season"]
  dbt_dep["dbt-rebuild-baseball"]
  mlb["mlb-schedule-ingest-year"]

  ingest["Statcast ingest tasks"]
  extra_flow["statcast_extra_ingest_year_flow"]
  dbt_flow["dbt_rebuild_baseball_flow"]

  recent --> ingest
  recent --> extra_flow
  recent --> dbt_flow

  date --> ingest
  date --> extra_flow
  date --> dbt_flow

  backfill --> ingest
  backfill --> extra_flow
  backfill --> dbt_flow

  extra_dep --> extra_flow
  extra_flow -->|rebuild_dbt=true| dbt_extra["dbt: post_statcast_extra_ingest"]

  full --> ingest
  full --> dbt_flow

  season --> ingest
  season --> dbt_flow

  dbt_dep --> dbt_flow
  dbt_flow --> dbt_all["dbt: post_statcast_ingest"]

  extra_flow -.->|rebuild_dbt=false when nested| dbt_extra
```

When a parent Statcast flow calls `statcast_extra_ingest_year_flow`, it passes `rebuild_dbt=False` so dbt is not run twice. The parent then runs `dbt_rebuild_baseball_flow` with the full `post_statcast_ingest` selector.

### Deployments at a glance

| Deployment | Warehouse writes | Nested flows | dbt selector (if run) |
|------------|------------------|--------------|------------------------|
| `statcast-update-recent` | `statcast`, `statcast_extra` | extra → full dbt | `post_statcast_ingest` |
| `statcast-update-date` | `statcast`, `statcast_extra` | extra → full dbt | `post_statcast_ingest` |
| `statcast-backfill` | `statcast`, optional `statcast_extra` | extra (if dates filled) → full dbt | `post_statcast_ingest` |
| `statcast-extra-ingest-year` | `statcast_extra` | dbt when rows written | `post_statcast_extra_ingest` |
| `statcast-update-full` | `statcast` | full dbt only | `post_statcast_ingest` |
| `statcast-season` | `statcast` | full dbt only | `post_statcast_ingest` |
| `dbt-rebuild-baseball` | — | dbt only | `post_statcast_ingest` (default) |
| `mlb-schedule-ingest-year` | `mlb_schedule` | none | none (run dbt manually for coverage marts) |

dbt rebuilds are **skipped** when [`statcast_relevant_data_changed`](../etl_scripts/dbt_runner.py) reports no changes, unless `force=true` on `dbt-rebuild-baseball`.

---

## dbt model DAG

Models live under [`dbt/models/`](../dbt/models/). Marts default to `materialized_view` and tag `post_statcast_ingest` ([`dbt_project.yml`](../dbt_project.yml)). `statcast_events` and `abs_challenges` also carry tag `post_statcast_extra_ingest`.

### Sources → marts

```mermaid
flowchart BT
  subgraph sources["sources (public)"]
    statcast[(statcast)]
    extra[(statcast_extra)]
    schedule[(mlb_schedule)]
    weights[(weights)]
    players[(players)]
    teams[(teams)]
    park[(park_factors)]
  end

  stg_games[stg_statcast__games]
  stg_bat[stg_statcast__batting_events]
  stg_pit[stg_statcast__pitching_events]
  games[games]
  game_cov[game_coverage]
  daily_cov[daily_game_coverage]
  events[statcast_events]
  abs[abs_challenges]
  int_tot[int_batting__player_totals]
  int_rate[int_batting__rate_stats]
  int_adv[int_batting__advanced]
  int_lrc[int_batting__league_wrc]
  int_tq[int_batting__team_qualifiers]
  batting[current_season_batting_stats]
  int_pit_tot[int_pitching__player_totals]
  int_pit_rate[int_pitching__rate_stats]
  pitching[current_season_pitching_stats]

  statcast --> stg_games --> games
  statcast --> stg_bat --> int_tot --> int_rate --> int_adv
  statcast --> stg_pit --> int_pit_tot --> int_pit_rate --> pitching
  int_adv --> int_lrc
  int_adv --> batting
  statcast --> int_tq --> int_adv
  weights --> int_rate
  weights --> int_adv
  weights --> int_pit_rate
  weights --> batting
  players --> int_rate
  players --> int_pit_rate
  players --> batting
  players --> pitching
  teams --> int_adv
  teams --> pitching
  park --> batting
  games --> game_cov
  schedule --> game_cov
  games --> daily_cov
  schedule --> daily_cov
  statcast --> events
  extra --> events
  extra --> abs
```

### Selectors → models

Defined in [`selectors.yml`](../selectors.yml).

| Selector | Tag | Models |
|----------|-----|--------|
| `post_statcast_ingest` | `post_statcast_ingest` | All marts (default after Statcast ingest) |
| `post_statcast_extra_ingest` | `post_statcast_extra_ingest` | `statcast_events`, `abs_challenges` |

List models for a selector:

```bash
uv run dbt list --selector post_statcast_ingest --resource-type model
uv run dbt list --selector post_statcast_extra_ingest --resource-type model
```

Upstream of one mart:

```bash
uv run dbt list --select +statcast_events+ --resource-type model
```

---

## Viewing lineage interactively

### dbt (model dependencies)

Generates a browsable DAG from `ref()` / `source()` in the project:

```bash
uv run dbt docs generate
uv run dbt docs serve
```

Open the **Lineage** tab and click any node to expand upstream/downstream.

### Prefect (flow nesting)

For a specific run: Prefect UI → flow run → task/subflow tree. Nested flows (`statcast_extra_ingest_year_flow`, `dbt_rebuild_baseball_flow`) appear as child runs when a parent deployment triggers them.

Prefect does **not** produce a static repo-wide diagram of deployment relationships; those are only encoded in Python call sites under [`flows/`](../flows/).

---

## Keeping this doc accurate

Update this file when you:

- Add or rewire Prefect deployments or subflow calls in `flows/`.
- Add dbt models, sources, or selectors.
- Change which selector runs after which ingest.

For dbt, `dbt docs generate` always reflects the current model graph. For Prefect, the UI reflects actual run structure per deployment.
