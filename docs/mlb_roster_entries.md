# Roster stints (MLB Stats API → `mlb_roster_entries`)

Authoritative **roster membership history** for MLB players, as *stints* (date
intervals), suitable for MLB **service-time** calculation. Each row is one
interval of one player's membership on one team under one status — the API hands
back intervals directly, so there is **no per-day fan-out** and no snapshot
collapse to do.

Companion table: `mlb_transactions` (the *why* — option / IL / DFA / trade), keyed
on the same bare-integer `person_id`. These stints are the *who/where/when*.

- Logic: [`etl_scripts/mlb_roster_entries.py`](../etl_scripts/mlb_roster_entries.py)
- CLI: `etl roster` ([`etl_scripts/commands/roster.py`](../etl_scripts/commands/roster.py))
- Models: [`models/mlb_roster_entries.py`](../models/mlb_roster_entries.py)

## Source

`GET /api/v1/people/{personId}?hydrate=rosterEntries`
(`MlbApiClient.stats.get_person`). One API call per player returns
`people[0].rosterEntries[]`; each entry is a stint. The player **universe** is the
distinct `person_id` set in `mlb_transactions`, scraped one player at a time.

## Grain & schema

**One row per `(person_id, team_id, start_date, status_code)`** — this is the
primary key (idempotent upsert; re-running replaces values, never duplicates).

| Column | Type | Notes |
|---|---|---|
| `person_id` | INTEGER | MLB person id (bare; no FK enforced). |
| `team_id` | INTEGER | The team for this stint — **any level**, not just the 30 MLB clubs. |
| `parent_org_id` | INTEGER | Set for **minor-league affiliates** (points at the MLB parent club); NULL for MLB clubs and other non-affiliated teams. |
| `jersey_number` | TEXT | Nullable (~22% null). |
| `position_code` | TEXT | Always present. `position_abbreviation` alongside (e.g. `DH`, `1B`). |
| `position_abbreviation` | TEXT | |
| `status_code` | TEXT | Stint status — **the key field for service time**. See taxonomy below. |
| `status_desc` | TEXT | Human label for `status_code`. |
| `is_active` | BOOLEAN | ⚠️ Reflects the player's *current* membership snapshot, **not** whether a historical stint accrued. Do **not** use for accrual logic. |
| `is_active_forty_man` | BOOLEAN | Same caveat as `is_active`. |
| `start_date` | DATE | Stint start (always present). |
| `end_date` | DATE | Stint end. **NULL = open/current stint** (clip to `CURRENT_DATE`). |
| `status_date` | DATE | Date the status was last set (always present). |

### Important: this table spans ALL levels (MLB + minors)
Only ~9% of rows are MLB-parent-level; the rest are minor-league affiliate stints,
across **577 distinct `team_id`s**. To restrict to MLB roster membership, filter
`team_id` to the 30 MLB clubs (join your `teams` dimension, which holds only those
30) — **except** for rehab assignments (`RA`), where the `team_id` is the
minor-league club even though the player remains on the MLB IL.

## Status taxonomy (full table, 243,688 rows)

`status_code` is **not sufficient alone** — the same code occurs at MLB and minor
levels (e.g. `A` "Active" is both an MLB active roster and a minor-league active
roster). Combine status **with** roster level for service time.

| code | meaning | n | service time |
|---|---|---:|---|
| `ASG` | Assigned to New Team/Level | 80,446 | ❌ |
| `A`   | Active | 65,093 | ✅ **only at MLB level** |
| `RL`  | Released | 32,564 | ❌ |
| `FA`  | Free Agent | 16,091 | ❌ |
| `RA`  | Rehab Assignment | 11,531 | ✅ (player on MLB IL; `team_id` is the minor club — keep regardless of level) |
| `RM`  | Reassigned to Minors | 10,666 | ❌ |
| `TR`  | Traded | 9,928 | ❌ |
| `RES` | Reserve List (Minors) | 7,389 | ❌ (optional assignment) |
| `CL`  | Claimed | 2,623 | ❌ |
| `RET` | Voluntarily Retired List | 1,930 | ❌ |
| `DES` | Designated for Assignment | 1,531 | ⚠️ short (≤7d) limbo; accrues until outright/release — edge case |
| `D7`  | Injured 7-Day (concussion) | 1,280 | ✅ MLB IL |
| `D60` | Injured 60-Day | 848 | ✅ MLB IL |
| `WA`  | Waived | 605 | ❌ |
| `RST` | Restricted List | 411 | ❌ |
| `TI`  | Temporary Inactive List | 191 | ❌ |
| `ILF` | Injured - Full Season | 160 | ✅ MLB IL |
| `D15` | Injured 15-Day | 118 | ✅ MLB IL |
| `DEV` | Development List | 72 | ❌ |
| `D10` | Injured 10-Day | 71 | ✅ MLB IL |
| `NYR` | Not Yet Reported | 42 | ❌ |
| `SU`  | Suspended | 36 | ❌ |
| `DEC` | Deceased | 32 | ❌ |
| `MIL` | Military Leave | 11 | ❌ |
| `IN`  | Ineligible List | 6 | ❌ |
| `TAX` | Taxi Squad | 6 | ❌ |
| `PL`  | Paternity List | 3 | ✅ (MLB special leave) |
| `ADM` | Administrative Leave | 3 | ⚠️ context-dependent |
| `FME` | Family Medical Emergency | 1 | ✅ (MLB special leave) |

**Accruing set (MLB active + MLB IL + special leave):** `A` (MLB level only),
`D7 D10 D15 D60 ILF` (MLB IL), `RA` (rehab, any level), `PL`/`FME` (special
leave). `DES` and `ADM` are judgement calls.

## Service-time calc (summary)

1 service day per day on the **MLB active roster or MLB IL** during the
**championship season** window; **172 days = 1 year** and each season is **capped
at 172**; 6 years → free agency. Steps: derive each season's regular-season window
from `mlb_schedule` (`game_type='R'`, min/max date), clip accruing stints
(`COALESCE(end_date, CURRENT_DATE)`), intersect with each window, **count distinct
calendar days per player-season** (stints can overlap — dedupe to avoid
double-counting), cap at 172, sum, format `years.days`. Special-case **2020**
(prorated season). Use `mlb_transactions` to cross-check ambiguous stint
boundaries.

## Coverage (as loaded 2026-06-12)

- **243,688 rows**, **44,972 players** with ≥1 stint, **577 teams**, start dates **1924 → present**, **9,766 open stints** (`end_date IS NULL`).
- The player universe is ~75,974 distinct `person_id`s in `mlb_transactions`; the gap is expected and benign: ~30K of those have **no `rosterEntries`** in the API (recent draftees/signings who never accrued a tracked stint), and 380 are non-resolving (404) ids. Every player who has a roster history in the Stats API is loaded.

## Usage

```bash
# One player (smoke test)
uv run etl roster person 660271

# Full backfill over every person in mlb_transactions (concurrent; resumable)
uv run etl roster backfill --max-workers 12 --skip-existing

# Nightly: refresh players moved in the last N days + anyone with an open stint
uv run etl roster update-recent --days 7
```

`update-recent` re-fetches players appearing in the trailing N days of
`mlb_transactions` **plus** anyone holding an open stint (since open stints close
later) — bounded and idempotent. Backend switches via `ETL_DB_BACKEND` (`duckdb`
for local `dev.duckdb`; default is prod Postgres).
