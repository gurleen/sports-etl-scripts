{{ config(materialized='view') }}

{#
  One row per play from the unified play-by-play fact, regular season only, with
  split dimensions derived and additive components renamed/normalized.

  retrosheet_plays holds both sources and they overlap on completed seasons (e.g.
  2025 exists as both 'retrosheet' and 'mlbam'), so we pick ONE source per season
  to avoid double-counting: Retrosheet for any season it has published (the
  finalized historical record), and mlbam only for seasons Retrosheet doesn't yet
  cover (the in-progress current season). Both batting (group by batter) and
  pitching (group by pitcher) marts and custom-period queries build on this.
#}

with raw as (
    select *
    from {{ source('pbp', 'retrosheet_plays') }}
    where game_type = 'regular'
),

retro_seasons as (
    select distinct season from raw where source = 'retrosheet'
),

src as (
    select *
    from raw
    where source = 'retrosheet'
       or season not in (select season from retro_seasons)
)

select
    source,
    game_id,
    play_number,
    game_date,
    season,
    extract(month from game_date)::int as game_month,
    -- subjects
    batter_mlbam,
    pitcher_mlbam,
    -- split dimensions
    bat_side,
    pit_hand,
    case when bat_home then 'home' else 'away' end as home_away,
    outs_pre,
    count_balls || '-' || count_strikes as count_state,
    inning,
    case
        when inning >= 10 then '10+'
        when inning >= 7 then '7-9'
        when inning >= 4 then '4-6'
        else '1-3'
    end as inning_bucket,
    -- late & close: 7th inning or later with the score within one run
    (inning >= 7 and abs(coalesce(score_bat, 0) - coalesce(score_pit, 0)) <= 1) as late_close,
    case
        when on_1b and on_2b and on_3b then 'loaded'
        when on_2b or on_3b then 'risp'
        when on_1b then 'men_on'
        else 'empty'
    end as base_state,
    -- additive components (RISP = base_state in ('risp','loaded'))
    pa,
    ab,
    hit as h,
    single as singles,
    double as doubles,
    triple as triples,
    home_run as hr,
    walk as ubb,
    walk + intent_walk as bb,
    intent_walk as ibb,
    hit_by_pitch as hbp,
    strikeout as so,
    sac_fly as sf,
    sac_bunt as sh,
    outs_on_play as outs,
    runs_on_play as runs,
    earned_runs as er,
    -- responsible-pitcher run slots (basis for official season ER/ERA)
    run_b_pitcher_mlbam,
    run_b_earned,
    run_1_pitcher_mlbam,
    run_1_earned,
    run_2_pitcher_mlbam,
    run_2_earned,
    run_3_pitcher_mlbam,
    run_3_earned
from src
