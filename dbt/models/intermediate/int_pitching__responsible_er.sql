{#
  Earned runs charged to the responsible pitcher (not the facing pitcher), per
  retrosheet_plays run slots. Summing run_*_earned by run_*_pitcher_mlbam matches
  MLB's official per-pitcher ER (er + tur / Rule 9.16 inherited runners).
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

slots as (
    select season, game_month, run_b_pitcher_mlbam as pitcher_mlbam, run_b_earned as er
    from events
    union all
    select season, game_month, run_1_pitcher_mlbam, run_1_earned
    from events
    union all
    select season, game_month, run_2_pitcher_mlbam, run_2_earned
    from events
    union all
    select season, game_month, run_3_pitcher_mlbam, run_3_earned
    from events
)

select
    pitcher_mlbam,
    season,
    game_month,
    sum(er) as er
from slots
where pitcher_mlbam is not null
group by pitcher_mlbam, season, game_month
