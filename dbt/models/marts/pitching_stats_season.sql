{#
  Standard + sabermetric pitching line per pitcher per season, all seasons.
  Runs/earned runs are attributed to the pitcher facing the batter on each play
  (earned/unearned classification is exact; exact responsible-pitcher ERA for
  inherited runners is available via the run_*_earned columns on retrosheet_plays).
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

totals as (
    select
        pitcher_mlbam as player_id,
        season,
        sum(pa) as bf,
        sum(ab) as ab,
        sum(h) as h,
        sum(singles) as singles,
        sum(doubles) as doubles,
        sum(triples) as triples,
        sum(hr) as hr,
        sum(ubb) as ubb,
        sum(bb) as bb,
        sum(ibb) as ibb,
        sum(hbp) as hbp,
        sum(so) as so,
        sum(outs) as outs,
        sum(runs) as r,
        sum(er) as er
    from events
    where pitcher_mlbam is not null
    group by pitcher_mlbam, season
),

season_games as (
    select season, round(2.0 * count(distinct game_id) / 30.0) as games_per_team
    from events
    group by season
)

select
    t.player_id,
    t.season,
    p.full_name,
    t.bf,
    t.h,
    t.hr,
    t.bb,
    t.ubb,
    t.ibb,
    t.hbp,
    t.so,
    t.r,
    t.er,
    (t.outs / 3.0 >= sg.games_per_team) as qualified,
    {{ pbp_pitching_rate_stats('w') }}
from totals as t
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join season_games as sg on t.season = sg.season
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
