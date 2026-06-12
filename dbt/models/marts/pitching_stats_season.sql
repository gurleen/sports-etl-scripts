{#
  Standard + sabermetric pitching line per pitcher per season, all seasons.
  Counting stats (BF, H, SO, outs, R, etc.) use the facing pitcher on each play.
  ER/ERA use responsible-pitcher attribution via run_*_earned slots (official MLB
  basis; correct for inherited runners). Split marts keep facing-pitcher ER.
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
        sum(runs) as r
    from events
    where pitcher_mlbam is not null
    group by pitcher_mlbam, season
),

er_totals as (
    select
        pitcher_mlbam as player_id,
        season,
        sum(er) as er
    from {{ ref('int_pitching__responsible_er') }}
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
    t.bf::integer,
    t.h::integer,
    t.hr::integer,
    t.bb::integer,
    t.ubb::integer,
    t.ibb::integer,
    t.hbp::integer,
    t.so::integer,
    t.r::integer,
    coalesce(er.er, 0)::integer as er,
    (t.outs / 3.0 >= sg.games_per_team) as qualified,
    {{ pbp_pitching_rate_stats('w') }}
from totals as t
left join er_totals as er
    on t.player_id = er.player_id
    and t.season = er.season
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join season_games as sg on t.season = sg.season
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
