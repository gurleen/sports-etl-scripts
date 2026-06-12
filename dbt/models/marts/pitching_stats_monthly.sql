{#
  Pitching line per pitcher per season per calendar month.
  ER uses responsible-pitcher run-slot attribution; R uses facing pitcher.
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

totals as (
    select
        pitcher_mlbam as player_id,
        season,
        game_month,
        sum(pa) as bf,
        sum(ab) as ab,
        sum(h) as h,
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
    group by pitcher_mlbam, season, game_month
)

select
    t.player_id,
    t.season,
    t.game_month,
    p.full_name,
    t.bf,
    t.h,
    t.hr,
    t.bb,
    t.so,
    t.r,
    coalesce(er.er, 0) as er,
    {{ pbp_pitching_rate_stats('w') }}
from totals as t
left join {{ ref('int_pitching__responsible_er') }} as er
    on t.player_id = er.pitcher_mlbam
    and t.season = er.season
    and t.game_month = er.game_month
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
