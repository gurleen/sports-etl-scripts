with agg as (
    select * from {{ ref('int_batting__rate_stats') }}
),

adv as (
    select
        a.batter as player_id,
        a.game_year,
        a.full_name,
        t.abbreviation as team,
        t.league,
        a.pa >= coalesce(tq.min_pa, 0) as qualified,
        a.pa,
        a.ab,
        a.so,
        a.avg,
        a.obp,
        a.slg,
        a.obp + a.slg as ops,
        a.babip,
        a.woba,
        ((a.woba - w.league_woba) / w.woba_scale) * a.pa as wraa,
        (
            ((a.woba - w.league_woba) / w.woba_scale) + w.runs_per_pa
        ) * a.pa as wrc,
        round((a.bb::numeric / a.pa), 3) as bb_pct,
        round((a.so::numeric / a.pa), 3) as so_pct
    from agg as a
    left join {{ source('warehouse', 'teams') }} as t on a.current_team_id = t.id
    left join {{ ref('int_batting__team_qualifiers') }} as tq on t.abbreviation = tq.team
    left join {{ source('warehouse', 'weights') }} as w on a.game_year = w.game_year
)

select
    *,
    wraa / pa as wraa_per_pa
from adv
