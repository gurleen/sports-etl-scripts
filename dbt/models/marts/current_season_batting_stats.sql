with adv as (
    select * from {{ ref('int_batting__advanced') }}
),

final_stats as (
    select
        a.player_id,
        p.full_name,
        p.position_name,
        a.team,
        a.league,
        a.pa,
        a.ab,
        a.avg,
        a.obp,
        a.slg,
        a.ops,
        a.babip,
        a.woba,
        round(a.wraa::numeric, 1) as wraa,
        round(a.wrc::numeric, 0) as wrc,
        round(
            (
                (
                    a.wraa_per_pa + w.runs_per_pa
                ) + (
                    w.runs_per_pa - ((pf.five_yr / 100.0) * w.runs_per_pa)
                )
            ) / lw.wrc_per_pa * 100
        )::int as wrc_plus,
        a.bb_pct,
        a.so_pct
    from adv as a
    left join {{ ref('int_batting__league_wrc') }} as lw on a.league = lw.league
    left join {{ source('warehouse', 'weights') }} as w on a.game_year = w.game_year
    left join {{ source('warehouse', 'park_factors') }} as pf
        on a.game_year - 1 = pf.game_year
        and a.team = pf.team
    left join {{ source('warehouse', 'players') }} as p on a.player_id = p.id
)

select *
from final_stats
order by woba desc
