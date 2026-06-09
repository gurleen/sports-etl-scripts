with final_stats as (
    select
        r.pitcher as player_id,
        p.full_name,
        p.position_name,
        t.abbreviation as team,
        t.league,
        r.bf,
        r.ip,
        r.h,
        r.bb,
        r.so,
        r.hr,
        r.r,
        r.era,
        r.whip,
        r.fip,
        r.k_pct,
        r.bb_pct
    from {{ ref('int_pitching__rate_stats') }} as r
    left join {{ source('warehouse', 'teams') }} as t on r.current_team_id = t.id
    left join {{ source('warehouse', 'players') }} as p on r.pitcher = p.id
)

select *
from final_stats
order by fip asc
