with pitching_stats as (
    select * from {{ ref('int_pitching__player_totals') }}
),

agg_stats as (
    select
        ps.pitcher,
        {{ get_season_year() }} as game_year,
        p.full_name,
        p.current_team_id,
        ps.bf,
        ps.outs,
        ps.outs::numeric / 3.0 as ip,
        ps.ubb,
        ps.bb,
        ps.ibb,
        ps.hbp,
        ps.h,
        ps.hr,
        ps.so,
        ps.r
    from pitching_stats as ps
    left join {{ source('warehouse', 'players') }} as p on ps.pitcher = p.id
)

select
    agg_stats.pitcher,
    agg_stats.game_year,
    agg_stats.full_name,
    agg_stats.current_team_id,
    agg_stats.bf,
    round(agg_stats.ip::numeric, 1) as ip,
    agg_stats.h,
    agg_stats.bb,
    agg_stats.so,
    agg_stats.hr,
    agg_stats.r,
    round(
        case when agg_stats.ip > 0 then 9.0 * agg_stats.r::numeric / agg_stats.ip end,
        2
    ) as era,
    round(
        case when agg_stats.ip > 0 then (agg_stats.bb + agg_stats.h)::numeric / agg_stats.ip end,
        3
    ) as whip,
    round(
        case
            when agg_stats.ip > 0
            then (
                (13 * agg_stats.hr + 3 * (agg_stats.ubb + agg_stats.hbp) - 2 * agg_stats.so)::numeric
                / agg_stats.ip
            ) + w.c_fip
        end,
        2
    ) as fip,
    round(
        case when agg_stats.bf > 0 then agg_stats.so::numeric / agg_stats.bf end,
        3
    ) as k_pct,
    round(
        case when agg_stats.bf > 0 then agg_stats.bb::numeric / agg_stats.bf end,
        3
    ) as bb_pct
from agg_stats
left join {{ source('warehouse', 'weights') }} as w on agg_stats.game_year = w.game_year
