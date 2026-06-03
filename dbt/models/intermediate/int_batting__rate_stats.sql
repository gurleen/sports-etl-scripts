with batting_stats as (
    select * from {{ ref('int_batting__player_totals') }}
),

derived as (
    select
        *,
        ab - so - hr + sf as babip_denom
    from batting_stats
),

agg_stats as (
    select
        d.batter,
        {{ get_season_year() }} as game_year,
        p.full_name,
        p.current_team_id,
        d.pa,
        d.ab,
        d.ubb,
        d.bb,
        d.ibb,
        d.hbp,
        d.h,
        d.single,
        d.double,
        d.triple,
        d.hr,
        d.sf,
        d.so,
        case when d.ab > 0 then d.h::numeric / d.ab end as batting_average,
        case
            when (d.ab + d.bb + d.hbp) > 0
            then (d.h + d.bb + d.hbp)::numeric / (d.ab + d.bb + d.hbp + d.sf)
        end as on_base_percentage,
        case
            when d.ab > 0
            then (d.single + 2 * d.double + 3 * d.triple + 4 * d.hr)::numeric / d.ab
        end as slugging_percentage,
        case
            when d.babip_denom > 0 then (d.h - d.hr)::numeric / d.babip_denom
        end as babip_value
    from derived as d
    left join {{ source('warehouse', 'players') }} as p on d.batter = p.id
)

select
    agg_stats.batter,
    agg_stats.game_year,
    agg_stats.full_name,
    agg_stats.current_team_id,
    agg_stats.pa,
    agg_stats.ab,
    agg_stats.ubb,
    agg_stats.bb,
    agg_stats.ibb,
    agg_stats.hbp,
    agg_stats.h,
    agg_stats.single,
    agg_stats.double,
    agg_stats.triple,
    agg_stats.hr,
    agg_stats.sf,
    agg_stats.so,
    round(agg_stats.batting_average::numeric, 3) as avg,
    round(agg_stats.on_base_percentage::numeric, 3) as obp,
    round(agg_stats.slugging_percentage::numeric, 3) as slg,
    round(agg_stats.babip_value::numeric, 3) as babip,
    round(
        case
            when (agg_stats.ab + agg_stats.bb - agg_stats.ibb + agg_stats.sf + agg_stats.hbp) > 0
            then (
                (w.w_bb * agg_stats.ubb) + (w.w_hbp * agg_stats.hbp) + (w.w_single * agg_stats.single)
                + (w.w_double * agg_stats.double) + (w.w_triple * agg_stats.triple) + (w.w_home_run * agg_stats.hr)
            )::numeric / (agg_stats.ab + agg_stats.bb - agg_stats.ibb + agg_stats.sf + agg_stats.hbp)
            else 0
        end,
        3
    ) as woba
from agg_stats
left join {{ source('warehouse', 'weights') }} as w on agg_stats.game_year = w.game_year
