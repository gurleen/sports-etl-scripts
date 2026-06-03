with logs as (
    select
        *,
        walk + intent_walk as all_walks
    from {{ ref('stg_statcast__batting_events') }}
)

select
    batter,
    sum(plate_appearance) as pa,
    sum(at_bat) as ab,
    sum(walk) as ubb,
    sum(all_walks) as bb,
    sum(intent_walk) as ibb,
    sum(hit_by_pitch) as hbp,
    sum(hit) as h,
    sum(single) as single,
    sum(double) as double,
    sum(triple) as triple,
    sum(home_run) as hr,
    sum(sac_fly) as sf,
    sum(strike_out) as so
from logs
group by batter
