with logs as (
    select
        *,
        walk + intent_walk as all_walks
    from {{ ref('stg_statcast__pitching_events') }}
)

select
    pitcher,
    sum(plate_appearance) as bf,
    sum(outs_on_play) as outs,
    sum(walk) as ubb,
    sum(all_walks) as bb,
    sum(intent_walk) as ibb,
    sum(hit_by_pitch) as hbp,
    sum(hit) as h,
    sum(home_run) as hr,
    sum(strike_out) as so,
    sum(runs_scored) as r
from logs
group by pitcher
