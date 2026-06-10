{#
  Template for pitching stats over an arbitrary date window (e.g. "last 14 days").
  Compile with --vars '{"start_date":"2025-08-01","end_date":"2025-08-14"}', or the
  app substitutes the date bounds and the season for the FIP-constant weights join.
#}

with events as (
    select *
    from {{ ref('stg_pbp__events') }}
    where game_date between '{{ var("start_date", "2025-01-01") }}'::date
                        and '{{ var("end_date", "2025-12-31") }}'::date
),

totals as (
    select
        pitcher_mlbam as player_id,
        sum(pa) as bf, sum(ab) as ab, sum(h) as h, sum(hr) as hr,
        sum(ubb) as ubb, sum(bb) as bb, sum(ibb) as ibb, sum(hbp) as hbp,
        sum(so) as so, sum(outs) as outs, sum(runs) as r, sum(er) as er
    from events
    where pitcher_mlbam is not null
    group by pitcher_mlbam
)

select
    t.player_id,
    p.full_name,
    t.bf, t.h, t.hr, t.bb, t.so, t.r, t.er,
    {{ pbp_pitching_rate_stats('w') }}
from totals as t
left join {{ source('warehouse', 'weights') }} as w
    on w.game_year = extract(year from '{{ var("end_date", "2025-12-31") }}'::date)::int
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
order by t.outs desc
