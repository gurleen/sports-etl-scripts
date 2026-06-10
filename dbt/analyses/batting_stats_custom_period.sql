{#
  Template for batting stats over an arbitrary date window (e.g. "last 14 days").
  Compile with --vars '{"start_date":"2025-08-01","end_date":"2025-08-14"}', or the
  app substitutes the two date bounds and the season for the wOBA weights join.
  Uses the same stg fact + rate macro as the stored marts, so numbers are identical.
#}

with events as (
    select *
    from {{ ref('stg_pbp__events') }}
    where game_date between '{{ var("start_date", "2025-01-01") }}'::date
                        and '{{ var("end_date", "2025-12-31") }}'::date
),

totals as (
    select
        batter_mlbam as player_id,
        sum(pa) as pa, sum(ab) as ab, sum(h) as h,
        sum(singles) as singles, sum(doubles) as doubles, sum(triples) as triples, sum(hr) as hr,
        sum(ubb) as ubb, sum(bb) as bb, sum(ibb) as ibb, sum(hbp) as hbp, sum(so) as so, sum(sf) as sf
    from events
    where batter_mlbam is not null
    group by batter_mlbam
)

select
    t.player_id,
    p.full_name,
    t.pa, t.ab, t.h, t.hr, t.bb, t.so,
    {{ pbp_batting_rate_stats('w') }}
from totals as t
left join {{ source('warehouse', 'weights') }} as w
    on w.game_year = extract(year from '{{ var("end_date", "2025-12-31") }}'::date)::int
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
order by t.pa desc
