{#
  Template for pitching stats over an arbitrary date window (e.g. "last 14 days").
  Compile with --vars '{"start_date":"2025-08-01","end_date":"2025-08-14"}', or the
  app substitutes the date bounds and the season for the FIP-constant weights join.
  ER uses responsible-pitcher run-slot attribution within the date window.
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
        sum(so) as so, sum(outs) as outs, sum(runs) as r
    from events
    where pitcher_mlbam is not null
    group by pitcher_mlbam
),

er_slots as (
    select run_b_pitcher_mlbam as pitcher_mlbam, run_b_earned as er from events
    union all
    select run_1_pitcher_mlbam, run_1_earned from events
    union all
    select run_2_pitcher_mlbam, run_2_earned from events
    union all
    select run_3_pitcher_mlbam, run_3_earned from events
),

er_totals as (
    select pitcher_mlbam as player_id, sum(er) as er
    from er_slots
    where pitcher_mlbam is not null
    group by pitcher_mlbam
)

select
    t.player_id,
    p.full_name,
    t.bf, t.h, t.hr, t.bb, t.so, t.r, coalesce(er.er, 0) as er,
    {{ pbp_pitching_rate_stats('w') }}
from totals as t
left join er_totals as er on t.player_id = er.player_id
left join {{ source('warehouse', 'weights') }} as w
    on w.game_year = extract(year from '{{ var("end_date", "2025-12-31") }}'::date)::int
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
order by t.outs desc
