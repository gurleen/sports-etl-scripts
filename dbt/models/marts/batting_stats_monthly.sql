{# Batting line per player per season per calendar month. #}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

totals as (
    select
        batter_mlbam as player_id,
        season,
        game_month,
        sum(pa) as pa,
        sum(ab) as ab,
        sum(h) as h,
        sum(singles) as singles,
        sum(doubles) as doubles,
        sum(triples) as triples,
        sum(hr) as hr,
        sum(ubb) as ubb,
        sum(bb) as bb,
        sum(ibb) as ibb,
        sum(hbp) as hbp,
        sum(so) as so,
        sum(sf) as sf
    from events
    where batter_mlbam is not null
    group by batter_mlbam, season, game_month
)

select
    t.player_id,
    t.season,
    t.game_month,
    p.full_name,
    t.pa,
    t.ab,
    t.h,
    t.doubles,
    t.triples,
    t.hr,
    t.bb,
    t.so,
    {{ pbp_batting_rate_stats('w') }}
from totals as t
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
