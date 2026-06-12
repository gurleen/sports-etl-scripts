{#
  Standard + sabermetric batting line per player per season, all seasons, from
  the unified play-by-play fact. wRC+ is intentionally omitted (needs the
  season-aware league dimension) — see docs.
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

totals as (
    select
        batter_mlbam as player_id,
        season,
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
    group by batter_mlbam, season
),

season_games as (
    select season, round(2.0 * count(distinct game_id) / 30.0) as games_per_team
    from events
    group by season
)

select
    t.player_id,
    t.season,
    p.full_name,
    t.pa::integer,
    t.ab::integer,
    t.h::integer,
    t.singles::integer,
    t.doubles::integer,
    t.triples::integer,
    t.hr::integer,
    t.bb::integer,
    t.ubb::integer,
    t.ibb::integer,
    t.hbp::integer,
    t.so::integer,
    t.sf::integer,
    (t.pa >= 3.1 * sg.games_per_team) as qualified,
    {{ pbp_batting_rate_stats('w') }}
from totals as t
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join season_games as sg on t.season = sg.season
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
