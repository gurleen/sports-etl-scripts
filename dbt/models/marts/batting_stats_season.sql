{#
  Standard + sabermetric batting line per player per season, all seasons, from
  the unified play-by-play fact.

  wRC+ is computed against a per-league (AL/NL), per-season baseline. The league
  for each play comes from int_pbp__team_league (season-aware, since two modern
  franchises switched leagues). The league baseline (wRC/PA) is aggregated
  directly from each league's component totals, so mid-season trades don't distort
  it; a traded player's own line is compared against the league where he took the
  most PAs.

  The park-factor term is still dropped — without a per-PA park lookup it defaults
  to league-average (1.0) and the park adjustment cancels — so this is the
  league-relative, park-neutral wRC+ = (wRC/PA) / (lg wRC/PA) * 100. See
  current_season_batting_stats for the additionally park-adjusted variant.
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
),

team_league as (
    select * from {{ ref('int_pbp__team_league') }}
),

-- one row per play, with the batting team's league attached
events_lg as (
    select
        e.*,
        tl.league
    from events as e
    left join team_league as tl
        on e.bat_team = tl.team
        and e.season = tl.season
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
    from events_lg
    where batter_mlbam is not null
    group by batter_mlbam, season
),

season_games as (
    select season, round(2.0 * count(distinct game_id) / 30.0) as games_per_team
    from events_lg
    group by season
),

-- league each player-season is assigned to (the league of the plurality of his PAs)
player_league as (
    select player_id, season, league
    from (
        select
            batter_mlbam as player_id,
            season,
            league,
            row_number() over (
                partition by batter_mlbam, season
                order by sum(pa) desc, league
            ) as rn
        from events_lg
        where batter_mlbam is not null
        group by batter_mlbam, season, league
    )
    where rn = 1
),

-- Weighted runs created per player-season, from the (unrounded) wOBA components.
wrc as (
    select
        t.player_id,
        t.season,
        case
            when t.pa > 0 and (t.ab + t.ubb + t.sf + t.hbp) > 0 then (
                (
                    (
                        (
                            w.w_bb * t.ubb + w.w_hbp * t.hbp + w.w_single * t.singles
                            + w.w_double * t.doubles + w.w_triple * t.triples + w.w_home_run * t.hr
                        )::numeric / (t.ab + t.ubb + t.sf + t.hbp) - w.league_woba
                    ) / w.woba_scale
                ) + w.runs_per_pa
            ) * t.pa
        end as wrc
    from totals as t
    left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
),

-- league component totals per season+league, for the baseline
league_totals as (
    select
        season,
        league,
        sum(pa) as pa,
        sum(ab) as ab,
        sum(singles) as singles,
        sum(doubles) as doubles,
        sum(triples) as triples,
        sum(hr) as hr,
        sum(ubb) as ubb,
        sum(hbp) as hbp,
        sum(sf) as sf
    from events_lg
    where league is not null
    group by season, league
),

-- per (season, league) baseline: runs created per PA
league_wrc as (
    select
        lt.season,
        lt.league,
        case
            when (lt.ab + lt.ubb + lt.sf + lt.hbp) > 0 then (
                (
                    (
                        w.w_bb * lt.ubb + w.w_hbp * lt.hbp + w.w_single * lt.singles
                        + w.w_double * lt.doubles + w.w_triple * lt.triples + w.w_home_run * lt.hr
                    )::numeric / (lt.ab + lt.ubb + lt.sf + lt.hbp) - w.league_woba
                ) / w.woba_scale
            ) + w.runs_per_pa
        end as wrc_per_pa
    from league_totals as lt
    left join {{ source('warehouse', 'weights') }} as w on lt.season = w.game_year
)

select
    t.player_id,
    t.season,
    pl.league,
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
    {{ pbp_batting_rate_stats('w') }},
    round(wc.wrc::numeric, 0)::integer as wrc,
    round((wc.wrc / nullif(t.pa, 0)) / nullif(lw.wrc_per_pa, 0) * 100)::integer as wrc_plus
from totals as t
left join {{ source('warehouse', 'weights') }} as w on t.season = w.game_year
left join season_games as sg on t.season = sg.season
left join player_league as pl on t.player_id = pl.player_id and t.season = pl.season
left join wrc as wc on t.player_id = wc.player_id and t.season = wc.season
left join league_wrc as lw on t.season = lw.season and pl.league = lw.league
left join {{ source('warehouse', 'players') }} as p on t.player_id = p.id
