with schedule as (
    select
        game_pk,
        season_year,
        official_date,
        game_type,
        away_team_name as away_team,
        home_team_name as home_team,
        abstract_game_state,
        detailed_state,
        venue_name
    from {{ source('warehouse', 'mlb_schedule') }}
    where detailed_state != 'Cancelled'
),

statcast_games as (
    select
        game_pk,
        game_date,
        game_year,
        game_type,
        away_team,
        home_team,
        away_score,
        home_score
    from {{ ref('games') }}
)

select
    coalesce(s.game_pk, g.game_pk) as game_pk,
    coalesce(s.official_date, g.game_date) as calendar_date,
    coalesce(s.season_year, g.game_year) as season_year,
    coalesce(s.game_type, g.game_type) as game_type,
    coalesce(g.away_team, s.away_team) as away_team,
    coalesce(g.home_team, s.home_team) as home_team,
    s.venue_name,
    s.abstract_game_state,
    s.detailed_state,
    g.game_date as statcast_game_date,
    g.away_score as statcast_away_score,
    g.home_score as statcast_home_score,
    (g.game_pk is not null) as has_statcast_data,
    (s.game_pk is not null) as in_schedule,
    case
        when g.game_pk is not null and s.game_pk is not null then 'covered'
        when s.game_pk is not null then 'missing_data'
        else 'data_only'
    end as coverage_status
from schedule as s
full outer join statcast_games as g
    on s.game_pk = g.game_pk
