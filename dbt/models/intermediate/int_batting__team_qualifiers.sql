with unique_teams as (
    select distinct
        case when inning_topbot = 'Top' then away_team else home_team end as team,
        game_pk
    from {{ source('warehouse', 'statcast') }}
    where game_year = {{ get_season_year() }}
)

select
    team,
    count(distinct game_pk) as games,
    count(distinct game_pk) * 3.1 as min_pa
from unique_teams
group by team
