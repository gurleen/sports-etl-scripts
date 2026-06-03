with source as (
    select *
    from {{ source('warehouse', 'statcast') }}
    where game_pk is not null
)

select
    game_pk,
    max(game_date) as game_date,
    max(game_year) as game_year,
    max(game_type) as game_type,
    max(home_team) as home_team,
    max(away_team) as away_team,
    max(post_home_score) as home_score,
    max(post_away_score) as away_score
from source
group by game_pk
