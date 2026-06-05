with games_by_day as (
    select
        game_date as calendar_date,
        game_type,
        max(game_year) as season_year,
        count(*) as games_with_data
    from {{ ref('games') }}
    group by game_date, game_type
),

schedule_by_day as (
    select
        official_date as calendar_date,
        game_type,
        season_year,
        count(*) as games_scheduled
    from {{ source('warehouse', 'mlb_schedule') }}
    where detailed_state != 'Cancelled'
    group by official_date, game_type, season_year
)

select
    coalesce(g.calendar_date, s.calendar_date) as calendar_date,
    coalesce(g.game_type, s.game_type) as game_type,
    coalesce(g.season_year, s.season_year) as season_year,
    coalesce(g.games_with_data, 0) as games_with_data,
    coalesce(s.games_scheduled, 0) as games_scheduled,
    coalesce(g.games_with_data, 0) - coalesce(s.games_scheduled, 0) as data_minus_scheduled
from games_by_day as g
full outer join schedule_by_day as s
    on g.calendar_date = s.calendar_date
    and g.game_type = s.game_type
    and g.season_year = s.season_year
