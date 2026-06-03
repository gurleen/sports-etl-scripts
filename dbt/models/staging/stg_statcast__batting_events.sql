with source as (
    select *
    from {{ source('warehouse', 'statcast') }}
    where events not in ('truncated_pa', '')
      and game_type = 'R'
      and game_year = {{ get_season_year() }}
)

select
    game_date,
    game_pk,
    game_year,
    batter,
    1 as plate_appearance,
    case
        when s.events not in (
            'walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play',
            'catcher_interference', 'intent_walk', 'catchers_interf'
        ) then 1
        else 0
    end as at_bat,
    case when s.events in ('single', 'double', 'triple', 'home_run') then 1 else 0 end as hit,
    case when s.events = 'walk' then 1 else 0 end as walk,
    case when s.events = 'intent_walk' then 1 else 0 end as intent_walk,
    case when s.events = 'hit_by_pitch' then 1 else 0 end as hit_by_pitch,
    case when s.events = 'single' then 1 else 0 end as single,
    case when s.events = 'double' then 1 else 0 end as double,
    case when s.events = 'triple' then 1 else 0 end as triple,
    case when s.events = 'home_run' then 1 else 0 end as home_run,
    case when s.events = 'sac_fly' then 1 else 0 end as sac_fly,
    case when s.events = 'strikeout' then 1 else 0 end as strike_out
from source as s
