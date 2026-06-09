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
    pitcher,
    1 as plate_appearance,
    case when s.events in ('single', 'double', 'triple', 'home_run') then 1 else 0 end as hit,
    case when s.events = 'walk' then 1 else 0 end as walk,
    case when s.events = 'intent_walk' then 1 else 0 end as intent_walk,
    case when s.events = 'hit_by_pitch' then 1 else 0 end as hit_by_pitch,
    case when s.events = 'single' then 1 else 0 end as single,
    case when s.events = 'double' then 1 else 0 end as double,
    case when s.events = 'triple' then 1 else 0 end as triple,
    case when s.events = 'home_run' then 1 else 0 end as home_run,
    case when s.events = 'strikeout' then 1 else 0 end as strike_out,
    case
        when s.events in (
            'strikeout', 'field_out', 'force_out', 'fielders_choice', 'fielders_choice_out',
            'sac_fly', 'sac_bunt', 'bunt_out'
        ) then 1
        when s.events in (
            'grounded_into_double_play', 'double_play', 'strikeout_double_play',
            'sac_fly_double_play'
        ) then 2
        else 0
    end as outs_on_play,
    coalesce(s.post_bat_score, 0) - coalesce(s.bat_score, 0) as runs_scored
from source as s
