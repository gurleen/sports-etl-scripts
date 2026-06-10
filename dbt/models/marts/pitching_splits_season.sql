{#
  Tidy-long pitching splits: one row per pitcher x season x split_type x split_value.
  vs_hand is the batter's side; home_away is the pitcher's team perspective.
  Runs/ER attributed to the facing pitcher (see pitching_stats_season note).
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
    where pitcher_mlbam is not null
),

{% set splits = [
    ('overall',    "'all'",                                          'true'),
    ('vs_hand',    'bat_side',                                        'bat_side is not null'),
    ('home_away',  "case when home_away = 'home' then 'away' else 'home' end", 'true'),
    ('outs',       'outs_pre::text',                                  'outs_pre is not null'),
    ('count',      'count_state',                                     'true'),
    ('inning',     'inning_bucket',                                   'true'),
    ('late_close', "'late_close'",                                    'late_close'),
    ('base_state', 'base_state',                                      'true'),
] %}

splits as (
{% for stype, sval, filt in splits %}
    select
        pitcher_mlbam as player_id,
        season,
        '{{ stype }}' as split_type,
        {{ sval }} as split_value,
        sum(pa) as bf, sum(ab) as ab, sum(h) as h,
        sum(hr) as hr, sum(ubb) as ubb, sum(bb) as bb, sum(ibb) as ibb,
        sum(hbp) as hbp, sum(so) as so, sum(outs) as outs, sum(runs) as r, sum(er) as er
    from events
    where {{ filt }}
    group by player_id, season, split_value
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)

select
    s.player_id,
    s.season,
    s.split_type,
    s.split_value,
    p.full_name,
    s.bf, s.h, s.hr, s.bb, s.so, s.r, s.er,
    {{ pbp_pitching_rate_stats('w') }}
from splits as s
left join {{ source('warehouse', 'weights') }} as w on s.season = w.game_year
left join {{ source('warehouse', 'players') }} as p on s.player_id = p.id
