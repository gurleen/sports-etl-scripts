{#
  Tidy-long batting splits: one row per player x season x split_type x split_value.
  split_type='overall' is the full-season line. RISP = base_state in ('risp','loaded').
#}

with events as (
    select * from {{ ref('stg_pbp__events') }}
    where batter_mlbam is not null
),

{% set splits = [
    ('overall',    "'all'",          'true'),
    ('vs_hand',    'pit_hand',        'pit_hand is not null'),
    ('home_away',  'home_away',       'true'),
    ('outs',       'outs_pre::text',  'outs_pre is not null'),
    ('count',      'count_state',     'true'),
    ('inning',     'inning_bucket',   'true'),
    ('late_close', "'late_close'",    'late_close'),
    ('base_state', 'base_state',      'true'),
] %}

splits as (
{% for stype, sval, filt in splits %}
    select
        batter_mlbam as player_id,
        season,
        '{{ stype }}' as split_type,
        {{ sval }} as split_value,
        sum(pa) as pa, sum(ab) as ab, sum(h) as h,
        sum(singles) as singles, sum(doubles) as doubles, sum(triples) as triples, sum(hr) as hr,
        sum(ubb) as ubb, sum(bb) as bb, sum(ibb) as ibb, sum(hbp) as hbp, sum(so) as so, sum(sf) as sf
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
    s.pa, s.ab, s.h, s.hr, s.bb, s.so,
    {{ pbp_batting_rate_stats('w') }}
from splits as s
left join {{ source('warehouse', 'weights') }} as w on s.season = w.game_year
left join {{ source('warehouse', 'players') }} as p on s.player_id = p.id
