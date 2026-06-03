select
    league,
    sum(wrc) / sum(pa) as wrc_per_pa
from {{ ref('int_batting__advanced') }}
where league is not null
group by league
