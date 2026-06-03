select *
from {{ ref('stg_statcast__games') }}
