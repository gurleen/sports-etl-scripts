{#
  Season-aware Retrosheet-franchise-code -> league (AL/NL) bridge.

  The play-by-play fact carries a Retrosheet franchise code (bat_team) but no
  league, and neither warehouse source can supply a *season-accurate* league: the
  `teams` table has one league per franchise (not season-keyed) and uses MLBAM
  abbreviations (NYY/SF) rather than the Retrosheet codes used here (NYA/SFN). So
  the mapping lives here, derived only for the (team, season) pairs that actually
  appear in the data.

  Two modern franchises switched leagues; both are handled explicitly:
    - HOU: NL through 2012, AL from 2013.
    - MIL: AL through 1997, NL from 1998.

  Codes not enumerated below (e.g. defunct/relocated pre-modern franchises) resolve
  to NULL, which propagates to a NULL wrc_plus rather than a wrong league — add
  them here as older seasons are loaded.
#}

with team_seasons as (
    select distinct bat_team as team, season
    from {{ ref('stg_pbp__events') }}
    where bat_team is not null
)

select
    team,
    season,
    case
        -- league switches
        when team = 'HOU' then case when season >= 2013 then 'AL' else 'NL' end
        when team = 'MIL' then case when season >= 1998 then 'NL' else 'AL' end
        -- stable American League franchises
        when team in (
            'ANA', 'ATH', 'BAL', 'BOS', 'CHA', 'CLE', 'DET', 'KCA',
            'MIN', 'NYA', 'SEA', 'TBA', 'TEX', 'TOR'
        ) then 'AL'
        -- stable National League franchises
        when team in (
            'ARI', 'ATL', 'CHN', 'CIN', 'COL', 'LAN', 'MIA', 'NYN',
            'PHI', 'PIT', 'SDN', 'SFN', 'SLN', 'WAS'
        ) then 'NL'
    end as league
from team_seasons
