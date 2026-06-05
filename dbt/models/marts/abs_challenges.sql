select
    game_pk,
    play_id,
    ab_number,
    pitch_number,
    inning,
    half_inning,
    pre_balls,
    pre_strikes,
    pitch_type,
    pitch_call,
    abs_challenge_is_batter as is_batter,
    abs_challenge_is_in_progress as is_in_progress,
    abs_challenge_is_overturned as is_overturned,
    abs_challenge_challenge_team_id as challenge_team_id,
    round((abs_challenge_edge_distance * 12)::numeric, 2) as edge_distance,
    abs_challenge_challenging_player_id as challenging_player_id,
    abs_challenge_challenging_player_type as challenging_player_type,
    case
        when abs_challenge_is_overturned then 'overturned'
        when abs_challenge_is_overturned = false then 'confirmed'
        else 'pending'
    end as challenge_outcome
from {{ source('warehouse', 'statcast_extra') }}
where coalesce(is_abs_challenge, false)
