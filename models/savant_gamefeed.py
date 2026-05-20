from typing import Annotated, List, Literal, Optional, TypeAlias
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PlainValidator,
    ValidationError,
    model_validator,
)

HalfInning: TypeAlias = Literal["top", "bottom"]
Handedness: TypeAlias = Literal["R", "L"]


class AbsChallenge(BaseModel):
    """Nested ``abs_challenge`` on gamefeed rows when ``is_abs_challenge`` is true."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    is_batter: bool = Field(validation_alias=AliasChoices("isBatter", "is_batter"))
    is_in_progress: bool = Field(validation_alias=AliasChoices("isInProgress", "is_in_progress"))
    is_overturned: bool = Field(validation_alias=AliasChoices("isOverturned", "is_overturned"))
    challenge_team_id: int = Field(validation_alias=AliasChoices("challengeTeamId", "challenge_team_id"))
    edge_distance: float = Field(validation_alias=AliasChoices("edgeDistance", "edge_distance"))
    edge_distance_calc: float = Field(validation_alias=AliasChoices("edgeDistanceCalc", "edge_distance_calc"))
    challenging_player_id: int = Field(
        validation_alias=AliasChoices("challengingPlayerId", "challenging_player_id")
    )
    challenging_player_type: str = Field(
        validation_alias=AliasChoices("challengingPlayerType", "challenging_player_type")
    )


class PitchData(BaseModel):
    """One Savant gamefeed pitch row (`/gf?game_pk=…`). Accepts camelCase API keys."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    play_type: str = Field(validation_alias=AliasChoices("type", "play_type"))
    year: int
    sport_id: int = Field(validation_alias=AliasChoices("sportId", "sport_id"))
    play_id: UUID
    inning: int
    half_inning: HalfInning
    ab_number: int
    cap_index: int
    outs: int
    batter: int
    stand: Handedness
    batter_name: str
    pitcher: int
    p_throws: Handedness
    pitcher_name: str
    catcher: int
    catcher_name: str
    team_batting: str
    team_fielding: str
    team_batting_id: int
    team_fielding_id: int
    result: str
    des: str
    events: str
    strikes: int
    balls: int
    pre_strikes: int
    pre_balls: int
    call: str
    call_name: str
    pitch_type: str
    pitch_name: str
    description: str
    result_code: str
    pitch_call: str
    is_strike_swinging: bool
    balls_and_strikes: str
    start_speed: float
    end_speed: float
    sz_top: float
    sz_bot: float
    sz_depth: float
    sz_width: int
    extension: float
    plate_time: float = Field(validation_alias=AliasChoices("plateTime", "plate_time"))
    zone: int
    spin_rate: int
    break_x: float = Field(validation_alias=AliasChoices("breakX", "break_x"))
    break_z: float = Field(validation_alias=AliasChoices("breakZ", "break_z"))
    px: float
    pz: float
    x0: float
    y0: float
    z0: float
    ax: float
    ay: float
    az: float
    vx0: float
    vy0: float
    vz0: float
    pfx_x: float = Field(validation_alias=AliasChoices("pfxX", "pfx_x"))
    pfx_z: float = Field(validation_alias=AliasChoices("pfxZ", "pfx_z"))
    pfx_z_with_gravity: float = Field(validation_alias=AliasChoices("pfxZWithGravity", "pfx_z_with_gravity"))
    break_x_inches: float = Field(validation_alias=AliasChoices("breakXInches", "break_x_inches"))
    break_x_feet: float = Field(validation_alias=AliasChoices("breakXFeet", "break_x_feet"))
    break_z_induced_inches: float = Field(
        validation_alias=AliasChoices("breakZInducedInches", "break_z_induced_inches")
    )
    break_z_induced_feet: float = Field(
        validation_alias=AliasChoices("breakZInducedFeet", "break_z_induced_feet")
    )
    break_z_with_gravity_inches: float = Field(
        validation_alias=AliasChoices("breakZWithGravityInches", "break_z_with_gravity_inches")
    )
    break_z_with_gravity_feet: float = Field(
        validation_alias=AliasChoices("breakZWithGravityFeet", "break_z_with_gravity_feet")
    )
    pfx_z_direction: str = Field(validation_alias=AliasChoices("pfxZDirection", "pfx_z_direction"))
    pfx_x_with_gravity: float = Field(validation_alias=AliasChoices("pfxXWithGravity", "pfx_x_with_gravity"))
    pfx_x_no_abs: float = Field(validation_alias=AliasChoices("pfxXNoAbs", "pfx_x_no_abs"))
    plate_time_sz_depth: float = Field(
        validation_alias=AliasChoices("plateTimeSZDepth", "plate_time_sz_depth")
    )
    plate_x_poly: float = Field(validation_alias=AliasChoices("plateXPoly", "plate_x_poly"))
    plate_y_poly: float = Field(validation_alias=AliasChoices("plateYPoly", "plate_y_poly"))
    plate_z_poly: float = Field(validation_alias=AliasChoices("plateZPoly", "plate_z_poly"))
    pfx_x_direction: str = Field(validation_alias=AliasChoices("pfxXDirection", "pfx_x_direction"))
    induced_break_z_forced_sign: str = Field(
        validation_alias=AliasChoices("inducedBreakZForcedSign", "induced_break_z_forced_sign")
    )
    ivb_z_direction: str = Field(validation_alias=AliasChoices("ivbZDirection", "ivb_z_direction"))
    savant_is_in_zone: bool = Field(validation_alias=AliasChoices("savantIsInZone", "savant_is_in_zone"))
    is_in_zone: bool = Field(validation_alias=AliasChoices("isInZone", "is_in_zone"))
    is_sword: bool = Field(validation_alias=AliasChoices("isSword", "is_sword"))
    is_bip_out: str
    is_abs_challenge: Optional[bool] = False
    abs_challenge_is_batter: Optional[bool] = None
    abs_challenge_is_in_progress: Optional[bool] = None
    abs_challenge_is_overturned: Optional[bool] = None
    abs_challenge_challenge_team_id: Optional[int] = None
    abs_challenge_edge_distance: Optional[float] = None
    abs_challenge_edge_distance_calc: Optional[float] = None
    abs_challenge_challenging_player_id: Optional[int] = None
    abs_challenge_challenging_player_type: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_abs_challenge(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        nested = data.get("abs_challenge")
        if nested is None or not isinstance(nested, dict):
            return data
        out = {k: v for k, v in data.items() if k != "abs_challenge"}
        try:
            ac = AbsChallenge.model_validate(nested)
        except ValidationError:
            return out
        for key, value in ac.model_dump(mode="python").items():
            out[f"abs_challenge_{key}"] = value
        return out


def parse_pitch_rows(items: object) -> list[PitchData]:
    """Keep only gamefeed rows that satisfy :class:`PitchData` (e.g. skip ``no_pitch`` stubs)."""
    if not isinstance(items, list):
        raise TypeError(f"expected list, got {type(items).__name__}")
    parsed: list[PitchData] = []
    for item in items:
        if isinstance(item, PitchData):
            parsed.append(item)
            continue
        try:
            parsed.append(PitchData.model_validate(item))
        except ValidationError:
            continue
    return parsed


PitchRowList = Annotated[list[PitchData], PlainValidator(parse_pitch_rows)]


class SavantGamefeed(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team_home: PitchRowList
    team_away: PitchRowList
