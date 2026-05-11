from typing import List, Literal, TypeAlias
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

HalfInning: TypeAlias = Literal["top", "bottom"]
Handedness: TypeAlias = Literal["R", "L"]


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


class SavantGamefeed(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team_home: List[PitchData]
    team_away: List[PitchData]
