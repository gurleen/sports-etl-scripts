from typing import List, Literal, TypeAlias
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

HalfInning: TypeAlias = Literal["top", "bottom"]
Handedness: TypeAlias = Literal["R", "L"]


class PitchData(BaseModel):
    """One Savant gamefeed pitch row (`/gf?game_pk=…`). Accepts camelCase API keys. All fields optional for sparse payloads."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    play_type: str | None = Field(default=None, validation_alias=AliasChoices("type", "play_type"))
    year: int | None = None
    sport_id: int | None = Field(default=None, validation_alias=AliasChoices("sportId", "sport_id"))
    play_id: UUID | None = None
    inning: int | None = None
    half_inning: HalfInning | None = None
    ab_number: int | None = None
    cap_index: int | None = None
    outs: int | None = None
    batter: int | None = None
    stand: Handedness | None = None
    batter_name: str | None = None
    pitcher: int | None = None
    p_throws: Handedness | None = None
    pitcher_name: str | None = None
    catcher: int | None = None
    catcher_name: str | None = None
    team_batting: str | None = None
    team_fielding: str | None = None
    team_batting_id: int | None = None
    team_fielding_id: int | None = None
    result: str | None = None
    des: str | None = None
    events: str | None = None
    strikes: int | None = None
    balls: int | None = None
    pre_strikes: int | None = None
    pre_balls: int | None = None
    call: str | None = None
    call_name: str | None = None
    pitch_type: str | None = None
    pitch_name: str | None = None
    description: str | None = None
    result_code: str | None = None
    pitch_call: str | None = None
    is_strike_swinging: bool | None = None
    balls_and_strikes: str | None = None
    start_speed: float | None = None
    end_speed: float | None = None
    sz_top: float | None = None
    sz_bot: float | None = None
    sz_depth: float | None = None
    sz_width: int | None = None
    extension: float | None = None
    plate_time: float | None = Field(default=None, validation_alias=AliasChoices("plateTime", "plate_time"))
    zone: int | None = None
    spin_rate: int | None = None
    break_x: float | None = Field(default=None, validation_alias=AliasChoices("breakX", "break_x"))
    break_z: float | None = Field(default=None, validation_alias=AliasChoices("breakZ", "break_z"))
    px: float | None = None
    pz: float | None = None
    x0: float | None = None
    y0: float | None = None
    z0: float | None = None
    ax: float | None = None
    ay: float | None = None
    az: float | None = None
    vx0: float | None = None
    vy0: float | None = None
    vz0: float | None = None
    pfx_x: float | None = Field(default=None, validation_alias=AliasChoices("pfxX", "pfx_x"))
    pfx_z: float | None = Field(default=None, validation_alias=AliasChoices("pfxZ", "pfx_z"))
    pfx_z_with_gravity: float | None = Field(
        default=None, validation_alias=AliasChoices("pfxZWithGravity", "pfx_z_with_gravity")
    )
    break_x_inches: float | None = Field(
        default=None, validation_alias=AliasChoices("breakXInches", "break_x_inches")
    )
    break_x_feet: float | None = Field(default=None, validation_alias=AliasChoices("breakXFeet", "break_x_feet"))
    break_z_induced_inches: float | None = Field(
        default=None, validation_alias=AliasChoices("breakZInducedInches", "break_z_induced_inches")
    )
    break_z_induced_feet: float | None = Field(
        default=None, validation_alias=AliasChoices("breakZInducedFeet", "break_z_induced_feet")
    )
    break_z_with_gravity_inches: float | None = Field(
        default=None, validation_alias=AliasChoices("breakZWithGravityInches", "break_z_with_gravity_inches")
    )
    break_z_with_gravity_feet: float | None = Field(
        default=None, validation_alias=AliasChoices("breakZWithGravityFeet", "break_z_with_gravity_feet")
    )
    pfx_z_direction: str | None = Field(default=None, validation_alias=AliasChoices("pfxZDirection", "pfx_z_direction"))
    pfx_x_with_gravity: float | None = Field(
        default=None, validation_alias=AliasChoices("pfxXWithGravity", "pfx_x_with_gravity")
    )
    pfx_x_no_abs: float | None = Field(default=None, validation_alias=AliasChoices("pfxXNoAbs", "pfx_x_no_abs"))
    plate_time_sz_depth: float | None = Field(
        default=None, validation_alias=AliasChoices("plateTimeSZDepth", "plate_time_sz_depth")
    )
    plate_x_poly: float | None = Field(default=None, validation_alias=AliasChoices("plateXPoly", "plate_x_poly"))
    plate_y_poly: float | None = Field(default=None, validation_alias=AliasChoices("plateYPoly", "plate_y_poly"))
    plate_z_poly: float | None = Field(default=None, validation_alias=AliasChoices("plateZPoly", "plate_z_poly"))
    pfx_x_direction: str | None = Field(default=None, validation_alias=AliasChoices("pfxXDirection", "pfx_x_direction"))
    induced_break_z_forced_sign: str | None = Field(
        default=None, validation_alias=AliasChoices("inducedBreakZForcedSign", "induced_break_z_forced_sign")
    )
    ivb_z_direction: str | None = Field(default=None, validation_alias=AliasChoices("ivbZDirection", "ivb_z_direction"))
    savant_is_in_zone: bool | None = Field(
        default=None, validation_alias=AliasChoices("savantIsInZone", "savant_is_in_zone")
    )
    is_in_zone: bool | None = Field(default=None, validation_alias=AliasChoices("isInZone", "is_in_zone"))
    is_sword: bool | None = Field(default=None, validation_alias=AliasChoices("isSword", "is_sword"))
    is_bip_out: str | None = None


class SavantGamefeed(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team_home: List[PitchData]
    team_away: List[PitchData]
