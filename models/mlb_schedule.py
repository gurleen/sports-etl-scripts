"""Pydantic models for MLB Stats API ``GET /schedule`` responses."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, TypeAlias

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

AbstractGameState: TypeAlias = Literal["Preview", "Live", "Final"]
CodedGameState: TypeAlias = Literal["S", "P", "I", "F", "D", "U", "T", "O"]
DoubleHeaderFlag: TypeAlias = Literal["N", "Y"]
GameTypeCode: TypeAlias = Literal["S", "E", "R", "F", "D", "L", "W", "C", "N", "P"]


class ScheduleGameStatus(BaseModel):
    """``status`` on a schedule game."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    abstract_game_state: str = Field(
        validation_alias=AliasChoices("abstractGameState", "abstract_game_state")
    )
    detailed_state: str = Field(validation_alias=AliasChoices("detailedState", "detailed_state"))
    coded_game_state: str = Field(
        validation_alias=AliasChoices("codedGameState", "coded_game_state")
    )


class ScheduleTeamRef(BaseModel):
    """``team`` nested under ``teams.home`` / ``teams.away``."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    name: str


class ScheduleTeamSide(BaseModel):
    """One side of ``teams.home`` / ``teams.away``."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team: ScheduleTeamRef
    score: int | None = None
    is_winner: bool | None = Field(
        default=None, validation_alias=AliasChoices("isWinner", "is_winner")
    )


class ScheduleGameTeams(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    away: ScheduleTeamSide
    home: ScheduleTeamSide


class ScheduleVenue(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    name: str


class ScheduleGame(BaseModel):
    """One game in a schedule ``dates[].games`` entry."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    game_pk: int = Field(validation_alias=AliasChoices("gamePk", "game_pk"))
    game_date: datetime = Field(validation_alias=AliasChoices("gameDate", "game_date"))
    official_date: date = Field(validation_alias=AliasChoices("officialDate", "official_date"))
    status: ScheduleGameStatus
    teams: ScheduleGameTeams
    venue: ScheduleVenue
    game_type: str = Field(validation_alias=AliasChoices("gameType", "game_type"))
    series_description: str | None = Field(
        default=None, validation_alias=AliasChoices("seriesDescription", "series_description")
    )
    double_header: str = Field(validation_alias=AliasChoices("doubleHeader", "double_header"))
    is_tie: bool = Field(default=False, validation_alias=AliasChoices("isTie", "is_tie"))


class ScheduleDate(BaseModel):
    """One ``dates`` element grouping games by calendar day."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    date: date
    total_items: int = Field(validation_alias=AliasChoices("totalItems", "total_items"))
    total_events: int = Field(validation_alias=AliasChoices("totalEvents", "total_events"))
    total_games: int = Field(validation_alias=AliasChoices("totalGames", "total_games"))
    total_games_in_progress: int = Field(
        validation_alias=AliasChoices("totalGamesInProgress", "total_games_in_progress")
    )
    games: list[ScheduleGame]


class ScheduleResponse(BaseModel):
    """Top-level ``GET /schedule`` payload."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    copyright: str
    total_items: int = Field(validation_alias=AliasChoices("totalItems", "total_items"))
    total_events: int = Field(validation_alias=AliasChoices("totalEvents", "total_events"))
    total_games: int = Field(validation_alias=AliasChoices("totalGames", "total_games"))
    total_games_in_progress: int = Field(
        validation_alias=AliasChoices("totalGamesInProgress", "total_games_in_progress")
    )
    dates: list[ScheduleDate]
