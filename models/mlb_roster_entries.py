"""Pydantic models for MLB Stats API ``GET /people/{id}?hydrate=rosterEntries``.

Each ``rosterEntries[]`` item is already a *stint* — an interval
(``startDate`` → ``endDate``) of one player's membership on one team under one
status. We keep only the ids and the stint-defining fields; the human names live
in the (separate) teams/players dimensions.
"""

from __future__ import annotations

from datetime import date

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class RosterEntryTeam(BaseModel):
    """The ``team`` on a roster entry — kept as its bare id plus parent org.

    ``parentOrgId`` is present for minor-league affiliates (it points at the MLB
    parent club) and absent for MLB clubs themselves.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    parent_org_id: int | None = Field(
        default=None, validation_alias=AliasChoices("parentOrgId", "parent_org_id")
    )


class RosterEntryPosition(BaseModel):
    """The ``position`` on a roster entry."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    code: str | None = None
    abbreviation: str | None = None


class RosterEntryStatus(BaseModel):
    """The ``status`` on a roster entry (e.g. ``A`` Active, ``FA`` Free Agent)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    code: str
    description: str | None = None


class RosterEntry(BaseModel):
    """One entry in ``people[].rosterEntries[]`` — a single roster stint.

    ``end_date`` is ``None`` for an open (current) stint; the API fills it in once
    the stint closes, so re-fetching keeps trailing rows correct.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    jersey_number: str | None = Field(
        default=None, validation_alias=AliasChoices("jerseyNumber", "jersey_number")
    )
    position: RosterEntryPosition | None = None
    status: RosterEntryStatus
    team: RosterEntryTeam
    is_active: bool | None = Field(
        default=None, validation_alias=AliasChoices("isActive", "is_active")
    )
    is_active_forty_man: bool | None = Field(
        default=None, validation_alias=AliasChoices("isActiveFortyMan", "is_active_forty_man")
    )
    start_date: date = Field(validation_alias=AliasChoices("startDate", "start_date"))
    end_date: date | None = Field(
        default=None, validation_alias=AliasChoices("endDate", "end_date")
    )
    status_date: date | None = Field(
        default=None, validation_alias=AliasChoices("statusDate", "status_date")
    )


class Person(BaseModel):
    """One entry in ``people[]`` — we keep the id and its hydrated roster entries."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    roster_entries: list[RosterEntry] = Field(
        default_factory=list,
        validation_alias=AliasChoices("rosterEntries", "roster_entries"),
    )


class PeopleResponse(BaseModel):
    """Top-level ``GET /people/{id}`` payload."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    copyright: str | None = None
    people: list[Person] = Field(default_factory=list)
