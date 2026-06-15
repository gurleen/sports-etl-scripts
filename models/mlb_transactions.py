"""Pydantic models for MLB Stats API ``GET /transactions`` responses."""

from __future__ import annotations

from datetime import date

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class TransactionRef(BaseModel):
    """A ``person`` / ``fromTeam`` / ``toTeam`` reference — we keep only the id."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int


class Transaction(BaseModel):
    """One entry in ``transactions[]``.

    ``person``, ``fromTeam``, and ``toTeam`` are optional (e.g. trades carry no
    ``person`` row, and most non-trade moves carry no ``fromTeam``).
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    person: TransactionRef | None = None
    from_team: TransactionRef | None = Field(
        default=None, validation_alias=AliasChoices("fromTeam", "from_team")
    )
    to_team: TransactionRef | None = Field(
        default=None, validation_alias=AliasChoices("toTeam", "to_team")
    )
    date: date
    effective_date: date | None = Field(
        default=None, validation_alias=AliasChoices("effectiveDate", "effective_date")
    )
    resolution_date: date | None = Field(
        default=None, validation_alias=AliasChoices("resolutionDate", "resolution_date")
    )
    type_code: str = Field(validation_alias=AliasChoices("typeCode", "type_code"))
    type_desc: str = Field(validation_alias=AliasChoices("typeDesc", "type_desc"))
    description: str | None = None


class TransactionsResponse(BaseModel):
    """Top-level ``GET /transactions`` payload."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    copyright: str
    transactions: list[Transaction] = Field(default_factory=list)
