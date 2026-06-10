"""Skeleton client for MLB public APIs."""

from __future__ import annotations

from typing import Any

from api_clients.base import BaseApiClient
from models.mlb_schedule import ScheduleResponse

STATS_API_V1_BASE = "https://statsapi.mlb.com/api/v1"
SAVANT_BASE = "https://baseballsavant.mlb.com"


class MlbApiClient:
    """
    Facade for MLB HTTP APIs used by this repo.

    Sub-clients are split by host so callers can grow endpoints without one
    oversized class. Methods are stubs until concrete endpoints are wired in.
    """

    def __init__(
        self,
        *,
        stats_base_url: str = STATS_API_V1_BASE,
        savant_base_url: str = SAVANT_BASE,
        timeout_sec: float | None = None,
        user_agent: str | None = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if timeout_sec is not None:
            kwargs["timeout_sec"] = timeout_sec
        if user_agent is not None:
            kwargs["user_agent"] = user_agent

        self.stats = _StatsApiClient(base_url=stats_base_url, **kwargs)
        self.savant = _SavantClient(base_url=savant_base_url, **kwargs)


class _StatsApiClient(BaseApiClient):
    """MLB Stats API (``statsapi.mlb.com``)."""

    def get_schedule(
        self,
        *,
        sport_id: int | None = None,
        date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        team_id: int | None = None,
        league_id: int | None = None,
        season: int | None = None,
        game_pk: int | None = None,
        game_type: str | None = None,
        hydrate: str | None = None,
        fields: str | None = None,
    ) -> ScheduleResponse:
        """
        ``GET /schedule`` — games for a date, range, team, or sport.

        Date parameters use ``YYYY-MM-DD``. ``game_type`` examples: ``R``, ``P``, ``S``, ``E``.
        """
        params = {
            "sportId": sport_id,
            "date": date,
            "startDate": start_date,
            "endDate": end_date,
            "teamId": team_id,
            "leagueId": league_id,
            "season": season,
            "gamePk": game_pk,
            "gameType": game_type,
            "hydrate": hydrate,
            "fields": fields,
        }
        payload = self.get_json("/schedule", params=params)
        return ScheduleResponse.model_validate(payload)

    def get_game(self, game_pk: int) -> Any:
        """``GET /game/{game_pk}/feed/live`` — full live feed (play-by-play).

        The live feed lives on the ``v1.1`` API; the rest of this client targets
        ``v1``, so we resolve the version per call.
        """
        live_base = self.base_url.replace("/api/v1", "/api/v1.1")
        saved = self.base_url
        try:
            self.base_url = live_base
            return self.get_json(f"/game/{game_pk}/feed/live")
        finally:
            self.base_url = saved


class _SavantClient(BaseApiClient):
    """Baseball Savant (``baseballsavant.mlb.com``)."""

    def get_gamefeed(self, game_pk: int) -> Any:
        """Savant ``/gf`` gamefeed JSON for ``game_pk``."""
        raise NotImplementedError
