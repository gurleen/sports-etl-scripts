"""Client for the FanGraphs RosterResource API (``fangraphs.com/api/roster-resource``).

FanGraphs sits behind Cloudflare's bot-management challenge, which blocks plain
``urllib`` requests (even with a browser ``User-Agent``) with ``HTTP 403``. We use
``curl_cffi`` with ``impersonate="chrome"`` to match a real Chrome TLS fingerprint,
which is sufficient to pass the challenge.
"""

from __future__ import annotations

from typing import Any

from curl_cffi import requests

from models.fangraphs_contracts import TeamContractsResponse

ROSTER_RESOURCE_BASE = "https://www.fangraphs.com/api/roster-resource"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SEC = 60.0


class FanGraphsApiClient:
    """Facade for FanGraphs HTTP APIs used by this repo."""

    def __init__(
        self,
        *,
        base_url: str = ROSTER_RESOURCE_BASE,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.roster_resource = _RosterResourceClient(
            base_url=base_url, timeout_sec=timeout_sec, user_agent=user_agent
        )


class _RosterResourceClient:
    """RosterResource payroll/contract endpoints (``/api/roster-resource``)."""

    def __init__(self, *, base_url: str, timeout_sec: float, user_agent: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.user_agent = user_agent

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{path}"
        resp = requests.get(
            url,
            params={k: v for (k, v) in (params or {}).items() if v is not None},
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/plain, */*",
            },
            impersonate="chrome",
            timeout=self.timeout_sec,
        )
        resp.raise_for_status()
        return resp.json()

    def get_team_contracts(self, team_id: int, season: int) -> TeamContractsResponse:
        """``GET /contracts/team-2020`` — payroll and contract data for one team/season."""
        payload = self.get_json(
            "/contracts/team-2020", params={"teamid": team_id, "season": season}
        )
        return TeamContractsResponse.model_validate(payload)
