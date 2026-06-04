"""Shared HTTP helpers for API clients."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; etl-scripts/1.0; +https://github.com/) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SEC = 60.0


class BaseApiClient:
    """Minimal JSON HTTP client using :mod:`urllib`."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.user_agent = user_agent

    def _build_url(self, path: str, params: dict[str, Any] | None = None) -> str:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{path}"
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            if query:
                url = f"{url}?{query}"
        return url

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """GET ``path`` (relative to ``base_url``) and parse the response as JSON."""
        url = self._build_url(path, params)
        req = Request(
            url,
            headers={"User-Agent": self.user_agent, "Accept": "application/json"},
            method="GET",
        )
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read()
        except HTTPError as e:
            raise RuntimeError(f"HTTP {e.code} for {url}") from e
        except URLError as e:
            raise RuntimeError(f"Request failed for {url}: {e.reason}") from e
        return json.loads(raw.decode("utf-8"))
