import httpx
import asyncio
from typing import Dict, Any, List, Optional
from config.settings import settings
from config.exceptions import BetsAPIError, RateLimitError


class Bet365Client:
    def __init__(self):
        self.base_url = settings.BASE_URL
        self.api_key = settings.BETSAPI_API_KEY
        self.client = httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT)

    async def _make_request(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        if params is None:
            params = {}

        params["token"] = self.api_key

        try:
            # Remova o "/v1" duplicado da URL
            url = f"{self.base_url}/{endpoint}"
            if url.endswith("//"):
                url = url[:-1]  # Remove uma barra se houver duplicação

            response = await self.client.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if data.get("success") == 0:
                error_msg = data.get("error", "Unknown error")
                if "rate limit" in error_msg.lower():
                    raise RateLimitError(error_msg)
                raise BetsAPIError(error_msg)

            return data

        except httpx.HTTPError as e:
            raise BetsAPIError(f"HTTP error: {str(e)}")

    # Bet365 InPlay
    async def inplay(self) -> Dict[str, Any]:
        return await self._make_request("bet365/inplay")

    # Bet365 InPlay Filter
    async def inplay_filter(
        self, sport_id: Optional[int] = None, league_id: Optional[int] = None
    ) -> Dict[str, Any]:
        params = {}
        if sport_id:
            params["sport_id"] = sport_id
        if league_id:
            params["league_id"] = league_id
        return await self._make_request("bet365/inplay_filter", params)

    # Bet365 Inplay Event
    async def event(
        self, FI: str, stats: bool = False, lineup: bool = False, raw: bool = False
    ) -> Dict[str, Any]:
        params = {"FI": FI}
        if stats:
            params["stats"] = 1
        if lineup:
            params["lineup"] = 1
        if raw:
            params["raw"] = 1
        return await self._make_request("bet365/event", params)

    # Bet365 Upcoming Events
    async def upcoming(
        self,
        sport_id: int,
        league_id: Optional[int] = None,
        day: Optional[str] = None,
        page: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = {"sport_id": sport_id}
        if league_id:
            params["league_id"] = league_id
        if day:
            params["day"] = day
        if page:
            params["page"] = page
        return await self._make_request("bet365/upcoming", params)

    # Bet365 PreMatch Odds
    async def prematch(self, FI: str, raw: bool = False) -> Dict[str, Any]:
        params = {"FI": FI}
        if raw:
            params["raw"] = 1
        return await self._make_request("bet365/prematch", params)

    # Bet365 Result
    async def result(self, event_id: str, raw: bool = False) -> Dict[str, Any]:
        params = {"event_id": event_id}
        if raw:
            params["raw"] = 1
        return await self._make_request("bet365/result", params)

    async def close(self):
        await self.client.aclose()
