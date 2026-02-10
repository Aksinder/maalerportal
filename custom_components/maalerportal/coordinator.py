"""DataUpdateCoordinator for Målerportal integration."""
from datetime import timedelta
import logging
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

class MaalerportalCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Målerportal data."""

    def __init__(
        self,
        hass: HomeAssistant,
        api_key: str,
        base_url: str,
        installation: dict,
        polling_interval: timedelta,
    ) -> None:
        """Initialize."""
        self.api_key = api_key
        self.base_url = base_url
        self.installation = installation
        self.installation_id = installation["installationId"]
        self.session = async_get_clientsession(hass)

        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{self.installation_id}",
            update_interval=polling_interval,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from API endpoint."""
        try:
            async with aiohttp.ClientTimeout(total=30) as timeout:
                response = await self.session.get(
                    f"{self.base_url}/installations/{self.installation_id}/readings/latest",
                    headers={"ApiKey": self.api_key},
                    timeout=timeout,
                )

                if response.status == 429:
                    raise UpdateFailed("Rate limit exceeded")
                
                if response.status in (403, 404):
                    # Installation possibly removed or key invalid
                    raise UpdateFailed(f"Installation not accessible: {response.status}")

                if not response.ok:
                    raise UpdateFailed(f"Error communicating with API: {response.status}")

                data = await response.json()
                
                # Check for valid data structure
                if not data or "meterCounters" not in data:
                    _LOGGER.debug(
                        "No meterCounters in response for %s", self.installation_id
                    )
                    return {"meterCounters": []}

                _LOGGER.debug(
                    "Received %d meter counters for %s", 
                    len(data.get("meterCounters", [])), 
                    self.installation_id
                )
                
                return data

        except aiohttp.ClientConnectorError as err:
            raise UpdateFailed(f"Connection error: {err}") from err
        except Exception as err:
            raise UpdateFailed(f"Unexpected error: {err}") from err
