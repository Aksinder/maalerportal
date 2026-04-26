"""DataUpdateCoordinator for Målerportal integration."""
from datetime import datetime, timedelta, timezone
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

# Some meters return latestValue=null from /readings/latest even when the
# historical endpoint has perfectly good data. We then backfill the
# coordinator's response from the most recent historical reading. The
# API rejects ranges longer than 31 days with HTTP 500, so we stay one
# day under the limit to leave room for any timezone slack on either
# end of the request window.
_FALLBACK_HISTORY_DAYS = 30

class MaalerportalCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Målerportal data."""

    def __init__(
        self,
        hass: HomeAssistant,
        api_key: str,
        base_url: str,
        installation: dict,
        polling_interval: timedelta,
        currency: str = "SEK",
    ) -> None:
        """Initialize."""
        self.api_key = api_key
        self.base_url = base_url
        self.installation = installation
        self.installation_id = installation["installationId"]
        self.currency = currency
        self.session = async_get_clientsession(hass)

        # Per-counter fallback for when /readings/latest returns null.
        # Populated lazily from /readings/historical and refreshed only
        # when the cache is empty for a counter that needs it. Real
        # latestValues clear the cache as they arrive.
        self._fallback_values: dict[str, dict[str, Any]] = {}

        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{self.installation_id}",
            update_interval=polling_interval,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from API endpoint."""
        try:
            async with self.session.get(
                f"{self.base_url}/installations/{self.installation_id}/readings/latest",
                headers={"ApiKey": self.api_key},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:

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

                await self._backfill_null_latest_values(data["meterCounters"])

                return data

        except aiohttp.ClientConnectorError as err:
            raise UpdateFailed(f"Connection error: {err}") from err
        except Exception as err:
            raise UpdateFailed(f"Unexpected error: {err}") from err

    async def _backfill_null_latest_values(
        self, counters: list[dict[str, Any]]
    ) -> None:
        """Replace ``latestValue: null`` with the freshest historical reading.

        Some Målerportal installations expose data through
        ``/readings/historical`` only; ``/readings/latest`` returns
        nulls. Without this fallback those sensors stay "Unknown"
        forever even though the user can clearly see meter readings
        elsewhere. We fetch one 31-day historical window when needed,
        cache the result, and apply it. Live latestValues from the API
        always win and clear the cache for that counter.
        """
        needs_lookup: list[str] = []
        for counter in counters:
            counter_id = counter.get("meterCounterId")
            if not counter_id:
                continue
            if counter.get("latestValue") is None:
                if counter_id not in self._fallback_values:
                    needs_lookup.append(counter_id)
            else:
                # Real reading came in — discard any stale fallback.
                self._fallback_values.pop(counter_id, None)

        if needs_lookup:
            await self._refresh_fallback_from_history(needs_lookup)

        for counter in counters:
            counter_id = counter.get("meterCounterId")
            if counter.get("latestValue") is None and counter_id in self._fallback_values:
                fb = self._fallback_values[counter_id]
                counter["latestValue"] = fb["value"]
                counter["latestTimestamp"] = fb["timestamp"]
                # Marker so downstream consumers can tell live from filled.
                counter["isFallback"] = True

    async def _refresh_fallback_from_history(self, counter_ids: list[str]) -> None:
        """Populate fallback cache for the given counters from /readings/historical."""
        try:
            now = datetime.now(timezone.utc)
            # Use precise timestamps (no midnight rounding) so the request
            # span stays strictly inside the API's 31-day limit. With
            # _FALLBACK_HISTORY_DAYS = 30 we get exactly 30 days, leaving
            # room for any clock drift between client and server.
            from_date = (now - timedelta(days=_FALLBACK_HISTORY_DAYS)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            to_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            _LOGGER.debug(
                "Fetching fallback history for %s: %s -> %s (counters: %s)",
                self.installation_id, from_date, to_date, counter_ids,
            )
            async with self.session.post(
                f"{self.base_url}/installations/{self.installation_id}/readings/historical",
                json={"from": from_date, "to": to_date},
                headers={"ApiKey": self.api_key, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if not response.ok:
                    body = await response.text()
                    # Bump to WARNING — debug-only made this silently fail
                    # the first time around; users need visibility.
                    _LOGGER.warning(
                        "Fallback history fetch returned HTTP %s for installation %s: %s",
                        response.status,
                        self.installation_id,
                        body[:200],
                    )
                    return
                data = await response.json()
        except (aiohttp.ClientError, TimeoutError) as err:
            _LOGGER.warning("Fallback history fetch failed for %s: %s",
                            self.installation_id, err)
            return

        readings = data.get("readings", []) if isinstance(data, dict) else []
        for counter_id in counter_ids:
            candidates = [
                r for r in readings
                if r.get("meterCounterId") == counter_id
                and r.get("value") is not None
                and r.get("timestamp")
            ]
            if not candidates:
                continue
            # Pick the most recent — timestamp strings sort lexicographically
            # because they are ISO-8601 with consistent timezone offset.
            candidates.sort(key=lambda r: r["timestamp"], reverse=True)
            latest = candidates[0]
            self._fallback_values[counter_id] = {
                "value": latest["value"],
                "timestamp": latest["timestamp"],
            }
            _LOGGER.info(
                "Backfilled latestValue for counter %s on installation %s "
                "from history: %s @ %s",
                counter_id,
                self.installation_id,
                latest["value"],
                latest["timestamp"],
            )
