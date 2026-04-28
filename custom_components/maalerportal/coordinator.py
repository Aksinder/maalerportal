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
        readings_log: Any = None,
    ) -> None:
        """Initialize."""
        self.api_key = api_key
        self.base_url = base_url
        self.installation = installation
        self.installation_id = installation["installationId"]
        self.currency = currency
        self.session = async_get_clientsession(hass)
        # Optional CSV append-only log of every reading we observe.
        # Provided by __init__ at setup time; coordinator just calls it
        # on each successful poll and after fallback fetches.
        self.readings_log = readings_log

        # Per-counter fallback for when /readings/latest returns null.
        # Populated lazily from /readings/historical and refreshed only
        # when the cache is empty for a counter that needs it. Real
        # latestValues clear the cache as they arrive.
        self._fallback_values: dict[str, dict[str, Any]] = {}

        # Tracks (latestTimestamp, datetime_we_first_saw_it) per counter.
        # Used to compute report-lag — i.e. how long after the meter
        # recorded a value did our integration receive it. Reset
        # whenever latestTimestamp changes (= new reading observed).
        self._first_observed: dict[str, tuple[str, datetime]] = {}

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
                self._update_first_observed(data["meterCounters"])
                await self._log_readings(data["meterCounters"])

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

    def _update_first_observed(self, counters: list[dict[str, Any]]) -> None:
        """Update the first-observed-at marker for each counter.

        When latestTimestamp changes (= new reading came in), record now
        as the moment we first saw it. The marker survives until the
        next new timestamp arrives. Used by extra_state_attributes on
        sensors to compute report_lag_minutes (time between meter
        recording and our integration observing).
        """
        now = datetime.now(timezone.utc)
        for counter in counters:
            cid = counter.get("meterCounterId")
            ts = counter.get("latestTimestamp")
            if not cid or not ts:
                continue
            cached = self._first_observed.get(cid)
            if cached is None or cached[0] != ts:
                self._first_observed[cid] = (ts, now)

    def first_observed_at(self, counter_id: str, timestamp: str) -> datetime | None:
        """Return when we first saw this exact timestamp for the counter,
        or None if we haven't observed it yet."""
        cached = self._first_observed.get(counter_id)
        if cached is None or cached[0] != timestamp:
            return None
        return cached[1]

    async def _log_readings(self, counters: list[dict[str, Any]]) -> None:
        """Forward the latest reading per counter to the CSV archive.

        Live polls log with source="latest"; if the value came from the
        coordinator's null-value fallback (counter["isFallback"]=True)
        we tag it as such so the user can tell the two apart in the
        CSV.
        """
        if self.readings_log is None:
            return
        for counter in counters:
            ts = counter.get("latestTimestamp")
            value = counter.get("latestValue")
            if not ts or value is None:
                continue
            source = "fallback" if counter.get("isFallback") else "latest"
            await self.readings_log.async_record(
                timestamp=ts,
                counter_type=counter.get("counterType", ""),
                meter_counter_id=counter.get("meterCounterId", ""),
                value=value,
                unit=counter.get("unit", ""),
                source=source,
            )

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

        # Archive every historical reading we fetched, not just the latest
        # we'll use as the fallback. Dedup in the log makes this safe.
        if self.readings_log is not None:
            await self.readings_log.async_record_many(readings, source="historical")

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
