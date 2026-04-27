"""Per-installation stale-data monitoring with auto-tuned thresholds.

Watches the upstream ``latestTimestamp`` for each installation and raises
a Repairs issue when a meter goes silent for noticeably longer than its
observed reporting cadence.

Cadence is computed from real upstream-reported timestamps in
``/readings/historical`` — not from when WE happen to poll. This means
an hourly meter actually gets an hourly cadence (not 18-min based on
poll-time observations of bursts), and a sparse LPWAN meter gets a
multi-day cadence. The historical fetch is cached in the per-entry
Store so we don't re-fetch on every poll.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store

from .const import DOMAIN
from .coordinator import MaalerportalCoordinator

_LOGGER = logging.getLogger(__name__)

CONF_STALE_FACTOR = "stale_data_factor"
CONF_STALE_FALLBACK_HOURS = "stale_data_fallback_hours"

DEFAULT_STALE_FACTOR = 3.0
DEFAULT_STALE_FALLBACK_HOURS = 12.0

# How many days of upstream history we look at when computing cadence.
# 14 days captures both bursty and sparse meters without paying for a
# full 30-day chunk.
_CADENCE_HISTORY_DAYS = 14
# Minimum readings needed before we trust the auto-tuned interval over
# the fallback. Below this we treat the cadence as unknown.
_MIN_SAMPLES_FOR_AUTOTUNE = 3
# Lower bound for the threshold so we never alarm on a sub-hour delay
# even if the meter would in theory report faster.
_THRESHOLD_FLOOR = timedelta(hours=1)
# How long the cached cadence is considered valid before we re-fetch
# upstream history. Cadence drifts slowly so a week is fine.
_CADENCE_CACHE_TTL = timedelta(days=7)

# Legacy ring-buffer constant kept for backwards-compatible storage
# reads. New deployments do not write to it.
_LEGACY_SAMPLE_BUFFER_SIZE = 20

_STORE_VERSION = 1
_STORE_KEY_FMT = f"{DOMAIN}.stale_monitor.{{entry_id}}"


def _issue_id(installation_id: str) -> str:
    return f"stale_data_{installation_id}"


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        return None


def _median_delta(timestamps: list[datetime]) -> timedelta | None:
    if len(timestamps) < _MIN_SAMPLES_FOR_AUTOTUNE:
        return None
    sorted_ts = sorted(timestamps)
    deltas = [
        (sorted_ts[i + 1] - sorted_ts[i]).total_seconds()
        for i in range(len(sorted_ts) - 1)
    ]
    sorted_deltas = sorted(deltas)
    median_seconds = sorted_deltas[len(sorted_deltas) // 2]
    return timedelta(seconds=median_seconds)


class StaleDataStore:
    """Persists the auto-tuned cadence per installation.

    Cadence is computed from upstream-reported timestamps via
    /readings/historical, cached for a week, then re-fetched.
    """

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._store = Store(
            hass, _STORE_VERSION, _STORE_KEY_FMT.format(entry_id=entry_id)
        )
        self._data: dict[str, dict[str, Any]] | None = None

    async def async_load(self) -> None:
        loaded = await self._store.async_load()
        self._data = dict(loaded) if isinstance(loaded, dict) else {}

    def get_cached_cadence(self, installation_id: str) -> timedelta | None:
        """Return cached cadence if still fresh, else None."""
        if self._data is None:
            return None
        bucket = self._data.get(installation_id, {})
        seconds = bucket.get("cadence_seconds")
        computed_at = bucket.get("cadence_computed_at")
        if seconds is None or computed_at is None:
            return None
        cached_dt = _parse_iso(computed_at)
        if cached_dt is None:
            return None
        if datetime.now(timezone.utc) - cached_dt > _CADENCE_CACHE_TTL:
            return None
        try:
            return timedelta(seconds=float(seconds))
        except (TypeError, ValueError):
            return None

    async def async_set_cadence(
        self, installation_id: str, cadence: timedelta
    ) -> None:
        if self._data is None:
            self._data = {}
        bucket = self._data.setdefault(installation_id, {})
        bucket["cadence_seconds"] = cadence.total_seconds()
        bucket["cadence_computed_at"] = datetime.now(timezone.utc).isoformat()
        # Discard the legacy ring-buffer; we don't use it anymore.
        bucket.pop("timestamps", None)
        await self._store.async_save(self._data)


def _primary_counter_id(coordinator: MaalerportalCoordinator) -> str | None:
    if not coordinator.data:
        return None
    for counter in coordinator.data.get("meterCounters", []):
        if counter.get("isPrimary"):
            return counter.get("meterCounterId")
    # Fallback: first counter we have if no primary marker
    counters = coordinator.data.get("meterCounters", [])
    if counters:
        return counters[0].get("meterCounterId")
    return None


async def _async_compute_cadence_from_history(
    coordinator: MaalerportalCoordinator,
) -> timedelta | None:
    """Fetch /readings/historical and compute median delta between
    upstream-reported timestamps for the primary counter.

    Returns None on API failure or when there isn't enough data.
    """
    primary_id = _primary_counter_id(coordinator)
    if not primary_id:
        return None

    now = datetime.now(timezone.utc)
    from_dt = (now - timedelta(days=_CADENCE_HISTORY_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    to_dt = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        async with coordinator.session.post(
            f"{coordinator.base_url}/installations/"
            f"{coordinator.installation_id}/readings/historical",
            json={"from": from_dt, "to": to_dt},
            headers={
                "ApiKey": coordinator.api_key,
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if not response.ok:
                _LOGGER.debug(
                    "Cadence history fetch returned HTTP %s for %s",
                    response.status,
                    coordinator.installation_id,
                )
                return None
            data = await response.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        _LOGGER.debug(
            "Cadence history fetch failed for %s: %s",
            coordinator.installation_id,
            err,
        )
        return None

    timestamps: list[datetime] = []
    for reading in data.get("readings", []) if isinstance(data, dict) else []:
        if reading.get("meterCounterId") != primary_id:
            continue
        parsed = _parse_iso(reading.get("timestamp"))
        if parsed is not None:
            timestamps.append(parsed)

    if len(timestamps) < _MIN_SAMPLES_FOR_AUTOTUNE:
        return None

    # Deduplicate identical timestamps (some endpoints emit duplicates
    # across counters even after the meterCounterId filter).
    unique_sorted = sorted(set(timestamps))
    if len(unique_sorted) < _MIN_SAMPLES_FOR_AUTOTUNE:
        return None

    deltas = [
        (unique_sorted[i + 1] - unique_sorted[i]).total_seconds()
        for i in range(len(unique_sorted) - 1)
    ]
    sorted_deltas = sorted(deltas)
    median_seconds = sorted_deltas[len(sorted_deltas) // 2]
    return timedelta(seconds=median_seconds)


def _latest_observed(coordinator: MaalerportalCoordinator) -> tuple[datetime, str] | None:
    if not coordinator.data:
        return None
    latest_dt: datetime | None = None
    latest_iso: str | None = None
    for counter in coordinator.data.get("meterCounters", []):
        ts_iso = counter.get("latestTimestamp")
        if not ts_iso:
            continue
        parsed = _parse_iso(ts_iso)
        if parsed is None:
            continue
        if latest_dt is None or parsed > latest_dt:
            latest_dt = parsed
            latest_iso = ts_iso
    if latest_dt is None or latest_iso is None:
        return None
    return latest_dt, latest_iso


async def async_check_stale_data(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator: MaalerportalCoordinator,
    store: StaleDataStore,
) -> None:
    """Re-evaluate the stale-data Repairs issue for one installation."""
    observed = _latest_observed(coordinator)
    installation_id = coordinator.installation["installationId"]
    if observed is None:
        # No upstream timestamp at all — clear any existing issue, this
        # is most likely an installation that hasn't received its first
        # reading yet rather than a stale one.
        ir.async_delete_issue(hass, DOMAIN, _issue_id(installation_id))
        return

    latest_dt, _latest_iso = observed

    factor = float(entry.options.get(CONF_STALE_FACTOR, DEFAULT_STALE_FACTOR))
    fallback = timedelta(
        hours=float(
            entry.options.get(
                CONF_STALE_FALLBACK_HOURS, DEFAULT_STALE_FALLBACK_HOURS
            )
        )
    )

    # Cadence is derived from upstream-reported timestamps via
    # /readings/historical, cached in the store for a week. This avoids
    # the previous poll-time sampling which over-fit to bursts.
    cadence = store.get_cached_cadence(installation_id)
    if cadence is None:
        fresh = await _async_compute_cadence_from_history(coordinator)
        if fresh is not None:
            await store.async_set_cadence(installation_id, fresh)
            cadence = fresh
    interval = cadence if cadence is not None else fallback
    threshold = max(interval * factor, _THRESHOLD_FLOOR)

    age = datetime.now(timezone.utc) - latest_dt

    issue_id = _issue_id(installation_id)
    if age > threshold:
        installation = coordinator.installation
        device_label = (
            f"{installation.get('address', 'Unknown')} - "
            f"{installation.get('meterSerial', 'Unknown')}"
        )
        ir.async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            is_persistent=True,
            severity=ir.IssueSeverity.WARNING,
            translation_key="stale_data",
            translation_placeholders={
                "installation": device_label,
                "hours_since": f"{age.total_seconds() / 3600:.1f}",
                "expected_hours": f"{interval.total_seconds() / 3600:.1f}",
                "threshold_hours": f"{threshold.total_seconds() / 3600:.1f}",
            },
        )
    else:
        ir.async_delete_issue(hass, DOMAIN, issue_id)


@callback
def attach_stale_monitor(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator: MaalerportalCoordinator,
    store: StaleDataStore,
) -> callback:
    """Subscribe to coordinator updates so each refresh re-evaluates the
    stale state. Returns the unsubscribe handle."""

    @callback
    def _on_update() -> None:
        hass.async_create_task(
            async_check_stale_data(hass, entry, coordinator, store)
        )

    return coordinator.async_add_listener(_on_update)
