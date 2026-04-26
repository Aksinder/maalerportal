"""Per-installation stale-data monitoring with auto-tuned thresholds.

Watches the upstream ``latestTimestamp`` for each installation and raises
a Repairs issue when a meter goes silent for noticeably longer than its
observed reporting cadence. Threshold is auto-tuned from the median
observed interval × a configurable multiplier, so an hourly meter
alarms after a few hours while a daily LPWAN meter only alarms after
several days — without per-meter manual config.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Any

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

# How many recent timestamps we use to estimate the meter's cadence.
_SAMPLE_BUFFER_SIZE = 20
# Minimum samples before we trust the auto-tuned interval over the
# fallback. Below this we treat the cadence as unknown.
_MIN_SAMPLES_FOR_AUTOTUNE = 3
# Lower bound for the threshold so we never alarm on a sub-hour delay
# even if the meter would in theory report faster.
_THRESHOLD_FLOOR = timedelta(hours=1)

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
    """Persists the observed timestamp ring buffer per installation."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._store = Store(
            hass, _STORE_VERSION, _STORE_KEY_FMT.format(entry_id=entry_id)
        )
        self._data: dict[str, dict[str, list[str]]] | None = None

    async def async_load(self) -> None:
        loaded = await self._store.async_load()
        self._data = dict(loaded) if isinstance(loaded, dict) else {}

    def get_timestamps(self, installation_id: str) -> list[datetime]:
        if self._data is None:
            return []
        raw = self._data.get(installation_id, {}).get("timestamps", [])
        parsed = [p for p in (_parse_iso(t) for t in raw) if p is not None]
        return parsed

    async def async_record(self, installation_id: str, ts_iso: str) -> None:
        if self._data is None:
            self._data = {}
        bucket = self._data.setdefault(installation_id, {"timestamps": []})
        timestamps: list[str] = bucket.setdefault("timestamps", [])
        # Skip duplicates — same reading observed on multiple polls.
        if timestamps and timestamps[-1] == ts_iso:
            return
        if ts_iso in timestamps:
            return
        timestamps.append(ts_iso)
        if len(timestamps) > _SAMPLE_BUFFER_SIZE:
            del timestamps[: len(timestamps) - _SAMPLE_BUFFER_SIZE]
        await self._store.async_save(self._data)


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

    latest_dt, latest_iso = observed
    await store.async_record(installation_id, latest_iso)

    factor = float(entry.options.get(CONF_STALE_FACTOR, DEFAULT_STALE_FACTOR))
    fallback = timedelta(
        hours=float(
            entry.options.get(
                CONF_STALE_FALLBACK_HOURS, DEFAULT_STALE_FALLBACK_HOURS
            )
        )
    )

    median = _median_delta(store.get_timestamps(installation_id))
    interval = median if median is not None else fallback
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
