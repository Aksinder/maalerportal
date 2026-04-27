"""The Målerportal integration."""
from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, ATTR_ENTITY_ID
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import (
    device_registry as dr,
    entity_registry as er,
    issue_registry as ir,
)
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
import homeassistant.helpers.config_validation as cv

from datetime import timedelta
from .const import DOMAIN, DEFAULT_POLLING_INTERVAL, CONF_CURRENCY, DEFAULT_CURRENCY
from .coordinator import MaalerportalCoordinator
from .reconcile import (
    TRACKED_INSTALLATION_FIELDS,
    find_new_installations,
    reconcile_installations,
)
from .stale_monitor import (
    StaleDataStore,
    async_check_stale_data,
    attach_stale_monitor,
)

# Storage for meter offsets — kept separate from entry.data so updates
# don't trigger config-entry reload listeners.
_OFFSET_STORE_VERSION = 1
_OFFSET_STORE_KEY_FMT = f"{DOMAIN}.meter_offsets.{{entry_id}}"

_LOGGER = logging.getLogger(__name__)

# List the platforms that you want to support.
PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]

# Service constants
SERVICE_REFRESH = "refresh"
SERVICE_FETCH_MORE_HISTORY = "fetch_more_history"
ATTR_INSTALLATION_ID = "installation_id"
ATTR_FROM_DAYS = "from_days"
ATTR_TO_DAYS = "to_days"

# Event constants
EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"

# Service schemas
SERVICE_REFRESH_SCHEMA = vol.Schema({
    vol.Optional(ATTR_INSTALLATION_ID): str,
})

SERVICE_FETCH_MORE_HISTORY_SCHEMA = vol.Schema({
    vol.Required(ATTR_ENTITY_ID): cv.entity_ids,
    vol.Optional(ATTR_FROM_DAYS, default=60): vol.Coerce(int),
    vol.Optional(ATTR_TO_DAYS, default=30): vol.Coerce(int),
})


async def _fetch_fresh_installations(
    session: aiohttp.ClientSession, base_url: str, api_key: str
) -> list[dict[str, Any]] | None:
    """Fetch the current installation list from the API.

    Returns a flat list of installation dicts (matching the shape we store
    in entry.data["installations"]), or None if the call failed. Returning
    None signals callers to fall back to cached data.
    """
    try:
        async with session.get(
            f"{base_url}/addresses",
            headers={"ApiKey": api_key},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:
            if not response.ok:
                _LOGGER.warning(
                    "Could not refresh installations from API: HTTP %s", response.status
                )
                return None
            addresses = await response.json()
    except (aiohttp.ClientError, TimeoutError) as err:
        _LOGGER.warning("Could not refresh installations from API: %s", err)
        return None

    fresh: list[dict[str, Any]] = []
    for address in addresses or []:
        for installation in address.get("installations", []):
            fresh.append(
                {
                    "installationId": installation.get("installationId"),
                    "address": address.get("address"),
                    "timezone": address.get("timezone"),
                    "installationType": installation.get("installationType"),
                    "utilityName": installation.get("utilityName"),
                    "meterSerial": installation.get("meterSerial"),
                    "nickname": installation.get("nickname", ""),
                }
            )
    return fresh


def _missing_installation_issue_id(installation_id: str) -> str:
    """Stable issue id used to dedupe Repair entries across restarts."""
    return f"missing_installation_{installation_id}"


def _surface_reconciliation_changes(
    hass: HomeAssistant,
    missing_ids: set[str],
    serial_changes: dict[str, dict[str, tuple[Any, Any]]],
    saved: list[dict[str, Any]],
    fresh: list[dict[str, Any]],
) -> None:
    """Surface reconciliation results: Repairs issues + log entries."""
    saved_ids = {i.get("installationId") for i in saved}

    # Create a Repairs issue per missing installation. async_create_issue is
    # idempotent on (domain, issue_id), so a restart with the same missing
    # installation won't spam the user — they'll see one entry in Repairs.
    for installation_id in missing_ids:
        ir.async_create_issue(
            hass,
            DOMAIN,
            _missing_installation_issue_id(installation_id),
            is_fixable=False,
            is_persistent=True,
            severity=ir.IssueSeverity.WARNING,
            translation_key="missing_installation",
            translation_placeholders={"installation_id": installation_id},
        )
        _LOGGER.info(
            "Installation %s is no longer present upstream — see Repairs "
            "for cleanup instructions.",
            installation_id,
        )

    # If a previously missing installation is back, clear its Repairs issue.
    for saved_id in saved_ids:
        if saved_id not in missing_ids:
            ir.async_delete_issue(
                hass, DOMAIN, _missing_installation_issue_id(saved_id)
            )

    for installation_id, changes in serial_changes.items():
        if "meterSerial" in changes:
            old, new = changes["meterSerial"]
            _LOGGER.info(
                "Installation %s: meter serial changed %s -> %s (likely meter replacement). "
                "Statistics will be re-anchored to keep the accumulated total continuous.",
                installation_id,
                old,
                new,
            )
        for field, (old, new) in changes.items():
            if field == "meterSerial":
                continue
            _LOGGER.debug(
                "Installation %s: %s changed %r -> %r",
                installation_id,
                field,
                old,
                new,
            )

    for new_installation in find_new_installations(saved, fresh):
        _LOGGER.info(
            "Found new installation %s in the account that is not configured. "
            "Reconfigure the integration to add it.",
            new_installation.get("installationId"),
        )


def _promote_legacy_last_reading_entities(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Move LastReadingSensor entities out of Diagnostic.

    The sensor was originally placed under EntityCategory.DIAGNOSTIC,
    which hides it from the primary device card by default. It's
    actually a high-value signal (was-the-meter-alive-recently?), so
    we want it visible without expanding Diagnostic. Idempotent —
    only updates entities that still carry the old category.
    """
    entity_registry = er.async_get(hass)
    for entity in list(entity_registry.entities.values()):
        if entity.config_entry_id != entry.entry_id:
            continue
        if entity.domain != "sensor":
            continue
        if not (entity.unique_id or "").endswith("_last_reading"):
            continue
        if entity.entity_category is not None:
            entity_registry.async_update_entity(
                entity.entity_id,
                entity_category=None,
            )
            _LOGGER.info(
                "Promoted %s out of Diagnostic to main sensors",
                entity.entity_id,
            )


def _hide_legacy_statistic_entities(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Migrate visibility for stats-only sensors created before the
    ``entity_registry_visible_default = False`` change.

    ``entity_registry_visible_default`` only controls the default for
    NEW entities; existing entries keep whatever visibility they had.
    Find any of our statistic sensors that are still visible by default
    (``hidden_by is None``) and hide them via the integration so the
    user's device card stops showing the "Unknown" eyesore. Users who
    explicitly un-hid them are respected (we only change `None`).
    """
    entity_registry = er.async_get(hass)
    for entity in list(entity_registry.entities.values()):
        if entity.config_entry_id != entry.entry_id:
            continue
        if entity.domain != "sensor":
            continue
        if "_statistic_" not in (entity.unique_id or ""):
            continue
        if entity.hidden_by is None:
            entity_registry.async_update_entity(
                entity.entity_id,
                hidden_by=er.RegistryEntryHider.INTEGRATION,
            )
            _LOGGER.info(
                "Hid legacy stats-only entity %s from default device card",
                entity.entity_id,
            )


def _update_device_registry(
    hass: HomeAssistant, entry: ConfigEntry, installations: list[dict[str, Any]]
) -> None:
    """Sync device registry entries (name, serial number) with fresh data."""
    device_registry = dr.async_get(hass)
    for installation in installations:
        installation_id = installation.get("installationId")
        if not installation_id:
            continue
        device = device_registry.async_get_device(
            identifiers={(DOMAIN, installation_id)}
        )
        if device is None:
            continue

        new_name = f"{installation.get('address')} - {installation.get('meterSerial')}"
        if installation.get("nickname"):
            new_name += f" ({installation['nickname']})"
        new_serial = installation.get("meterSerial")
        new_manufacturer = installation.get("utilityName")

        updates: dict[str, Any] = {}
        # Only update the default name; respect any user-set name_by_user.
        if device.name != new_name:
            updates["name"] = new_name
        if new_serial and device.serial_number != new_serial:
            updates["serial_number"] = new_serial
        if new_manufacturer and device.manufacturer != new_manufacturer:
            updates["manufacturer"] = new_manufacturer

        if updates:
            device_registry.async_update_device(device.id, **updates)


class MeterOffsetStore:
    """Persistent per-counter offset store backed by HA's Store helper.

    Offsets keep the user-facing accumulated total continuous across
    physical meter replacements: ``displayed_sum = raw_value + offset``.
    Storage is separate from ``entry.data`` so writes don't trigger the
    config-entry reload listener.
    """

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self._store = Store(
            hass,
            _OFFSET_STORE_VERSION,
            _OFFSET_STORE_KEY_FMT.format(entry_id=entry_id),
        )
        self._data: dict[str, dict[str, float]] | None = None

    async def async_load(self) -> None:
        """Load existing offsets from disk."""
        loaded = await self._store.async_load()
        self._data = dict(loaded) if isinstance(loaded, dict) else {}

    def get(self, installation_id: str, counter_id: str) -> float:
        """Return offset for (installation, counter), 0.0 if unset."""
        if self._data is None:
            return 0.0
        return float(self._data.get(installation_id, {}).get(counter_id, 0.0))

    async def async_set(
        self, installation_id: str, counter_id: str, offset: float
    ) -> None:
        """Update and persist offset for (installation, counter)."""
        if self._data is None:
            self._data = {}
        self._data.setdefault(installation_id, {})[counter_id] = float(offset)
        await self._store.async_save(self._data)
        _LOGGER.info(
            "Persisted meter offset for installation %s counter %s: %.4f",
            installation_id,
            counter_id,
            offset,
        )


def is_swap_pending(
    hass: HomeAssistant, entry_id: str, installation_id: str
) -> bool:
    """Whether the installation's meter just changed and offsets need recompute."""
    store = hass.data.get(DOMAIN, {}).get(entry_id, {})
    return installation_id in store.get("pending_swap_installations", set())


def consume_swap_pending(
    hass: HomeAssistant, entry_id: str, installation_id: str
) -> None:
    """Mark a pending swap as handled so it isn't reapplied."""
    store = hass.data.get(DOMAIN, {}).get(entry_id)
    if store is None:
        return
    pending = set(store.get("pending_swap_installations", set()))
    pending.discard(installation_id)
    store["pending_swap_installations"] = pending


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Målerportal from a config entry."""

    hass.data.setdefault(DOMAIN, {})

    base_url = entry.data["smarthome_base_url"]
    api_key = entry.data["api_key"]
    session = async_get_clientsession(hass)

    # One-shot migration: hide statistic sensors that pre-date the
    # entity_registry_visible_default change (idempotent).
    _hide_legacy_statistic_entities(hass, entry)
    _promote_legacy_last_reading_entities(hass, entry)

    # Reconcile saved installations against the API so that meter swaps,
    # nickname/address edits etc. are picked up automatically at startup.
    fresh_installations = await _fetch_fresh_installations(session, base_url, api_key)
    pending_swap_ids: set[str] = set()
    if fresh_installations is not None:
        (
            merged_installations,
            missing_ids,
            serial_changes,
            changed,
        ) = reconcile_installations(entry.data["installations"], fresh_installations)
        _surface_reconciliation_changes(
            hass,
            missing_ids,
            serial_changes,
            entry.data["installations"],
            fresh_installations,
        )
        pending_swap_ids = set(serial_changes.keys())
        if changed:
            hass.config_entries.async_update_entry(
                entry,
                data={**entry.data, "installations": merged_installations},
            )
            _update_device_registry(hass, entry, merged_installations)
        installations = merged_installations
    else:
        installations = entry.data["installations"]
        missing_ids = set()

    # Load persisted meter offsets used to keep the user-facing accumulated
    # total continuous across physical meter replacements.
    offset_store = MeterOffsetStore(hass, entry.entry_id)
    await offset_store.async_load()

    # Stale-data monitor — observed-timestamp ring buffer per installation,
    # used to auto-tune the "no recent data" Repairs threshold.
    stale_store = StaleDataStore(hass, entry.entry_id)
    await stale_store.async_load()

    # `pending_swap_installations` lists installations whose meter serial
    # just changed; their statistic sensors recompute the offset on next run.
    hass.data[DOMAIN][entry.entry_id] = {
        "smarthome_base_url": base_url,
        "installations": installations,
        "email": entry.data.get("email", ""),
        "sensors": [],  # Will be populated by sensor platform
        "history_fetched_days": entry.options.get("history_fetched_days", 7),
        "coordinators": {},  # Will store coordinators by installation_id
        "offset_store": offset_store,
        "stale_store": stale_store,
        "stale_monitor_unsubs": [],
        "pending_swap_installations": pending_swap_ids,
    }

    # Initialize coordinators for each installation
    polling_interval_minutes = entry.options.get("polling_interval", DEFAULT_POLLING_INTERVAL)
    polling_interval = timedelta(minutes=polling_interval_minutes)

    # Currency: options override takes precedence over initial config data
    currency = entry.options.get(
        CONF_CURRENCY,
        entry.data.get(CONF_CURRENCY, DEFAULT_CURRENCY),
    )

    for installation in installations:
        installation_id = installation["installationId"]

        if installation_id in missing_ids:
            # Skip coordinator setup for installations that no longer exist
            # upstream, otherwise their guaranteed-to-fail first refresh would
            # block the whole entry from loading via ConfigEntryNotReady.
            _LOGGER.debug(
                "Skipping coordinator for missing installation %s", installation_id
            )
            continue

        # Create coordinator
        coordinator = MaalerportalCoordinator(
            hass,
            api_key,
            base_url,
            installation,
            polling_interval,
            currency,
        )

        # Perform initial fetch
        await coordinator.async_config_entry_first_refresh()

        hass.data[DOMAIN][entry.entry_id]["coordinators"][installation_id] = coordinator

        # Re-evaluate stale-data state right away (before subscribing
        # so the listener doesn't double-fire on first run) and then
        # subscribe to keep it current on every coordinator refresh.
        await async_check_stale_data(hass, entry, coordinator, stale_store)
        unsub = attach_stale_monitor(hass, entry, coordinator, stale_store)
        hass.data[DOMAIN][entry.entry_id]["stale_monitor_unsubs"].append(unsub)
    
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    # Register listener for options updates
    entry.async_on_unload(entry.add_update_listener(async_options_update_listener))

    # Register refresh service (only once)
    if not hass.services.has_service(DOMAIN, SERVICE_REFRESH):
        async def async_refresh_service(call: ServiceCall) -> None:
            """Handle refresh service call."""
            installation_id = call.data.get(ATTR_INSTALLATION_ID)
            
            _LOGGER.debug("Refresh service called with installation_id: %s", installation_id)
            
            # Find all sensors across ALL config entries and refresh them
            entity_registry = er.async_get(hass)
            
            # Since we switched to DataUpdateCoordinator, refreshing means triggering the coordinator
            # But the service originally just called async_update on entities.
            # With coordinators, we should refresh the coordinator.
            
            # Find coordinators matching installation_id
            for entry_id, entry_data in hass.data[DOMAIN].items():
                coordinators = entry_data.get("coordinators", {})
                for inst_id, coordinator in coordinators.items():
                    if installation_id and inst_id != installation_id:
                        continue
                        
                    _LOGGER.info("Forcing refresh for installation %s", inst_id)
                    await coordinator.async_request_refresh()
            
            # For non-coordinator sensors (if any remain manual polling), we can still iterate entities
            # But `sensor.py` logic now uses coordinator for almost everything except history sensors?
            # History sensors (Statistic/Consumption) are MaalerportalPollingSensor.
            # They implement async_update.
            # We can find them in "sensors" list.
            
            for entry_id, entry_data in hass.data[DOMAIN].items():
                sensors = entry_data.get("sensors", [])
                for sensor in sensors:
                    # Filter by installation_id if provided
                    # Sensor objects have _installation_id attribute
                    if hasattr(sensor, "_installation_id"):
                        if installation_id and sensor._installation_id != installation_id:
                            continue
                        
                        # Only update polling sensors (coordinator sensors update via coordinator)
                        if hasattr(sensor, "async_update") and not hasattr(sensor, "coordinator"):
                             _LOGGER.debug("Updating polling sensor %s", sensor.entity_id)
                             hass.async_create_task(sensor.async_update())

        hass.services.async_register(
            DOMAIN, SERVICE_REFRESH, async_refresh_service, schema=SERVICE_REFRESH_SCHEMA
        )

    # Register fetch more history service (only once)
    if not hass.services.has_service(DOMAIN, SERVICE_FETCH_MORE_HISTORY):
        async def async_fetch_history_service(call: ServiceCall) -> None:
            """Handle fetch more history service call."""
            entity_ids = call.data.get(ATTR_ENTITY_ID)
            from_days = call.data.get(ATTR_FROM_DAYS)
            to_days = call.data.get(ATTR_TO_DAYS)
            
            if isinstance(entity_ids, str):
                entity_ids = [entity_ids]
                
            _LOGGER.info("Fetch history service called for %s (days %d to %d)", 
                         entity_ids, from_days, to_days)
            
            # Iterate all sensors and find matches
            for entry_id, entry_data in hass.data[DOMAIN].items():
                sensors = entry_data.get("sensors", [])
                for sensor in sensors:
                    if sensor.entity_id in entity_ids:
                        # Check if sensor supports history fetching
                        if hasattr(sensor, "async_fetch_older_history"):
                            _LOGGER.info("Fetching history for %s", sensor.entity_id)
                            hass.async_create_task(
                                sensor.async_fetch_older_history(from_days, to_days)
                            )
                        else:
                            _LOGGER.warning("Sensor %s does not support fetching history", sensor.entity_id)

        hass.services.async_register(
            DOMAIN, 
            SERVICE_FETCH_MORE_HISTORY, 
            async_fetch_history_service, 
            schema=SERVICE_FETCH_MORE_HISTORY_SCHEMA
        )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok and DOMAIN in hass.data and entry.entry_id in hass.data[DOMAIN]:
        store = hass.data[DOMAIN][entry.entry_id]
        for unsub in store.get("stale_monitor_unsubs", []):
            try:
                unsub()
            except Exception:  # noqa: BLE001
                pass
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


async def async_options_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(entry.entry_id)
