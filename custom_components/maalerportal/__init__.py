"""The Målerportal integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# List the platforms that you want to support.
PLATFORMS: list[Platform] = [Platform.SENSOR]

# Service constants
SERVICE_REFRESH = "refresh"
SERVICE_FETCH_MORE_HISTORY = "fetch_more_history"
ATTR_INSTALLATION_ID = "installation_id"

# Event constants
EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"

# Service schemas
SERVICE_REFRESH_SCHEMA = vol.Schema({
    vol.Optional(ATTR_INSTALLATION_ID): str,
})

SERVICE_FETCH_MORE_HISTORY_SCHEMA = vol.Schema({})


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Målerportal from a config entry."""

    hass.data.setdefault(DOMAIN, {})
    
    # Store the configuration data for sensors to use
    hass.data[DOMAIN][entry.entry_id] = {
        "api_key": entry.data["api_key"],
        "smarthome_base_url": entry.data["smarthome_base_url"],
        "installations": entry.data["installations"],
        "email": entry.data.get("email", ""),
        "sensors": [],  # Will be populated by sensor platform
        "history_fetched_days": entry.options.get("history_fetched_days", 30),
    }
    
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
            for eid in hass.data[DOMAIN]:
                entities = er.async_entries_for_config_entry(entity_registry, eid)
                
                for entity_entry in entities:
                    # Filter by installation_id if provided
                    if installation_id:
                        if not entity_entry.unique_id.startswith(f"{installation_id}_"):
                            continue
                    
                    # Schedule an update for this entity
                    hass.async_create_task(
                        hass.services.async_call(
                            "homeassistant",
                            "update_entity",
                            {"entity_id": entity_entry.entity_id},
                        )
                    )
                    _LOGGER.debug("Scheduled refresh for entity: %s", entity_entry.entity_id)

        hass.services.async_register(
            DOMAIN,
            SERVICE_REFRESH,
            async_refresh_service,
            schema=SERVICE_REFRESH_SCHEMA,
        )
        _LOGGER.info("Registered %s.%s service", DOMAIN, SERVICE_REFRESH)

    # Register fetch_more_history service (only once)
    if not hass.services.has_service(DOMAIN, SERVICE_FETCH_MORE_HISTORY):
        async def async_fetch_more_history_service(call: ServiceCall) -> None:
            """Handle fetch more history service call."""
            from .sensor import MaalerportalStatisticSensor
            
            total_records = 0
            
            for eid, entry_data in hass.data[DOMAIN].items():
                current_days = entry_data.get("history_fetched_days", 30)
                from_days = current_days + 30
                to_days = current_days
                
                _LOGGER.info(
                    "Fetching more history for entry %s: %d to %d days ago",
                    eid, from_days, to_days
                )
                
                sensors = entry_data.get("sensors", [])
                for sensor in sensors:
                    if isinstance(sensor, MaalerportalStatisticSensor):
                        count = await sensor.async_fetch_older_history(from_days, to_days)
                        total_records += count
                
                # Persist updated days to config entry options (survives restarts)
                entry_data["history_fetched_days"] = from_days
                config_entry = hass.config_entries.async_get_entry(eid)
                if config_entry:
                    new_options = dict(config_entry.options)
                    new_options["history_fetched_days"] = from_days
                    hass.config_entries.async_update_entry(
                        config_entry, options=new_options
                    )
            
            _LOGGER.info(
                "Fetch more history complete: %d total records inserted",
                total_records
            )

        hass.services.async_register(
            DOMAIN,
            SERVICE_FETCH_MORE_HISTORY,
            async_fetch_more_history_service,
            schema=SERVICE_FETCH_MORE_HISTORY_SCHEMA,
        )
        _LOGGER.info("Registered %s.%s service", DOMAIN, SERVICE_FETCH_MORE_HISTORY)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
        
        # Unregister service if no more entries
        if not hass.data[DOMAIN]:
            hass.services.async_remove(DOMAIN, SERVICE_REFRESH)
            hass.services.async_remove(DOMAIN, SERVICE_FETCH_MORE_HISTORY)
            _LOGGER.info("Unregistered %s services", DOMAIN)

    return unload_ok


async def async_options_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    # Don't reload if only history_fetched_days changed (not a config change)
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    stored_days = entry_data.get("history_fetched_days", 30)
    option_days = entry.options.get("history_fetched_days", 30)
    if stored_days == option_days:
        # A real config change (e.g. polling_interval) — reload
        _LOGGER.info("Options updated, reloading integration")
        await hass.config_entries.async_reload(entry.entry_id)
    else:
        # Just history_fetched_days update — sync in-memory value, no reload needed
        entry_data["history_fetched_days"] = option_days
        _LOGGER.debug("Updated history_fetched_days to %d (no reload)", option_days)


def fire_meter_updated_event(
    hass: HomeAssistant,
    installation_id: str,
    meter_value: float,
    unit: str,
    timestamp: str | None = None,
    counter_type: str | None = None,
) -> None:
    """Fire an event when meter data is updated."""
    event_data: dict[str, Any] = {
        "installation_id": installation_id,
        "meter_value": meter_value,
        "unit": unit,
    }
    if timestamp:
        event_data["timestamp"] = timestamp
    if counter_type:
        event_data["counter_type"] = counter_type
    
    hass.bus.fire(EVENT_METER_UPDATED, event_data)
    _LOGGER.debug("Fired %s event: %s", EVENT_METER_UPDATED, event_data)
