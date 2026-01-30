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
ATTR_INSTALLATION_ID = "installation_id"

# Event constants
EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"

# Service schema
SERVICE_REFRESH_SCHEMA = vol.Schema({
    vol.Optional(ATTR_INSTALLATION_ID): str,
})


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Målerportal from a config entry."""

    hass.data.setdefault(DOMAIN, {})
    
    # Store the configuration data for sensors to use
    hass.data[DOMAIN][entry.entry_id] = {
        "api_key": entry.data["api_key"],
        "smarthome_base_url": entry.data["smarthome_base_url"],
        "installations": entry.data["installations"],
        "sensors": [],  # Will be populated by sensor platform
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
            
            # Find all sensors and refresh them
            entity_registry = er.async_get(hass)
            entities = er.async_entries_for_config_entry(entity_registry, entry.entry_id)
            
            for entity_entry in entities:
                # Get the entity state to check if it belongs to the right installation
                if installation_id:
                    # Filter by installation_id if provided
                    if installation_id not in entity_entry.unique_id:
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

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
        
        # Unregister service if no more entries
        if not hass.data[DOMAIN]:
            hass.services.async_remove(DOMAIN, SERVICE_REFRESH)
            _LOGGER.info("Unregistered %s.%s service", DOMAIN, SERVICE_REFRESH)

    return unload_ok


async def async_options_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    _LOGGER.info("Options updated, reloading integration")
    await hass.config_entries.async_reload(entry.entry_id)


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
