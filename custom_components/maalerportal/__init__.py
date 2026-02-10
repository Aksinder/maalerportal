"""The Målerportal integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, ATTR_ENTITY_ID
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er
import homeassistant.helpers.config_validation as cv

from datetime import timedelta
from .const import DOMAIN, DEFAULT_POLLING_INTERVAL
from .coordinator import MaalerportalCoordinator

_LOGGER = logging.getLogger(__name__)

# List the platforms that you want to support.
PLATFORMS: list[Platform] = [Platform.SENSOR]

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


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Målerportal from a config entry."""

    hass.data.setdefault(DOMAIN, {})
    
    # Store the configuration data for sensors to use
    hass.data[DOMAIN][entry.entry_id] = {
        "smarthome_base_url": entry.data["smarthome_base_url"],
        "installations": entry.data["installations"],
        "email": entry.data.get("email", ""),
        "sensors": [],  # Will be populated by sensor platform
        "history_fetched_days": entry.options.get("history_fetched_days", 30),
        "coordinators": {},  # Will store coordinators by installation_id
    }
    
    # Initialize coordinators for each installation
    polling_interval_minutes = entry.options.get("polling_interval", DEFAULT_POLLING_INTERVAL)
    polling_interval = timedelta(minutes=polling_interval_minutes)
    
    # Use config entry ID as part of a unique session key if needed, or just use helper
    # We use async_get_clientsession(hass) in checking/sensors now, so no custom session here.
    
    for installation in entry.data["installations"]:
        installation_id = installation["installationId"]
        
        # Create coordinator
        coordinator = MaalerportalCoordinator(
            hass,
            entry.data["api_key"],
            entry.data["smarthome_base_url"],
            installation,
            polling_interval,
        )
        
        # Perform initial fetch
        await coordinator.async_config_entry_first_refresh()
        
        hass.data[DOMAIN][entry.entry_id]["coordinators"][installation_id] = coordinator
    
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
    if unload_ok and DOMAIN in hass.data and entry.entry_id in hass.data[DOMAIN]:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


async def async_options_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(entry.entry_id)
