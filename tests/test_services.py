"""Test Målerportal services."""
from unittest.mock import AsyncMock, patch, MagicMock
import pytest

from homeassistant.core import HomeAssistant
from custom_components.maalerportal.const import DOMAIN, SERVICE_FETCH_MORE_HISTORY

async def test_fetch_more_history_service(hass: HomeAssistant) -> None:
    """Test fetch_more_history service call."""
    
    # Mock a sensor that has the method
    mock_sensor = MagicMock()
    mock_sensor.entity_id = "sensor.test_statistic"
    mock_sensor.async_fetch_older_history = AsyncMock()
    
    # Mock hass data structure
    # We don't strictly need to pre-populate hass.data if setup runs correctly
    # But checking previous implementation, it did:
    # hass.data[DOMAIN] = ...

    from custom_components.maalerportal import async_setup_entry
    from tests.common import MockConfigEntry
    
    entry = MockConfigEntry(domain=DOMAIN, data={
        "api_key": "k", 
        "smarthome_base_url": "u",
        "installations": [],
        "email": "e"
    })
    entry.add_to_hass(hass)
    
    # We need to mock coordinator creation inside setup
    with patch("custom_components.maalerportal.MaalerportalCoordinator"), \
         patch("homeassistant.config_entries.ConfigEntries.async_forward_entry_setups"):
        
        await async_setup_entry(hass, entry)
        
        # Now inject our mock sensor into hass.data
        # The setup creates the structure and adds empty sensors list
        hass.data[DOMAIN][entry.entry_id]["sensors"].append(mock_sensor)
        
        # Call service
        data = {
            "entity_id": "sensor.test_statistic",
            "from_days": 90,
            "to_days": 60
        }
        await hass.services.async_call(DOMAIN, SERVICE_FETCH_MORE_HISTORY, data, blocking=True)
        
        # Verify method called
        mock_sensor.async_fetch_older_history.assert_called_once_with(90, 60)

async def test_fetch_more_history_service_no_entity(hass: HomeAssistant) -> None:
    """Test service call with non-existent entity."""
    # Similar setup
    from custom_components.maalerportal import async_setup_entry
    from tests.common import MockConfigEntry
    
    entry = MockConfigEntry(domain=DOMAIN, data={
        "api_key": "k", 
        "smarthome_base_url": "u",
        "installations": [],
        "email": "e"
    })
    entry.add_to_hass(hass)
    
    with patch("custom_components.maalerportal.MaalerportalCoordinator"), \
         patch("homeassistant.config_entries.ConfigEntries.async_forward_entry_setups"):
        
        await async_setup_entry(hass, entry)
        
        # Call service for unknown entity
        data = {"entity_id": "sensor.unknown", "from_days": 90}
        await hass.services.async_call(DOMAIN, SERVICE_FETCH_MORE_HISTORY, data, blocking=True)
        
        # Should just warn and not crash (no assertions needed other than no exception)
