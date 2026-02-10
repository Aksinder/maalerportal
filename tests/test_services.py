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
    hass.data[DOMAIN] = {
        "entry_1": {
            "sensors": [mock_sensor],
            "coordinators": {},
        }
    }
    
    # We need to register the service first, usually done in async_setup_entry
    # But here we can just register the handler manually or loop through setup
    # If we use full integration test setup, service is registered.
    
    # Let's mock the service registration or assume setup works.
    # We'll use a patch on async_setup_entry to ensure service is registered
    
    # Actually, the service logic is in __init__.py. 
    # Let's mock the service registration call in __init__ or just call the handler if exposed.
    # The handler is defined inside async_setup_entry.
    
    # Better approach: Setup the integration fully with a mock sensor
    # But integration setup creates real sensors.
    
    # Let's rely on patching `hass.data` AFTER setup.
    
    # 1. Setup integration (mocked)
    with patch("custom_components.maalerportal.async_setup_entry") as mock_setup:
        # We need the real async_setup_entry to run to register services
        # So we should import it and run it?
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
            # The setup creates the structure
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
