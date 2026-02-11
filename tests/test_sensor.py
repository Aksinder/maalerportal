"""Test Målerportal sensors."""
from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.util import dt as dt_util

from custom_components.maalerportal.const import DOMAIN
from tests.common import MockConfigEntry


async def test_sensor_setup(hass: HomeAssistant) -> None:
    """Test setting up sensors from config entry."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "api_key": "test_key",
            "smarthome_base_url": "http://test",
            "installations": [{
                "installationId": "123",
                "installationType": "Electricity",
                "address": "Test Address",
                "meterSerial": "M123",
                "utilityName": "Test Util",
            }],
            "email": "test@example.com",
        },
        options={"polling_interval": 30}
    )
    entry.add_to_hass(hass)

    # Mock coordinator update to return some data
    with patch(
        "custom_components.maalerportal.coordinator.MaalerportalCoordinator._async_update_data",
        new_callable=AsyncMock,
    ) as mock_update:
        mock_update.return_value = {
            "meterCounters": [
                {
                    "meterCounterId": "c1",
                    "counterType": "ElectricityFromGrid",
                    "readingType": "counter",
                    "isPrimary": True,
                    "unit": "kWh",
                    "latestValue": "1000.5",  # API returns strings sometimes
                    "latestTimestamp": "2023-01-01T12:00:00Z"
                },
                {
                    "counterType": "BatteryDaysRemaining",
                    "latestValue": "200",
                    "unit": "days",
                    "meterCounterId": "c2",
                    "readingType": "counter"
                }
            ]
        }
        
        # Setup entry
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        
        # Verify sensors are created
        # We can look up by unique_id if we have entity_registry, but generic state check is easier
        
        # Main sensor (unique_id: 123_main)
        # Name: Test Address - M123 Meter reading electricity
        # ID: sensor.test_address_m123_meter_reading_electricity (approx)
        
        # Let's find entities by iterating states
        states = hass.states.async_all()
        sensor_states = [s for s in states if s.entity_id.startswith("sensor.")]
        assert len(sensor_states) >= 2
        
        # Check values
        main_sensor = next((s for s in sensor_states if "meter_reading" in s.entity_id or "electricity" in s.entity_id), None)
        # Note: In actual HA env, reliable ID generation applies. Here we just assume it exists.
        
        # Just check that we have states with 1000.5 and 200
        # Filter out "unknown" which comes from statistics sensors
        values = [s.state for s in sensor_states if s.state != "unknown"]
        assert "1000.5" in values
        assert "200" in values
        
        # Cleanup
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

async def test_sensor_update(hass: HomeAssistant) -> None:
    """Test sensor updates."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "api_key": "test_key",
            "smarthome_base_url": "http://test",
            "installations": [{
                "installationId": "123",
                "installationType": "Electricity",
                "address": "Test Address",
                "meterSerial": "M123",
            }],
            "email": "test@example.com",
        },
    )
    entry.add_to_hass(hass)

    # Mock coordinator
    with patch(
        "custom_components.maalerportal.coordinator.MaalerportalCoordinator._async_update_data",
        new_callable=AsyncMock,
    ) as mock_update:
        # Initial data
        mock_update.return_value = {
            "meterCounters": [{
                "meterCounterId": "c1",
                "counterType": "ElectricityFromGrid",
                "isPrimary": True,
                "latestValue": "1000",
                "unit": "kWh",
                "latestTimestamp": "2023-01-01T12:00:00Z"
            }]
        }
        
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        
        # Find sensor
        states = hass.states.async_all()
        sensor = next((s for s in states if s.state == "1000"), None)
        assert sensor is not None
        
        # Update data
        mock_update.return_value = {
            "meterCounters": [{
                "meterCounterId": "c1",
                "counterType": "ElectricityFromGrid",
                "isPrimary": True,
                "latestValue": "1001",
                "unit": "kWh",
                "latestTimestamp": "2023-01-01T13:00:00Z"
            }]
        }
        
        # Trigger update via coordinator refresh
        # We need to access coordinator
        coordinator = hass.data[DOMAIN][entry.entry_id]["coordinators"]["123"]
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        
        # Check new state
        new_state = hass.states.get(sensor.entity_id)
        _LOGGER.debug("Sensor %s state: %s", sensor.entity_id, new_state.state)
        assert new_state.state == "1001"
        
        # Cleanup
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
