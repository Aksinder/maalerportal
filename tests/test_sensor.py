"""Test Målerportal sensors."""
from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.helpers import entity_registry as er
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
        
        # Find sensor (native_value is float 1000.0, state is "1000.0")
        states = hass.states.async_all()
        sensor = next((s for s in states if s.state == "1000.0"), None)
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
        assert new_state.state == "1001.0"

        # Cleanup
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_unique_ids_stable_across_meter_swap(hass: HomeAssistant) -> None:
    """A meter swap must NOT change entity unique_ids.

    History — both short-term states and long-term statistics — is keyed on
    entity_id, which HA derives from each entity's unique_id. A meter swap
    changes the meterSerial and every meterCounterId, but the installation
    (address) stays the same. If any unique_id depended on the serial or the
    counter id, HA would register brand-new entities after a swap and orphan
    all history for that address.

    This guards that invariant: same installation + same counter types +
    same primary/secondary => identical unique_ids, regardless of serial or
    meterCounterId. Keep it green to keep history continuous across swaps.
    """
    installation = {
        "installationId": "swap-test",
        "installationType": "ColdWater",
        "address": "Swap Street 1",
        "meterSerial": "SERIAL-OLD",
        "utilityName": "Test Util",
    }
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "api_key": "test_key",
            "smarthome_base_url": "http://test",
            "installations": [installation],
            "email": "test@example.com",
        },
        options={"polling_interval": 30},
    )
    entry.add_to_hass(hass)

    def _meter_data(counter_id: str) -> dict:
        # Same counter types and primary flags as a like-for-like swap would
        # report, but brand-new meterCounterIds (the new physical meter).
        return {
            "meterCounters": [
                {
                    "meterCounterId": counter_id,
                    "counterType": "ColdWater",
                    "readingType": "counter",
                    "isPrimary": True,
                    "unit": "m³",
                    "latestValue": "100.0",
                    "latestTimestamp": "2026-01-01T12:00:00Z",
                },
                {
                    "meterCounterId": f"{counter_id}-noise",
                    "counterType": "AcousticNoise",
                    "readingType": "Value",
                    "isPrimary": False,
                    "unit": "Hz",
                    "latestValue": "12",
                    "latestTimestamp": "2026-01-01T12:00:00Z",
                },
            ]
        }

    ent_reg = er.async_get(hass)

    def _identity() -> dict[str, str]:
        # Map unique_id -> entity_id for this entry. Both must survive a swap:
        # unique_id is the registry key, entity_id is what history is stored
        # under.
        return {
            e.unique_id: e.entity_id
            for e in er.async_entries_for_config_entry(ent_reg, entry.entry_id)
        }

    # The reconcile step fetches /addresses to detect swaps; stub it out so
    # the test deterministically drives the swap via entry.data instead of a
    # live HTTP call (which would otherwise mark the installation missing).
    with patch(
        "custom_components.maalerportal.coordinator.MaalerportalCoordinator._async_update_data",
        new_callable=AsyncMock,
    ) as mock_update, patch(
        "custom_components.maalerportal._fetch_fresh_installations",
        new_callable=AsyncMock,
        return_value=None,
    ):
        # --- Original meter ---
        mock_update.return_value = _meter_data("counter-old")
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        before = _identity()
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

        # --- Meter swap: new serial + new meterCounterIds, same installation ---
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                "installations": [{**installation, "meterSerial": "SERIAL-NEW"}],
            },
        )
        mock_update.return_value = _meter_data("counter-new")
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        after = _identity()
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    assert before, "No entities were registered for the installation"
    # Identical unique_id -> entity_id mapping => same entities reused => all
    # history (states + statistics) stays attached across the swap.
    assert before == after, (
        "Meter swap changed entity identity — history would be orphaned.\n"
        f"before={before}\nafter={after}"
    )
    # unique_ids must be scoped to the installation, never the serial/counter id.
    for unique_id in after:
        assert unique_id.startswith("swap-test"), unique_id
        assert "SERIAL" not in unique_id
        assert "counter-old" not in unique_id and "counter-new" not in unique_id
    # entity_ids are frozen at first creation, so they keep the ORIGINAL serial
    # slug even after the swap — proof the entities were reused, not recreated.
    assert any("serial_old" in entity_id for entity_id in after.values()), after
