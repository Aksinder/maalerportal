"""Common test tools."""
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from pytest_homeassistant_custom_component.common import MockConfigEntry as HAMockConfigEntry

class MockConfigEntry(HAMockConfigEntry):
    """Mock ConfigEntry for testing."""
    # We can inherit from pytest-homeassistant-custom-component's MockConfigEntry 
    # or just use it directly in tests, but tests/test_sensor.py imports it from tests.common
    pass
