"""Global fixtures for Målerportal integration tests."""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

# This fixture enables loading custom integrations in all tests.
# Usually to enable custom integrations, you need to add this fixture
# or use pytest-homeassistant-custom-component.
@pytest.fixture(autouse=True)
async def auto_enable_custom_integrations(enable_custom_integrations):
    """Enable custom integrations defined in the test dir."""
    yield

# Prepare a bypass for authentication calls
@pytest.fixture(name="bypass_auth")
def bypass_auth_fixture():
    """Skip authentication calls to Målerportal API."""
    with patch("custom_components.maalerportal.config_flow.check_auth_methods", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.attempt_login", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.get_api_key", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.test_api_connection", new_callable=AsyncMock):
        yield

@pytest.fixture(autouse=True)
def mock_recorder():
    """Mock recorder."""
    with patch("homeassistant.components.recorder.get_instance") as mock_get_instance, \
         patch("homeassistant.components.recorder.statistics.get_last_statistics"), \
         patch("homeassistant.components.recorder.statistics.async_import_statistics"):
        
        # Configure instance to support awaited methods
        mock_instance = MagicMock()
        mock_instance.async_add_executor_job = AsyncMock()
        mock_get_instance.return_value = mock_instance
        
        yield
